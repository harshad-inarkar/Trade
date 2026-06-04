"""
dhan_trade.py — Dhan HQ automated order placement (Object-Oriented).
"""

import math
import tomllib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Optional

import requests
from requests.exceptions import RequestException

from tradeapi.price_strike_calc import get_price_strike, get_strike_interval
from tradeapi.scrip_master import ScripMaster, _get_today_str
from utils.network.start_proxy import SSHProxyManager


__all__ = ["DhanTrader", "UIOverride", "Instrument", "PriceLevels"]

BASE_DIR         = Path(__file__).parent
SYMBOLS_CONFIG   = BASE_DIR / 'symbols_config.toml'
ACCESS_FILE_PATH = BASE_DIR / 'access_token.toml'
API_CONFIG_PATH  = BASE_DIR / 'dhan_trade.toml'


class PriceCondition(Enum):
    GREATER_THAN = 'GREATER_THAN'
    LESS_THAN    = 'LESS_THAN'


@dataclass
class PriceLevels:
    entry:      float
    limit:      float
    stop_loss:  float
    stop_limit: float
    target:     float
    trail:      float


@dataclass
class Instrument:
    symb:          str
    exch:          str
    seg:           str
    expiry_date:   str             = ''
    signal:        str             = ''
    quant:         int             = 1
    entry_val:     float           = 0.0
    trade_amount:  float           = 0.0
    strike:        Optional[float] = None
    opt_type:      Optional[str]   = None
    trigger_price: float           = 0.0
    limit_price:   float           = 0.0


@dataclass
class UIOverride:
    inst_type:     str             = ""
    strike:        float           = 0.0
    expiry:        str             = ""
    limit_price:   float           = 0.0
    trigger_price: float           = 0.0
    force_qty:     bool            = False
    opt_type:      Optional[str]   = None


def _signal_to_opt(signal: str) -> str:
    return 'CE' if signal == 'BUY' else 'PE'


def _invert_signal(signal: str) -> str:
    return 'SELL' if signal == 'BUY' else 'BUY'


def _adjust_price(base: float, perc: float, signal: str, opt_bump: bool = False) -> float:
    perc = 10 * perc if opt_bump else perc
    if signal == 'BUY':
        return math.ceil(base * (1 + perc / 100))
    return math.floor(base * (1 - perc / 100))


class SymbolsConfig:
    def __init__(self, path: Path):
        self._path = path
        self._mtime = None
        self._config = {}

    def get(self, key: str, default=None):
        self.refresh()
        return self._config.get(key, default)

    def refresh(self, retry=True) -> None:
        try:
            mtime = self._path.stat().st_mtime
        except OSError:
            rtry_str = '[Retry] ' if not retry else ''
            print(f'{rtry_str}Failed to Stat {self._path}')
            if retry:
                self._path = SYMBOLS_CONFIG
                print(f' Set config path to {self._path}')
                self.refresh(retry=False)
            return

        if self._mtime == mtime:
            return

        print('Symbols config file changed — reloading.')
        self._mtime = mtime

        try:
            with open(self._path, 'rb') as f:
                self._config = tomllib.load(f) or {}
            print('Symbol config loaded.')
        except Exception as exc:
            rtry_str = '[Retry] ' if not retry else ''
            print(f'{rtry_str}Failed to parse {self._path} TOML config: {exc}')


class DhanAPIConfig:
    """Loads execution logic mappings and API endpoints."""
    def __init__(self, path: Path):
        self.urls: dict[str, str] = {}
        self.segments: dict[str, list[str]] = {}
        self.opt_segments: frozenset[str] = frozenset()
        self.fut_segments: frozenset[str] = frozenset()
        self.fno_segments: frozenset[str] = frozenset()
        self.maps: dict[str, dict[str, str]] = {}
        self.fallback_steps: list[int] = []

        self._load(path)

    def _load(self, path: Path) -> None:
        if not path.exists():
            print(f"[!] Warning: {path} not found. Using defaults.")
            return

        with open(path, 'rb') as f:
            data = tomllib.load(f)

        self.urls = data.get('urls', {})
        self.segments = data.get('segments', {})
        self.opt_segments = frozenset(self.segments.get('opt_segments', []))
        self.fut_segments = frozenset(self.segments.get('fut_segments', []))
        self.fno_segments = frozenset(self.segments.get('fno_segments', []))
        
        self.maps = data.get('maps', {})
        self.fallback_steps = data.get('fallback', {}).get('steps', [])


class DhanTrader:
    def __init__(
        self,
        symb_config: Path = SYMBOLS_CONFIG,
        refresh_master_scrip: bool = False,
        restart_proxy: bool = False,
    ):
        self.api_cfg = DhanAPIConfig(API_CONFIG_PATH)
        self.cfg = SymbolsConfig(symb_config)
        self.cfg.refresh()
        
        self._defaults_config: MappingProxyType = MappingProxyType({
            'expiry':           self.cfg.get('def_expiry_date', ''),
            'quant':            self.cfg.get('def_quantity', 1),
            'trade_amount':     self.cfg.get('def_trade_amount', 10000),
            'order_mode':       self.cfg.get('def_order_mode', ''),
            'place_order_mode': self.cfg.get('place_order_mode', 'MARKET')
        })
        
        self.traded_this_scan = set()

        self.proxy_manager = SSHProxyManager()
        if restart_proxy:
            self.proxy_manager.restart()

        self.session = requests.Session()
        self._apply_proxy()

        self.client_id, self.access_token = self._load_credentials(ACCESS_FILE_PATH)
        self.api_headers = {
            'access-token': self.access_token,
            'Content-Type': 'application/json',
            'Accept':       'application/json',
        }

        self.scrip = ScripMaster(
            session_obj=self.session, refresh_master_scrip=refresh_master_scrip
        )

        self.entry_perc      = self.cfg.get('entry_price_perc', 0.1)
        self.limit_perc      = self.cfg.get('limit_price_perc', 0.2)
        self.target_perc     = self.cfg.get('target_perc', 4.0)
        self.stop_loss_perc  = self.cfg.get('stop_loss_perc', 0.7)
        self.stop_trail_perc = self.cfg.get('stop_trail_perc', 0.5)

    def _apply_proxy(self) -> None:
        try:
            proxy_cfg = self.proxy_manager.config.get('proxy', {})
            if proxy_host := proxy_cfg.get('proxy_host', ''):
                proxy_url = f"socks5h://{proxy_host}:{proxy_cfg.get('port', 0)}"
                self.session.proxies = {'http': proxy_url, 'https': proxy_url}
        except Exception:
            pass

    def _load_credentials(self, path: Path) -> tuple[str, str]:
        try:
            with open(path, 'rb') as f:
                data = tomllib.load(f)
            return data.get('CLIENT_ID', '').strip(), data.get('ACCESS_TOKEN', '').strip()
        except Exception:
            return '', ''

    def begin_session(self) -> None:
        self.cfg.refresh()
        self.traded_this_scan.clear()

    def _compute_price_levels(
        self, raw_entry: float, signal: str, opt_bump: bool = False
    ) -> PriceLevels:
        inv = _invert_signal(signal)
        entry = _adjust_price(raw_entry, self.entry_perc, signal, opt_bump)
        limit = _adjust_price(entry, self.limit_perc, signal, opt_bump)
        stop_loss = _adjust_price(entry, self.stop_loss_perc, inv, opt_bump)
        stop_limit = _adjust_price(stop_loss, self.limit_perc, inv, opt_bump)
        target = _adjust_price(entry, self.target_perc, signal, opt_bump)
        trail = math.ceil(entry * (self.stop_trail_perc * (10 if opt_bump else 1)) / 100)

        return PriceLevels(entry, limit, stop_loss, stop_limit, target, trail)

    def _get_symbol_config(self, symb: str, exch: str) -> dict:
        dfl = self._defaults_config
        res = {
            'order_mode':   dfl['order_mode'],
            'expiry_date':  dfl['expiry'],
            'quantity':     dfl['quant'],
            'trade_amount': dfl['trade_amount'],
            'call_strike':  None,
            'put_strike':   None,
            'strike':       None,
            'is_index':     False
        }

        if exch == 'NSE':
            nse_cfg = self.cfg.get('nse', {})
            
            indices = nse_cfg.get('indices', {})
            grp_cfg_indices = indices.get('config', {})
            if symb in indices.get('symbols', {}):
                sym_cfg = indices['symbols'][symb]
                res['order_mode'] = sym_cfg.get(
                    'order_mode', grp_cfg_indices.get('order_mode', res['order_mode'])
                )
                res['expiry_date'] = sym_cfg.get(
                    'expiry_date', grp_cfg_indices.get('expiry_date', res['expiry_date'])
                )
                res['quantity']    = sym_cfg.get('quantity', res['quantity'])
                res['call_strike'] = sym_cfg.get('call_strike')
                res['put_strike']  = sym_cfg.get('put_strike')
                res['strike']      = sym_cfg.get('strike')
                res['is_index']    = True
                return res

            stocks = nse_cfg.get('stocks', {})
            grp_cfg_stocks = stocks.get('config', {})
            if symb in stocks.get('symbols', {}):
                sym_cfg = stocks['symbols'][symb]
                res['order_mode'] = sym_cfg.get(
                    'order_mode', grp_cfg_stocks.get('order_mode', res['order_mode'])
                )
                res['expiry_date'] = sym_cfg.get(
                    'expiry_date', grp_cfg_stocks.get('expiry_date', res['expiry_date'])
                )
                res['trade_amount'] = sym_cfg.get(
                    'trade_amount', grp_cfg_stocks.get('trade_amount', res['trade_amount'])
                )
                res['quantity'] = sym_cfg.get('quantity', res['quantity'])
                return res

            res['order_mode']   = grp_cfg_stocks.get('order_mode', res['order_mode'])
            res['expiry_date']  = grp_cfg_stocks.get('expiry_date', res['expiry_date'])
            res['trade_amount'] = grp_cfg_stocks.get('trade_amount', res['trade_amount'])

        elif exch == 'MCX':
            mcx = self.cfg.get('mcx', {}).get('comm', {})
            grp_cfg_mcx = mcx.get('config', {})
            if symb in mcx.get('symbols', {}):
                sym_cfg = mcx['symbols'][symb]
                res['order_mode'] = sym_cfg.get(
                    'order_mode', grp_cfg_mcx.get('order_mode', res['order_mode'])
                )
                res['expiry_date'] = sym_cfg.get(
                    'expiry_date', grp_cfg_mcx.get('expiry_date', res['expiry_date'])
                )
                res['quantity'] = sym_cfg.get('quantity', res['quantity'])

        return res

    def resolve_instrument(
        self,
        symb: str,
        exch: str,
        signal: str,
        quant: int,
        entry_val: float,
        overrides: Optional[UIOverride] = None,
    ) -> Optional[Instrument]:
        overrides = overrides or UIOverride()

        data = self.scrip.get_data_by_display_name(symb)
        if data:
            symb = data['symbol']
            exch = data['exch']
            
            if not overrides.inst_type:
                overrides.inst_type = data['inst_type']
            if overrides.strike <= 0 and data['strike'] > 0:
                overrides.strike = data['strike']
            if not overrides.opt_type and data['opt_type']:
                overrides.opt_type = data['opt_type']
            if not overrides.expiry and data['expiry']:
                overrides.expiry = data['expiry']

        sym_cfg   = self._get_symbol_config(symb, exch)

        ord_mode  = overrides.inst_type or sym_cfg.get('order_mode', 'EQ')
        trade_amt = 0.0 if overrides.force_qty else sym_cfg.get('trade_amount', 0.0)
        fin_quant = quant if (overrides.force_qty or quant > 1) else sym_cfg.get('quantity', 1)

        raw_exp   = str(overrides.expiry or sym_cfg.get('expiry_date', ''))
        exp_parts = raw_exp.split(maxsplit=1)
        expiry    = exp_parts[0] if exp_parts else ""

        seg, opt_type, strike = '', None, None
        fin_signal = signal

        if exch == 'NSE':
            if sym_cfg.get('is_index'):
                match ord_mode:
                    case 'OPT': seg = 'OPTIDX'
                    case 'FUT': seg = 'FUTIDX'
            else:
                match ord_mode:
                    case 'EQ':  seg = 'EQUITY'
                    case 'OPT': seg = 'OPTSTK'
                    case 'FUT': seg = 'FUTSTK'
        elif exch == 'MCX':
            match ord_mode:
                case 'OPT': seg = 'OPTFUT'
                case 'FUT': seg = 'FUTCOM'
        else:
            return None

        if not seg:
            return None

        if ord_mode == 'OPT':
            opt_type   = overrides.opt_type or _signal_to_opt(signal)
            fin_signal = signal if overrides.opt_type else 'BUY'

            if overrides.strike > 0:
                strike = overrides.strike
            else:
                sig_key = 'call_strike' if signal == 'BUY' else 'put_strike'
                strike = (
                    sym_cfg.get(sig_key)
                    or sym_cfg.get('strike')
                    or get_price_strike(symb, entry_val, signal)
                )

        return Instrument(
            symb=symb, exch=exch, seg=seg, expiry_date=expiry,
            signal=fin_signal, quant=fin_quant, strike=strike,
            opt_type=opt_type, entry_val=entry_val, trade_amount=trade_amt,
            trigger_price=overrides.trigger_price, limit_price=overrides.limit_price
        )

    def get_instr_data(self, inst: Instrument) -> tuple[str, str]:
        if inst.seg in self.api_cfg.opt_segments:
            display_symb = f'{inst.symb} {inst.strike} {inst.opt_type} {inst.expiry_date}'
        elif inst.seg in self.api_cfg.fut_segments:
            display_symb = f'{inst.symb} Fut {inst.expiry_date}'
        else:
            display_symb = f'{inst.symb} {inst.seg}'

        seg_suffix = self.api_cfg.maps.get('seg_exchange_suffix', {}).get(inst.seg, '')
        exch_seg = 'IDX_I' if inst.seg == 'INDEX' else f'{inst.exch}_{seg_suffix}'
        return display_symb, exch_seg

    def _get_fallback_strike(self, base: str, strike: float, opt_type: str) -> Optional[float]:
        fb_step = None
        for step in self.api_cfg.fallback_steps:
            if step > get_strike_interval(base, strike):
                fb_step = step
                break

        if fb_step is None:
            return None

        if opt_type == 'CE':
            new_strike = math.floor(strike / fb_step) * fb_step
        else:
            new_strike = math.ceil(strike / fb_step) * fb_step

        return float(new_strike) if new_strike != strike else None

    def lookup_with_fallback(self, inst: Instrument) -> tuple[Optional[str], int]:
        sec_id, lot_size = self.scrip.lookup(
            inst.exch, inst.seg, inst.symb, inst.expiry_date, inst.strike, inst.opt_type
        )
        if sec_id is not None:
            return sec_id, lot_size

        if inst.seg not in self.api_cfg.opt_segments:
            return None, 0

        fb_strike = self._get_fallback_strike(inst.symb, inst.strike, inst.opt_type)
        if fb_strike:
            original_strike = inst.strike
            inst.strike = fb_strike
            sec_id, lot_size = self.scrip.lookup(
                inst.exch, inst.seg, inst.symb, inst.expiry_date, inst.strike, inst.opt_type
            )
            if sec_id is not None:
                return sec_id, lot_size
            inst.strike = original_strike

        return None, 0

    def _request_with_retry(
        self,
        method: str,
        url: str,
        label: str = '',
        retry: bool = True,
        **kwargs,
    ) -> Optional[requests.Response]:
        try:
            return self.session.request(
                method, url, headers=self.api_headers, timeout=10, **kwargs
            )
        except RequestException:
            if retry:
                self.proxy_manager.restart()
                return self._request_with_retry(
                    method, url, label=f'{label} (Retry)', retry=False, **kwargs
                )
            return None

    def _post_order(self, url: str, payload: dict, label: str = '') -> None:
        resp = self._request_with_retry('POST', url, label=label, json=payload)
        if resp and resp.status_code == 200:
            print(f'[✓] {label} Order placed successfully.')
        else:
            err_msg = resp.json() if resp is not None else ""
            print(f'[x] {label} Order Failed.\n{payload}\n{err_msg}')

    def _base_payload(self, signal: str, exchange_seg: str, sec_id: str) -> dict:
        return {
            'dhanClientId':     self.client_id,
            'correlationId':    f'auto_{self.client_id}',
            'transactionType':  signal,
            'exchangeSegment':  exchange_seg,
            'productType':      'INTRADAY',
            'orderType':        'MARKET',
            'validity':         'DAY',
            'securityId':       sec_id,
            'quantity':         0,
            'price':            0,
            'triggerPrice':     0,
            'afterMarketOrder': False,
            'amoTime':          'OPEN',
            'targetPrice':      0,
            'stopLossPrice':    0,
        }

    def _build_alert_payload(
        self,
        alert_exch_seg: str,
        alert_sec_id: str,
        operator: str,
        comp_price: float,
        exp_date: str,
        note: str,
        orders: list[dict],
    ) -> dict:
        return {
            'dhanClientId': self.client_id,
            'condition': {
                'comparisonType':  'PRICE_WITH_VALUE',
                'exchangeSegment': alert_exch_seg,
                'securityId':      alert_sec_id,
                'operator':        operator,
                'comparingValue':  comp_price,
                'expDate':         exp_date,
                'frequency':       'ONCE',
                'userNote':        note,
            },
            'orders': orders,
        }

    def _compute_quantity(
        self, trade_amount: float, price: float, lot_size: int, base_quant: int
    ) -> int:
        if trade_amount > 0 and price > 0:
            lots = math.ceil(trade_amount / (price * lot_size))
            return lots * lot_size
        return base_quant * lot_size

    # ── Order Placement Functions ──────────────────────────────────────────────
    def place_super_order(self, sec_id: str, lot_size: int, inst: Instrument) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        levels = self._compute_price_levels(inst.entry_val, inst.signal)
        total_quant = self._compute_quantity(
            inst.trade_amount, levels.entry, lot_size, inst.quant
        )

        final_limit = inst.limit_price if inst.limit_price > 0 else levels.limit

        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            'orderType':     'LIMIT',
            'quantity':      total_quant,
            'price':         final_limit,
            'stopLossPrice': levels.stop_loss,
            'trailingJump':  levels.trail,
        }
        self._post_order(self.api_cfg.urls.get('super_order', ''), payload, label='SUPER')

    def place_market_order(self, sec_id: str, lot_size: int, inst: Instrument) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            'quantity': inst.quant * lot_size
        }
        self._post_order(self.api_cfg.urls.get('order', ''), payload, label='MARKET')

    def _get_ord_type(self, inst: Instrument) -> str:
        if inst.trigger_price > 0 and inst.limit_price > 0:
            return 'STOP_LOSS'
        if inst.trigger_price > 0:
            return 'STOP_LOSS_MARKET'
        if inst.limit_price > 0:
            return 'LIMIT'
        return 'MARKET'

    def place_simple_order(self, sec_id: str, lot_size: int, inst: Instrument) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        ord_type = self._get_ord_type(inst)

        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            'quantity':     inst.quant * lot_size,
            'orderType':    ord_type,
            'price':        inst.limit_price,
            'triggerPrice': inst.trigger_price
        }
        self._post_order(self.api_cfg.urls.get('order', ''), payload, label=ord_type)

    def place_forever_order(
        self,
        sec_id: str,
        ord_type: str,
        signal: str,
        exchange_seg: str,
        quant: int,
        trigger_price: float,
        limit_price: float = 0.0,
        trigger_price1: float = 0.0,
        is_oco: bool = False,
        product_type: str = 'CNC',
    ) -> None:
        payload = self._base_payload(signal, exchange_seg, sec_id) | {
            'correlationId':     f'cond_{self.client_id}',
            'orderFlag':         'OCO' if is_oco else 'SINGLE',
            'orderType':         ord_type,
            'productType':       product_type,
            'validity':          'DAY',
            'quantity':          quant,
            'disclosedQuantity': 0,
            'triggerPrice':      trigger_price,
            'price':             limit_price,
            'price1':            0,
            'triggerPrice1':     trigger_price1,
            'quantity1':         quant if is_oco else 0,
        }
        self._post_order(self.api_cfg.urls.get('forever_order', ''), payload, label='FOREVER')

    def place_trigger_forever_order(self, sec_id: str, lot_size: int, inst: Instrument) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        levels = self._compute_price_levels(inst.entry_val, inst.signal)
        total_quant = self._compute_quantity(
            inst.trade_amount, levels.entry, lot_size, inst.quant
        )

        product_type = 'MARGIN' if inst.seg in self.api_cfg.fno_segments else 'CNC'
        trig_price   = inst.trigger_price if inst.trigger_price > 0 else levels.entry
        ord_type     = self._get_ord_type(inst)

        if ord_type != 'MARKET':
            ord_type = 'LIMIT'

        self.place_forever_order(
            sec_id, ord_type, inst.signal, exchange_seg,
            total_quant, trig_price, limit_price=inst.limit_price,
            product_type=product_type
        )

    def place_trigger_alert_order(
        self,
        sec_id: str,
        lot_size: int,
        inst: Instrument,
        fno_signal: Optional[str] = None,
    ) -> None:
        _, exchange_seg = self.get_instr_data(inst)
        alert_signal = fno_signal or inst.signal
        levels = self._compute_price_levels(inst.entry_val, alert_signal)
        total_quant = self._compute_quantity(
            inst.trade_amount, levels.entry, lot_size, inst.quant
        )

        ord_payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            'quantity': total_quant
        }

        if alert_signal == 'BUY':
            condition = PriceCondition.GREATER_THAN.value
        else:
            condition = PriceCondition.LESS_THAN.value

        alert_sec_id, alert_exch_seg = sec_id, exchange_seg
        if fno_signal:
            parent_seg = self.api_cfg.maps.get('underlying_seg_map', {}).get(inst.seg, inst.seg)
            
            # Lookup requires the separated parameters
            alert_sec_id, _ = self.scrip.lookup(
                inst.exch, parent_seg, inst.symb, 
                inst.expiry_date, inst.strike, inst.opt_type
            )

            if not alert_sec_id:
                return
            
            parent_suffix = self.api_cfg.maps.get('seg_exchange_suffix', {}).get(parent_seg, '')
            alert_exch_seg = 'IDX_I' if parent_seg == 'INDEX' else f'{inst.exch}_{parent_suffix}'

        payload = self._build_alert_payload(
            alert_exch_seg, alert_sec_id, condition, levels.entry,
            _get_today_str(), 'Main Order', [ord_payload]
        )
        self._post_order(self.api_cfg.urls.get('alert_order', ''), payload, label='ALERT')

    # ── API Getters / Deleters ────────────────────────────────────────────────
    def get_funds(self) -> float:
        resp = self._request_with_retry('GET', self.api_cfg.urls.get('fund_limit', ''), label='GET Funds')
        if not resp or resp.status_code != 200:
            return 0.0
        data = resp.json()
        return float(data.get('availabelBalance', data.get('availableBalance', 0.0)))

    def get_active_positions(self) -> list[dict]:
        resp = self._request_with_retry('GET', self.api_cfg.urls.get('positions', ''), label='GET Positions')
        if resp is None or resp.status_code != 200:
            return []

        active = []
        for pos in resp.json():
            if pos.get('netQty', 0) != 0 and pos.get('tradingSymbol'):
                sec_id = str(pos.get('securityId', ''))
                trade_sym = pos.get('tradingSymbol', '')

                display_sym = self.scrip.get_symbol_name(sec_id, trade_sym)
                base_sym = self.scrip.get_base_symbol(sec_id, trade_sym)
                pnl = float(pos.get('unrealizedProfit', 0.0))
                exch = pos.get('exchangeSegment', 'NSE_EQ').split('_')[0]

                entry = {
                    'display_name': display_sym,
                    'base_symbol':  base_sym,
                    'security_id':  sec_id,
                    'exchange_seg': pos.get('exchangeSegment', 'NSE_EQ'),
                    'exchange':     exch,
                    'pnl':          pnl,
                    'qty':          pos.get('netQty', 0),
                }
                active.append(entry)
        return active

    def get_pending_orders(
        self,
        pending_statuses: tuple[str, ...] = ('TRANSIT', 'PENDING', 'PART_TRADED'),
    ) -> list[dict]:
        resp = self._request_with_retry('GET', self.api_cfg.urls.get('order', ''), label='GET Orders')
        if not resp or resp.status_code != 200:
            return []

        results = []
        for order in resp.json():
            if order.get('orderStatus', '') not in pending_statuses:
                continue

            sec_id = str(order.get('securityId', ''))
            display_sym = self.scrip.get_symbol_name(sec_id, order.get('tradingSymbol', ''))

            entry = {
                'symbol':           display_sym,
                'order_id':         order.get('orderId', ''),
                'type':             order.get('orderType', 'MARKET'),
                'qty':              order.get('quantity', 0),
                'price':            order.get('price', 0.0),
                'trigger_price':    order.get('triggerPrice', 0.0),
                'transaction_type': order.get('transactionType', ''),
            }
            results.append(entry)
        return results

    def get_active_super_orders(self) -> set[tuple]:
        resp = self._request_with_retry('GET', self.api_cfg.urls.get('super_order', ''), label='GET Super Orders')
        if not resp or resp.status_code != 200:
            return set()

        active_orders = set()
        for order in resp.json():
            status = order.get('orderStatus', '')
            sec_id = str(order.get('securityId', ''))

            sym = self.scrip.get_symbol_name(sec_id, order.get('tradingSymbol', ''))
            oid = order.get('orderId', '')
            txn = order.get('transactionType', '')
            qty = order.get('quantity', 0)
            prc = order.get('price', 0.0)
            trg = order.get('triggerPrice', 0.0)

            if status in {'PENDING', 'PART_TRADED'}:
                active_orders.add((sym, oid, 'ENTRY_LEG', txn, qty, prc, trg))

            if status in {'PENDING', 'PART_TRADED', 'TRADED'}:
                for leg in order.get('legDetails', []):
                    if leg.get('orderStatus') == 'PENDING':
                        active_orders.add((
                            sym, oid, leg.get('legName', ''),
                            leg.get('transactionType', txn),
                            leg.get('quantity', qty),
                            leg.get('price', prc),
                            leg.get('triggerPrice', trg)
                        ))
        return active_orders

    def get_forever_orders(
        self,
        active_statuses: tuple[str, ...] = ('PENDING', 'CONFIRM'),
    ) -> list[dict]:
        resp = self._request_with_retry('GET', self.api_cfg.urls.get('forever_order', ''), label='GET Forever Orders')
        if not resp or resp.status_code != 200:
            return []

        results = []
        for order in resp.json():
            if order.get('orderStatus', '') not in active_statuses:
                continue

            sec_id = str(order.get('securityId', ''))
            display_sym = self.scrip.get_symbol_name(sec_id, order.get('tradingSymbol', ''))

            entry = {
                'symbol':           display_sym,
                'order_id':         order.get('orderId', ''),
                'type':             'FOREVER',
                'leg':              order.get('legName', 'TARGET_LEG'),
                'qty':              order.get('quantity', 0),
                'price':            order.get('price', 0.0),
                'trigger_price':    order.get('triggerPrice', 0.0),
                'transaction_type': order.get('transactionType', ''),
                'flag':             order.get('orderType', 'SINGLE'),
            }
            results.append(entry)
        return results

    def get_all_alerts(self, active_statuses: tuple[str, ...] = ('ACTIVE',)) -> list[dict]:
        resp = self._request_with_retry('GET', self.api_cfg.urls.get('alert_order', ''), label='GET Alert Orders')
        if not resp or resp.status_code != 200:
            return []

        results = []
        for alert in resp.json():
            if alert.get('alertStatus', '') not in active_statuses:
                continue

            cond    = alert.get('condition', {})
            orders  = alert.get('orders', [{}])

            sec_id  = str(orders[0].get('securityId', '')) if orders else ''
            qty     = orders[0].get('quantity', 0) if orders else 0
            prc     = orders[0].get('price', 0.0) if orders else 0.0
            txn     = orders[0].get('transactionType', '') if orders else ''

            display_sym = self.scrip.get_symbol_name(sec_id, f"Trig: {sec_id}")

            entry = {
                'symbol':           display_sym,
                'order_id':         alert.get('alertId', ''),
                'type':             'ALERT',
                'leg':              '',
                'qty':              qty,
                'price':            prc,
                'transaction_type': txn,
                'condition_note':   cond.get('userNote', ''),
                'comparing_value':  cond.get('comparingValue', 0.0),
                'exp_date':         cond.get('expDate', ''),
            }
            results.append(entry)
        return results

    def cancel_normal_order(self, order_id: str) -> bool:
        return self._request_with_retry(
            'DELETE', f"{self.api_cfg.urls.get('order', '')}/{order_id}", label=f'Cancel {order_id}'
        ) is not None

    def cancel_super_order(self, order_id: str, order_leg: str = 'ENTRY_LEG') -> bool:
        return self._request_with_retry(
            'DELETE',
            f"{self.api_cfg.urls.get('super_order', '')}/{order_id}/{order_leg}",
            label=f'Cancel {order_id}'
        ) is not None

    def cancel_forever_order(self, order_id: str) -> bool:
        return self._request_with_retry(
            'DELETE', f"{self.api_cfg.urls.get('forever_order', '')}/{order_id}", label=f'Cancel {order_id}'
        ) is not None

    def cancel_alert_order(self, alert_id: str) -> bool:
        return self._request_with_retry(
            'DELETE', f"{self.api_cfg.urls.get('alert_order', '')}/{alert_id}", label=f'Cancel {alert_id}'
        ) is not None

    def close_position_by_secid(self, sec_id: str, exchange_seg: str, net_qty: int) -> None:
        if net_qty == 0:
            return

        signal = 'SELL' if net_qty > 0 else 'BUY'
        payload = self._base_payload(signal, exchange_seg, sec_id) | {'quantity': abs(net_qty)}
        self._post_order(self.api_cfg.urls.get('order', ''), payload, label='CLOSE_POS')

    def dispatch_order(self, sec_id: str, lot_size: int, inst: Instrument, signal: str) -> None:
        place_order_mode = self._defaults_config.get('place_order_mode')

        if inst.seg in self.api_cfg.opt_segments and inst.exch == 'MCX':
            self.place_simple_order(sec_id, lot_size, inst)
            return

        if inst.seg in self.api_cfg.fno_segments:
            placed_order = False
            match place_order_mode:
                case 'ALERT':
                    if inst.exch != 'MCX':
                        self.place_trigger_alert_order(sec_id, lot_size, inst, fno_signal=signal)
                        placed_order = True
                case 'FOREVER':
                    if inst.seg not in self.api_cfg.opt_segments:
                        self.place_trigger_forever_order(sec_id, lot_size, inst)
                        placed_order = True
                case 'SUPER':
                    if inst.seg not in self.api_cfg.opt_segments:
                        self.place_super_order(sec_id, lot_size, inst)
                        placed_order = True
                case _:
                    self.place_simple_order(sec_id, lot_size, inst)
                    placed_order = True

            if not placed_order:
                self.place_simple_order(sec_id, lot_size, inst)
            return

        match place_order_mode:
            case 'ALERT':
                self.place_trigger_alert_order(sec_id, lot_size, inst)
            case 'SUPER':
                self.place_super_order(sec_id, lot_size, inst)
            case 'FOREVER':
                self.place_trigger_forever_order(sec_id, lot_size, inst)
            case _:
                self.place_simple_order(sec_id, lot_size, inst)

    def fire_trade(
        self, symb: str, exch: str, signal: str, quant: int = 1, entry_val: float = 0.0
    ) -> None:
        trade_key = f'{exch}:{symb}:{signal}'
        if trade_key in self.traded_this_scan:
            print(f'[skip] {trade_key} already traded this scan cycle.')
            return

        self.traded_this_scan.add(trade_key)

        inst = self.resolve_instrument(symb, exch, signal, quant, entry_val)
        if inst is None:
            return

        sec_id, lot_size = self.lookup_with_fallback(inst)
        if sec_id is None:
            return

        self.dispatch_order(sec_id, lot_size, inst, signal)

    def clean_orphaned_orders(self) -> None:
        print('\n─── Starting Cleanup Cycle ───')
        active_positions = self.get_active_positions()
        active_symbols   = {pos.get('display_name') for pos in active_positions}
        active_super_orders = self.get_active_super_orders()

        cancelled = [
            symb for symb, oid, leg, *_ in active_super_orders
            if symb not in active_symbols and self.cancel_super_order(oid, leg)
        ]

        if cancelled:
            print(f'\n[!] Cleanup Complete. Cancelled {len(cancelled)} orphaned orders.')
            print(f'    Symbols: {cancelled}')
        else:
            print('\n[✓] Cleanup Complete. No orphaned orders found.')


# ───────────────────────────────────────
# Test Execution
# ───────────────────────────────────────
if __name__ == '__main__':
    trader = DhanTrader()
    trader.begin_session()

    res = trader.scrip.search_symbols("sbin 980 call jun")
    print(res)