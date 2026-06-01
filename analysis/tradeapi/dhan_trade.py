"""""
dhan_trade.py — Dhan HQ automated order placement (Object-Oriented).
"""

import math
import requests
from requests.exceptions import RequestException
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import tomllib

from utils.data.paths import OUT_DIR
from tradeapi.price_strike_calc import get_price_strike, get_strike_interval

from utils.network.start_proxy import SSHProxyManager
from datetime import datetime

from enum import Enum
from types import MappingProxyType

# ───────────────────────────────────────
# Paths & Pure Constants
# ───────────────────────────────────────
BASE_DIR         = Path(__file__).parent
SYMBOLS_CONFIG   = BASE_DIR / 'symbols_config.toml'
LOCAL_CSV        = Path(OUT_DIR) / 'scrip_master.csv'
ACCESS_FILE_PATH = BASE_DIR / 'access_token.toml'

ORDER_URL              = 'https://api.dhan.co/v2/orders'
POSITIONS_URL          = 'https://api.dhan.co/v2/positions'
SUPER_ORDER_URL        = 'https://api.dhan.co/v2/super/orders'
FOREVER_ORDER_URL      = 'https://api.dhan.co/v2/forever/orders'
FOREVER_ALL_URL        = 'https://api.dhan.co/v2/forever/orders'
ALERT_ORDER_URL        = 'https://api.dhan.co/v2/alerts/orders'

INSTRUMENT_SEGMENTS = ['NSE_EQ', 'NSE_FNO', 'MCX_COMM', 'IDX_I']
INSTRUMENT_URL      = 'https://api.dhan.co/v2/instrument/{segment}'

FILTER_SEG  = frozenset({'EQUITY', 'INDEX', 'OPTSTK', 'OPTIDX', 'OPTFUT', 'FUTIDX', 'FUTCOM', 'FUTSTK'})
FILTER_EXCH = frozenset({'NSE', 'MCX'})
SCRIP_COLS  = [
    'EXCH_ID', 'INSTRUMENT',
    'UNDERLYING_SYMBOL', 'SECURITY_ID', 'UNDERLYING_SECURITY_ID',
    'LOT_SIZE', 'SM_EXPIRY_DATE', 'STRIKE_PRICE', 'OPTION_TYPE',
]

SEG_EXCHANGE_SUFFIX = MappingProxyType({
    'EQUITY': 'EQ', 'OPTFUT': 'COMM', 'FUTCOM': 'COMM',
    'OPTIDX': 'FNO', 'OPTSTK': 'FNO', 'FUTIDX': 'FNO', 'FUTSTK': 'FNO',
})

OPT_SEGMENTS = frozenset({'OPTSTK', 'OPTIDX', 'OPTFUT'})
FUT_SEGMENTS = frozenset({'FUTIDX', 'FUTCOM', 'FUTSTK'})
FNO_SEGMENTS = OPT_SEGMENTS | FUT_SEGMENTS

UNDERLYING_SEG_MAP = MappingProxyType({
    'OPTSTK': 'EQUITY', 'FUTSTK': 'EQUITY',
    'OPTFUT': 'FUTCOM', 'OPTIDX': 'INDEX',  'FUTIDX': 'INDEX',
})

EQ_INDEX_INSTR = frozenset({'EQUITY', 'INDEX'})

# ───────────────────────────────────────
# Pure utility helpers
# ───────────────────────────────────────
def _signal_to_opt(signal: str) -> str:
    return 'CE' if signal == 'BUY' else 'PE'

def _invert_signal(signal: str) -> str:
    return 'SELL' if signal == 'BUY' else 'BUY'

def _adjust_price(base: float, perc: float, signal: str, opt_bump: bool = False) -> float:
    perc = 10 * perc if opt_bump else perc
    return math.ceil(base * (1 + perc / 100)) if signal == 'BUY' else math.floor(base * (1 - perc / 100))

def _get_today_str() -> str:
    return datetime.now().strftime('%Y-%m-%d')

def _year_end_str() -> str:
    return datetime.now().replace(month=12, day=31).strftime('%Y-%m-%d')

_FALLBACK_STEPS = (1, 2, 5, 10, 20, 25, 50, 100, 200, 500, 1_000, 5_000)

def _next_round_step(current_step: int) -> Optional[int]:
    for s in _FALLBACK_STEPS:
        if s > current_step: return s
    return None

def _get_fallback_strike(base: str, strike: float, opt_type: str) -> Optional[float]:
    fb_step = _next_round_step(get_strike_interval(base, strike))
    if fb_step is None: return None
    new_strike = math.floor(strike / fb_step) * fb_step if opt_type == 'CE' else math.ceil(strike / fb_step) * fb_step
    return float(new_strike) if new_strike != strike else None


# ───────────────────────────────────────
# Data Classes
# ───────────────────────────────────────
@dataclass
class PriceLevels:
    entry:      float
    limit:      float
    stop_loss:  float
    stop_limit: float
    target:     float
    trail:      float

def compute_quantity(trade_amount: float, price: float, lot_size: int, base_quant: int) -> int:
    if trade_amount > 0 and price > 0:
        lots = int(trade_amount // (price * lot_size)) + 1
        return lots * lot_size
    return base_quant * lot_size

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
    trigger_price: float           = 0.0  # Added
    limit_price:   float           = 0.0  # Added

@dataclass
class UIOverride:
    inst_type:     str             = ""
    strike:        float           = 0.0
    expiry:        str             = ""
    limit_price:   float           = 0.0
    trigger_price: float           = 0.0
    force_qty:     bool            = False
    opt_type:      Optional[str]   = None
    

# ───────────────────────────────────────
# Config Manager
# ───────────────────────────────────────
class SymbolsConfig:
    def __init__(self, path: Path):
        self._path = path
        self._mtime = None
        self._config = {}

    def get(self, key: str, default=None):
        self.refresh()
        return self._config.get(key, default)

    def refresh(self, retry=True):
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

        if self._mtime == mtime: return
        
        if self._mtime is not None:
            print('Symbols config file changed — reloading.')
        self._mtime = mtime
        
        try:
            with open(self._path, 'rb') as f:
                self._config = tomllib.load(f) or {}
            print('Symbol config loaded.')
        except Exception as exc:
            rtry_str = '[Retry] ' if not retry else '' 
            print(f'{rtry_str}Failed to parse {self._path} TOML config: {exc}')


# ───────────────────────────────────────
# ScripMaster
# ───────────────────────────────────────
class ScripMaster:
    def __init__(self, session_obj,refresh_master_scrip: bool = False):
        self._eq_index     = None
        self._opt_index    = None
        self._expiry_index = None
        self._secid_info   = None
        self.session = session_obj
        self._ensure_loaded(refresh_master_scrip)
        

    def get_symbol_name(self, sec_id: str, fallback: str = "") -> str:
        if not self._secid_info: return fallback
        key = self._secid_info.get(str(sec_id))
        if not key: return fallback
        
        exch_id, inst, underlying, exp, strike, opt_type = key
        if inst in OPT_SEGMENTS: return f"{underlying} {strike} {opt_type} {exp}"
        elif inst in FUT_SEGMENTS: return f"{underlying} FUT {exp}"
        return underlying

    def get_base_symbol(self, sec_id: str, fallback: str = "") -> str:
        if not self._secid_info: return fallback
        key = self._secid_info.get(str(sec_id))
        return key[2] if key else fallback

    def _ensure_loaded(self, refresh_master_scrip: bool = False):
        if self._eq_index is not None: return
        if not LOCAL_CSV.exists() or refresh_master_scrip:
            raw_df = self._download_segments()
            if raw_df is not None: self._save_and_index(raw_df)
        else:
            self._index_from_csv()

    def _download_segments(self) -> Optional[pd.DataFrame]:
        import io
        frames = []
        for segment in INSTRUMENT_SEGMENTS:
            try:
                resp = self.session.request('GET',INSTRUMENT_URL.format(segment=segment), timeout=5)
                resp.raise_for_status()
                df = pd.read_csv(io.StringIO(resp.text), usecols=lambda c: c in SCRIP_COLS, low_memory=False)
                if 'EXCH_ID' in df.columns and 'INSTRUMENT' in df.columns:
                    df = df[df['EXCH_ID'].isin(FILTER_EXCH) & df['INSTRUMENT'].isin(FILTER_SEG)]
                frames.append(df)
            except Exception as exc:
                print(f'Failed to download {segment}: {exc}')
        return pd.concat(frames, ignore_index=True) if frames else None

    def _save_and_index(self, df: pd.DataFrame):
        today_str = _get_today_str()
        is_eq = df['INSTRUMENT'].isin(EQ_INDEX_INSTR)
        expiry_dates = df['SM_EXPIRY_DATE'].astype(str).str.split().str[0]
        df = df[is_eq | (~is_eq & (expiry_dates >= today_str))].copy()

        df.to_csv(LOCAL_CSV, index=False)
        eq_index, opt_index, expiry_index, secid_info = {}, {}, {}, {}
        self._fold_chunk(df, eq_index, opt_index, expiry_index, secid_info, today_str)
        self._commit_indexes(eq_index, opt_index, expiry_index, secid_info)

    def _index_from_csv(self):
        eq_index, opt_index, expiry_index, secid_info = {}, {}, {}, {}
        today_str = _get_today_str()
        try:
            with pd.read_csv(LOCAL_CSV, usecols=SCRIP_COLS, chunksize=50_000, low_memory=False) as reader:
                for chunk in reader:
                    self._fold_chunk(chunk, eq_index, opt_index, expiry_index, secid_info, today_str)
        except Exception as exc:
            print(f'Error loading scrip master: {exc}')
            return
        self._commit_indexes(eq_index, opt_index, expiry_index, secid_info)

    def _commit_indexes(self, eq_index: dict, opt_index: dict, expiry_index: dict, secid_info: dict):
        self._eq_index     = eq_index
        self._opt_index    = opt_index
        self._expiry_index = expiry_index
        self._secid_info   = secid_info

    @staticmethod
    def _fold_chunk(chunk: pd.DataFrame, eq_index: dict, opt_index: dict, expiry_index: dict, secid_info: dict, today_str: str) -> None:
        if chunk.empty: return

        expiry_strs = chunk['SM_EXPIRY_DATE'].astype(str).str.split().str[0]
        eq_mask     = chunk['INSTRUMENT'].isin(EQ_INDEX_INSTR)

        for row in chunk[eq_mask].itertuples(index=False):
            str_sec_id = str(row.SECURITY_ID)
            eq_index[(row.EXCH_ID, row.UNDERLYING_SYMBOL)] = (str_sec_id, int(row.LOT_SIZE))
            secid_info[str_sec_id] = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL, None, None, None)

        for row, exp in zip(chunk[~eq_mask].itertuples(index=False), expiry_strs[~eq_mask]):
            if exp < today_str: continue

            base_key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL)
            expiry_index.setdefault(base_key, set()).add(exp)

            strike, opt_type = None, None
            if row.INSTRUMENT in OPT_SEGMENTS:
                strike   = float(row.STRIKE_PRICE)
                opt_type = str(row.OPTION_TYPE).strip().upper()

            key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL, exp, strike, opt_type)
            if key not in opt_index:
                str_sec_id = str(row.SECURITY_ID)
                opt_index[key]         = (str_sec_id, int(row.LOT_SIZE))
                secid_info[str_sec_id] = key 

    def lookup(self, inst: Instrument, *, silent: bool = False) -> tuple[Optional[str], int]:
        self._ensure_loaded()
        if inst.seg in EQ_INDEX_INSTR:
            return self._eq_index.get((inst.exch, inst.symb)) if self._eq_index else (None, 0)

        valid_expiries = self._expiry_index.get((inst.exch, inst.seg, inst.symb))
        if not valid_expiries: return None, 0

        date_key = inst.expiry_date
        if not date_key or date_key not in valid_expiries:
            date_key = sorted(valid_expiries)[0]
            inst.expiry_date = date_key

        result = self._opt_index.get((inst.exch, inst.seg, inst.symb, date_key, inst.strike, inst.opt_type))
        return result if result else (None, 0)

    def lookup_with_fallback(self, inst: Instrument) -> tuple[Optional[str], int]:
        sec_id, lot_size = self.lookup(inst, silent=True)
        if sec_id is not None: return sec_id, lot_size
        if inst.seg not in OPT_SEGMENTS: return None, 0

        fb_strike = _get_fallback_strike(inst.symb, inst.strike, inst.opt_type)
        if fb_strike:
            original_strike = inst.strike
            inst.strike     = fb_strike
            sec_id, lot_size = self.lookup(inst, silent=True)
            if sec_id is not None: return sec_id, lot_size
            inst.strike = original_strike

        return None, 0


# ───────────────────────────────────────
# Core Dhan API class
# ───────────────────────────────────────
class DhanTrader:
    class PriceCondition(Enum):
        GREATER_THAN = 'GREATER_THAN'
        LESS_THAN    = 'LESS_THAN'

    def __init__(self, symb_config=SYMBOLS_CONFIG, refresh_master_scrip: bool = False, restart_proxy=False):
        self.cfg = SymbolsConfig(symb_config)
        self._defaults_config = None
        self._set_defaults_config()
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

        self.scrip = ScripMaster(session_obj=self.session,refresh_master_scrip=refresh_master_scrip)

        self.entry_perc      = self.cfg.get('entry_price_perc', 0.1)
        self.limit_perc      = self.cfg.get('limit_price_perc', 0.2)
        self.target_perc     = self.cfg.get('target_perc', 4.0)
        self.stop_loss_perc  = self.cfg.get('stop_loss_perc', 0.7)
        self.stop_trail_perc = self.cfg.get('stop_trail_perc', 0.5)

    def _apply_proxy(self):
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

    def begin_session(self):
        self.cfg.refresh()
        self.traded_this_scan.clear()

    def _defaults(self) -> dict:
        return self._defaults_config

    def _set_defaults_config(self):
        self._defaults_config = MappingProxyType({
            'expiry':           self.cfg.get('def_expiry_date', ''),
            'quant':            self.cfg.get('def_quantity', 1),
            'trade_amount':     self.cfg.get('def_trade_amount', 10000),
            'order_mode':       self.cfg.get('def_order_mode', ''),
            'place_order_mode': self.cfg.get('place_order_mode', 'MARKET')
        })

    def _compute_price_levels(self, raw_entry: float, signal: str, opt_bump: bool = False) -> PriceLevels:
        inv        = _invert_signal(signal)
        entry      = _adjust_price(raw_entry, self.entry_perc,     signal, opt_bump)
        limit      = _adjust_price(entry,     self.limit_perc,     signal, opt_bump)
        stop_loss  = _adjust_price(entry,     self.stop_loss_perc, inv,    opt_bump)
        stop_limit = _adjust_price(stop_loss, self.limit_perc,     inv,    opt_bump)
        target     = _adjust_price(entry,     self.target_perc,    signal, opt_bump)
        trail      = math.ceil(entry * (self.stop_trail_perc * (10 if opt_bump else 1)) / 100)
        return PriceLevels(entry, limit, stop_loss, stop_limit, target, trail)

    def _get_symbol_config(self, symb: str, exch: str) -> dict:
        """Returns a flat, clean dictionary of configuration variables for the requested symbol."""
        dfl = self._defaults()
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
            
            # Check Indices
            indices = nse_cfg.get('indices', {})
            if symb in indices.get('symbols', {}):
                sym_cfg = indices['symbols'][symb]
                grp_cfg = indices.get('config', {})
                res['order_mode']  = sym_cfg.get('order_mode', grp_cfg.get('order_mode', res['order_mode']))
                res['expiry_date'] = sym_cfg.get('expiry_date', grp_cfg.get('expiry_date', res['expiry_date']))
                res['quantity']    = sym_cfg.get('quantity', res['quantity'])
                res['call_strike'] = sym_cfg.get('call_strike')
                res['put_strike']  = sym_cfg.get('put_strike')
                res['strike']      = sym_cfg.get('strike')
                res['is_index']    = True
                return res
            
            # Check Stocks
            stocks = nse_cfg.get('stocks', {})
            if symb in stocks.get('symbols', {}):
                sym_cfg = stocks['symbols'][symb]
                grp_cfg = stocks.get('config', {})
                res['order_mode']   = sym_cfg.get('order_mode', grp_cfg.get('order_mode', res['order_mode']))
                res['expiry_date']  = sym_cfg.get('expiry_date', grp_cfg.get('expiry_date', res['expiry_date']))
                res['quantity']     = sym_cfg.get('quantity', res['quantity'])
                res['trade_amount'] = sym_cfg.get('trade_amount', grp_cfg.get('trade_amount', res['trade_amount']))
                return res

            # Fallback to general stock rules if not specifically listed
            grp_cfg = stocks.get('config', {})
            res['order_mode']   = grp_cfg.get('order_mode', res['order_mode'])
            res['expiry_date']  = grp_cfg.get('expiry_date', res['expiry_date'])
            res['trade_amount'] = grp_cfg.get('trade_amount', res['trade_amount'])

        elif exch == 'MCX':
            mcx = self.cfg.get('mcx', {}).get('comm', {})
            if symb in mcx.get('symbols', {}):
                sym_cfg = mcx['symbols'][symb]
                grp_cfg = mcx.get('config', {})
                res['order_mode']  = sym_cfg.get('order_mode', grp_cfg.get('order_mode', res['order_mode']))
                res['expiry_date'] = sym_cfg.get('expiry_date', grp_cfg.get('expiry_date', res['expiry_date']))
                res['quantity']    = sym_cfg.get('quantity', res['quantity'])

        return res

    def resolve_instrument(self, symb: str, exch: str, signal: str, quant: int, entry_val: float, overrides: UIOverride = None) -> Optional[Instrument]:
        """A clean, unified path for determining instrument parameters."""
        overrides = overrides or UIOverride()
        sym_cfg   = self._get_symbol_config(symb, exch)

        # 1. Apply UI Overrides
        ord_mode  = overrides.inst_type or sym_cfg.get('order_mode', 'EQ')
        trade_amt = 0.0 if overrides.force_qty else sym_cfg.get('trade_amount', 0.0)
        fin_quant = quant if (overrides.force_qty or quant > 1) else sym_cfg.get('quantity', 1)

        # 2. Safely parse expiry
        raw_exp   = str(overrides.expiry or sym_cfg.get('expiry_date', ''))
        exp_parts = raw_exp.split(maxsplit=1)
        expiry    = exp_parts[0] if exp_parts else ""

        # 3. Base assumptions
        seg, opt_type, strike = '', None, None
        fin_signal = signal

        # 4. Resolve Segment
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
        
        if not seg: return None

        # 5. Resolve Options-Specific Data
        if ord_mode == 'OPT':
            opt_type   = overrides.opt_type or _signal_to_opt(signal)
            fin_signal = signal if overrides.opt_type else 'BUY'
            
            if overrides.strike > 0:
                strike = overrides.strike
            else:
                sig_key = 'call_strike' if signal == 'BUY' else 'put_strike'
                strike  = sym_cfg.get(sig_key) or sym_cfg.get('strike') or get_price_strike(symb, entry_val, signal)

        # Injects the explicit trigger/limit overrides into the final Instrument object
        return Instrument(
            symb=symb, exch=exch, seg=seg, expiry_date=expiry,
            signal=fin_signal, quant=fin_quant, strike=strike,
            opt_type=opt_type, entry_val=entry_val, trade_amount=trade_amt,
            trigger_price=overrides.trigger_price, limit_price=overrides.limit_price
        )

    def get_instr_data(self, inst: Instrument) -> tuple[str, str]:
        if inst.seg in OPT_SEGMENTS:
            display_symb = f'{inst.symb} {inst.strike} {inst.opt_type} {inst.expiry_date}'
        elif inst.seg in FUT_SEGMENTS:
            display_symb = f'{inst.symb} Fut {inst.expiry_date}'
        else:
            display_symb = f'{inst.symb} {inst.seg}'
            
        exch_seg = 'IDX_I' if inst.seg == 'INDEX' else f'{inst.exch}_{SEG_EXCHANGE_SUFFIX[inst.seg]}'
        return display_symb, exch_seg

    def _request_with_retry(self, method: str, url: str, label: str = '', retry: bool = True, **kwargs) -> Optional[requests.Response]:
        try:
            return self.session.request(method, url, headers=self.api_headers, timeout=10, **kwargs)
        except RequestException as exc:
            if retry:
                self.proxy_manager.restart()
                return self._request_with_retry(method, url, label=f'{label} (Retry)', retry=False, **kwargs)
            return None

    def _post_order(self, url: str, payload: dict, label: str = ''):
        resp = self._request_with_retry('POST', url, label=label, json=payload)
        if resp and resp.status_code == 200:
            print(f'[✓] {label} Order placed successfully.')
        else:
            print(f'[x] {label} Order Failed.\n{payload}\n{resp.json() if resp is not None else ""}')

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

    def _build_alert_payload(self, alert_exch_seg: str, alert_sec_id: str, operator: str, comp_price: float, exp_date: str, note: str, orders: list[dict]) -> dict:
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

    # ── Order Placement Functions ──────────────────────────────────────────────
    def place_super_order(self, sec_id: str, lot_size: int, inst: Instrument):
        _, exchange_seg = self.get_instr_data(inst)
        levels = self._compute_price_levels(inst.entry_val, inst.signal)
        total_quant = compute_quantity(inst.trade_amount, levels.entry, lot_size, inst.quant)
        
        # Pull explicitly from the modified inst
        final_limit = inst.limit_price if inst.limit_price > 0 else levels.limit

        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            'orderType':     'LIMIT', 
            'quantity':      total_quant, 
            'price':         final_limit,
            'stopLossPrice': levels.stop_loss, 
            'trailingJump':  levels.trail,
        }
        self._post_order(SUPER_ORDER_URL, payload, label='SUPER')

    def place_market_order(self, sec_id: str, lot_size: int, inst: Instrument):
        _, exchange_seg = self.get_instr_data(inst)
        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {'quantity': inst.quant * lot_size}
        self._post_order(ORDER_URL, payload, label='MARKET')
    
    def _get_ord_type(self,inst):

        ord_type = 'MARKET'
        
        if inst.trigger_price > 0 and inst.limit_price > 0:
            ord_type = 'STOP_LOSS'
        elif inst.trigger_price > 0 and inst.limit_price == 0:
            ord_type = 'STOP_LOSS_MARKET'
        elif inst.trigger_price == 0 and inst.limit_price > 0:
            ord_type = 'LIMIT'
        
        return ord_type
    
    def place_simple_order(self, sec_id: str, lot_size: int, inst: Instrument):
        """Intelligently fires a Market, Limit, or Stop Loss order based on provided price parameters."""
        _, exchange_seg = self.get_instr_data(inst)
 
        ord_type = self._get_ord_type(inst)

        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            'quantity':     inst.quant * lot_size, 
            'orderType':    ord_type, 
            'price':        inst.limit_price, 
            'triggerPrice': inst.trigger_price 
        }
        self._post_order(ORDER_URL, payload, label=ord_type)

    def place_forever_order(self, sec_id: str, ord_type: str, signal: str, exchange_seg: str, quant: int, trigger_price: float, limit_price: float = 0.0, trigger_price1: float = 0, is_oco: bool = False, product_type: str = 'CNC'):
        
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
        self._post_order(FOREVER_ORDER_URL, payload, label='FOREVER')

    def place_trigger_forever_order(self, sec_id: str, lot_size: int, inst: Instrument):
        _, exchange_seg = self.get_instr_data(inst)
        levels = self._compute_price_levels(inst.entry_val, inst.signal)
        total_quant = compute_quantity(inst.trade_amount, levels.entry, lot_size, inst.quant)
        
        product_type = 'MARGIN' if inst.seg in FUT_SEGMENTS else 'CNC'
        trig_price   = inst.trigger_price if inst.trigger_price > 0 else levels.entry
        ord_type = self._get_ord_type(inst)
        if ord_type != 'MARKET':
            ord_type = 'LIMIT'

        self.place_forever_order(sec_id, ord_type, inst.signal, exchange_seg, total_quant, trig_price, limit_price=inst.limit_price, product_type=product_type)

    def place_trigger_alert_order(self, sec_id: str, lot_size: int, inst: Instrument, fno_signal: str = None):
        _, exchange_seg = self.get_instr_data(inst)
        alert_signal = fno_signal or inst.signal
        levels = self._compute_price_levels(inst.entry_val, alert_signal)
        total_quant = compute_quantity(inst.trade_amount, levels.entry, lot_size, inst.quant)

        ord_payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {'quantity': total_quant}
        condition = DhanTrader.PriceCondition.GREATER_THAN.value if alert_signal == 'BUY' else DhanTrader.PriceCondition.LESS_THAN.value

        alert_sec_id, alert_exch_seg = sec_id, exchange_seg
        if fno_signal:
            parent_seg = UNDERLYING_SEG_MAP.get(inst.seg, inst.seg)
            parent_instr = Instrument(symb=inst.symb, exch=inst.exch, seg=parent_seg)
            alert_sec_id, _ = self.scrip.lookup(parent_instr)
            if not alert_sec_id: return
            _, alert_exch_seg = self.get_instr_data(parent_instr)

        payload = self._build_alert_payload(alert_exch_seg, alert_sec_id, condition, levels.entry, _get_today_str(), 'Main Order', [ord_payload])
        self._post_order(ALERT_ORDER_URL, payload, label='ALERT')

    # ── API Getters / Deleters ────────────────────────────────────────────────
    def get_active_positions(self) -> list[dict]:
        resp = self._request_with_retry('GET', POSITIONS_URL, label='GET Positions')
        if resp is None or resp.status_code != 200: return []
        active = []
        for pos in resp.json():
            if pos.get('netQty', 0) != 0 and pos.get('tradingSymbol'):
                sec_id = str(pos.get('securityId', ''))
                trade_sym = pos.get('tradingSymbol', '')
                display_sym = self.scrip.get_symbol_name(sec_id, trade_sym)
                base_sym = self.scrip.get_base_symbol(sec_id, trade_sym)
                pnl =  float(pos.get('unrealizedProfit', 0.0)) + float(pos.get('realizedProfit', 0.0))
                exch = pos.get('exchangeSegment', 'NSE_EQ').split('_')[0]
                active.append({'display_name': display_sym, 'base_symbol': base_sym, 'security_id': sec_id, 'exchange_seg': pos.get('exchangeSegment', 'NSE_EQ'), 'exchange': exch, 'pnl': pnl, 'qty': pos.get('netQty', 0)})
        return active

    def get_pending_orders(self, pending_statuses: tuple[str, ...] = ('TRANSIT', 'PENDING', 'PART_TRADED')) -> list[dict]:
        resp = self._request_with_retry('GET', ORDER_URL, label='GET Orders')
        if not resp or resp.status_code != 200: return []
        results = []
        for order in resp.json():
            if order.get('orderStatus', '') not in pending_statuses: continue
            sec_id = str(order.get('securityId', ''))
            display_sym = self.scrip.get_symbol_name(sec_id, order.get('tradingSymbol', ''))
            results.append({'symbol': display_sym, 'order_id': order.get('orderId', ''), 'type': order.get('orderType', 'MARKET'), 'qty': order.get('quantity', 0), 'price': order.get('price', 0.0), 'trigger_price': order.get('triggerPrice', 0.0), 'transaction_type': order.get('transactionType', '')})
        return results

    def get_active_super_orders(self) -> set[tuple]:
        resp = self._request_with_retry('GET', SUPER_ORDER_URL, label='GET Super Orders')
        if not resp or resp.status_code != 200: return set()
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
                        active_orders.add((sym, oid, leg.get('legName', ''), leg.get('transactionType', txn), leg.get('quantity', qty), leg.get('price', prc), leg.get('triggerPrice', trg)))
        return active_orders

    def get_forever_orders(self, active_statuses: tuple[str, ...] = ('PENDING', 'CONFIRM')) -> list[dict]:
        resp = self._request_with_retry('GET', FOREVER_ALL_URL, label='GET Forever Orders')
        if not resp or resp.status_code != 200: return []
        results = []
        for order in resp.json():
            if order.get('orderStatus', '') not in active_statuses: continue
            sec_id = str(order.get('securityId', ''))
            display_sym = self.scrip.get_symbol_name(sec_id, order.get('tradingSymbol', ''))
            results.append({'symbol': display_sym, 'order_id': order.get('orderId', ''), 'type': 'FOREVER', 'leg': order.get('legName', 'TARGET_LEG'), 'qty': order.get('quantity', 0), 'price': order.get('price', 0.0), 'trigger_price': order.get('triggerPrice', 0.0), 'transaction_type': order.get('transactionType', ''), 'flag': order.get('orderType', 'SINGLE')})
        return results

    def get_all_alerts(self, active_statuses: tuple[str, ...] = ('ACTIVE',)) -> list[dict]:
        resp = self._request_with_retry('GET', ALERT_ORDER_URL, label='GET Alert Orders')
        if not resp or resp.status_code != 200: return []
        results = []
        for alert in resp.json():
            if alert.get('alertStatus', '') not in active_statuses: continue
            cond    = alert.get('condition', {})
            orders  = alert.get('orders', [{}])
            sec_id  = str(orders[0].get('securityId', '')) if orders else ''
            qty     = orders[0].get('quantity', 0) if orders else 0
            prc     = orders[0].get('price', 0.0) if orders else 0.0
            txn     = orders[0].get('transactionType', '') if orders else ''
            
            display_sym = self.scrip.get_symbol_name(sec_id, f"Trig: {sec_id}")
            results.append({'symbol': display_sym, 'order_id': alert.get('alertId', ''), 'type': 'ALERT', 'leg': '', 'qty': qty, 'price': prc, 'transaction_type': txn, 'condition_note': cond.get('userNote', ''), 'comparing_value': cond.get('comparingValue', 0.0), 'exp_date': cond.get('expDate', '')})
        return results

    def cancel_normal_order(self, order_id: str) -> bool: return self._request_with_retry('DELETE', f'{ORDER_URL}/{order_id}', label=f'Cancel {order_id}') is not None
    def cancel_super_order(self, order_id: str, order_leg: str = 'ENTRY_LEG') -> bool: return self._request_with_retry('DELETE', f'{SUPER_ORDER_URL}/{order_id}/{order_leg}', label=f'Cancel {order_id}') is not None
    def cancel_forever_order(self, order_id: str) -> bool: return self._request_with_retry('DELETE', f'{FOREVER_ORDER_URL}/{order_id}', label=f'Cancel {order_id}') is not None
    def cancel_alert_order(self, alert_id: str) -> bool: return self._request_with_retry('DELETE', f'{ALERT_ORDER_URL}/{alert_id}', label=f'Cancel {alert_id}') is not None

    def close_position_by_secid(self, sec_id: str, exchange_seg: str, net_qty: int):
        if net_qty == 0: return
        signal = 'SELL' if net_qty > 0 else 'BUY'
        payload = self._base_payload(signal, exchange_seg, sec_id) | {'quantity': abs(net_qty)}
        self._post_order(ORDER_URL, payload, label='CLOSE_POS')

    def place_order(self, sec_id, lot_size, inst, signal):
        place_order_mode = self._defaults_config.get('place_order_mode')
        
        if inst.seg in OPT_SEGMENTS and inst.exch == 'MCX':
            self.place_simple_order(sec_id, lot_size, inst)
            return

        if inst.seg in FNO_SEGMENTS:
            placed_order = False
            match place_order_mode:
                case 'ALERT':
                    if inst.exch != 'MCX':
                        self.place_trigger_alert_order(sec_id, lot_size, inst, fno_signal=signal)
                        placed_order = True
                case 'FOREVER':
                    if inst.seg not in OPT_SEGMENTS:
                        self.place_trigger_forever_order(sec_id, lot_size, inst)
                        placed_order = True
                case 'SUPER':
                    if inst.seg not in OPT_SEGMENTS:
                        self.place_super_order(sec_id, lot_size, inst)
                        placed_order = True
                case _:
                    self.place_simple_order(sec_id, lot_size, inst)
                    placed_order = True
            
            if not placed_order:
                self.place_simple_order(sec_id, lot_size, inst)
            return

        match place_order_mode:
            case 'ALERT':   self.place_trigger_alert_order(sec_id, lot_size, inst)
            case 'SUPER':   self.place_super_order(sec_id, lot_size, inst)
            case 'FOREVER': self.place_trigger_forever_order(sec_id, lot_size, inst)
            case _:         self.place_simple_order(sec_id, lot_size, inst)

    def fire_trade(self, symb: str, exch: str, signal: str, quant: int = 1, entry_val: float = 0):
        trade_key = f'{exch}:{symb}:{signal}'
        if trade_key in self.traded_this_scan:
            print(f'[skip] {trade_key} already traded this scan cycle.')
            return
        self.traded_this_scan.add(trade_key)

        inst = self.resolve_instrument(symb, exch, signal, quant, entry_val)
        if inst is None: return

        sec_id, lot_size = self.scrip.lookup_with_fallback(inst)
        if sec_id is None: return

        self.place_order(sec_id, lot_size, inst, signal)

    def clean_orphaned_orders(self):
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

    # trader.fire_trade('TCS',   'NSE', 'BUY',  entry_val=2400,  quant=1)
    # trader.fire_trade('TCS',   'NSE', 'SELL', entry_val=2200,  quant=1)
    # trader.fire_trade('NIFTY', 'NSE', 'BUY',  entry_val=24000, quant=1)
    # trader.fire_trade('NIFTY', 'NSE', 'SELL', entry_val=23000, quant=1)

    res= trader.get_forever_orders()
    print(res)
    # trader.clean_orphaned_orders()