"""
dhan_trade.py — Dhan HQ automated order placement (Object-Oriented).
"""

import math
import time
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

ORDER_URL         = 'https://api.dhan.co/v2/orders'
POSITIONS_URL     = 'https://api.dhan.co/v2/positions'
SUPER_ORDER_URL   = 'https://api.dhan.co/v2/super/orders'
FOREVER_ORDER_URL = 'https://api.dhan.co/v2/forever/orders'
ALERT_ORDER_URL   = 'https://api.dhan.co/v2/alerts/orders'

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
    'EQUITY': 'EQ',
    'OPTFUT': 'COMM',
    'FUTCOM': 'COMM',
    'OPTIDX': 'FNO',
    'OPTSTK': 'FNO',
    'FUTIDX': 'FNO',
    'FUTSTK': 'FNO',
})

OPT_SEGMENTS = frozenset({'OPTSTK', 'OPTIDX', 'OPTFUT'})
FUT_SEGMENTS = frozenset({'FUTIDX', 'FUTCOM', 'FUTSTK'})
FNO_SEGMENTS = OPT_SEGMENTS | FUT_SEGMENTS

# FIX #11 — was snake_case; module-level constants use UPPER_CASE
UNDERLYING_SEG_MAP = MappingProxyType({
    'OPTSTK': 'EQUITY', 'FUTSTK': 'EQUITY',
    'OPTFUT': 'FUTCOM',
    'OPTIDX': 'INDEX',  'FUTIDX': 'INDEX',
})

EQ_INDEX_INSTR = frozenset({'EQUITY', 'INDEX'})  # reused in eq_mask logic

# ───────────────────────────────────────
# Pure utility helpers
# ───────────────────────────────────────
def _signal_to_opt(signal: str) -> str:
    """'BUY' → 'CE', 'SELL' → 'PE'."""
    return 'CE' if signal == 'BUY' else 'PE'

def _invert_signal(signal: str) -> str:
    return 'SELL' if signal == 'BUY' else 'BUY'

def _adjust_price(base: float, perc: float, signal: str, opt_bump: bool = False) -> float:
    perc = 10 * perc if opt_bump else perc
    return math.ceil(base * (1 + perc / 100)) if signal == 'BUY' else math.floor(base * (1 - perc / 100))

def _get_today_str() -> str:
    """Returns today's date in IST as 'YYYY-MM-DD' for fast string comparison."""
    return datetime.now().strftime('%Y-%m-%d')

def _year_end_str() -> str:
    """Returns Dec 31 of the current year as 'YYYY-MM-DD'."""
    return datetime.now().replace(month=12, day=31).strftime('%Y-%m-%d')


# ───────────────────────────────────────
# Fallback strike helpers
# ───────────────────────────────────────
_FALLBACK_STEPS = (1, 2, 5, 10, 20, 25, 50, 100, 200, 500, 1_000, 5_000)

def _next_round_step(current_step: int) -> Optional[int]:
    for s in _FALLBACK_STEPS:
        if s > current_step:
            return s
    return None

def _get_fallback_strike(base: str, strike: float, opt_type: str) -> Optional[float]:
    fb_step = _next_round_step(get_strike_interval(base, strike))
    if fb_step is None:
        return None
    new_strike = math.floor(strike / fb_step) * fb_step if opt_type == 'CE' \
                 else math.ceil(strike / fb_step) * fb_step
    return float(new_strike) if new_strike != strike else None


# ───────────────────────────────────────
# Price levels & Data Classes
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
    symb:         str
    exch:         str
    seg:          str
    expiry_date:  str            = ''
    signal:       str            = ''
    quant:        int            = 1
    entry_val:    float          = 0.0
    trade_amount: float          = 0.0
    strike:       Optional[float] = None
    opt_type:     Optional[str]   = None


# ───────────────────────────────────────
# Config Manager
# ───────────────────────────────────────
class SymbolsConfig:
    def __init__(self, path: Path):
        self._path:   Path            = path
        self._mtime:  Optional[float] = None
        self._config: dict            = {}

    def get(self, key: str, default=None):
        self.refresh()
        return self._config.get(key, default)

    def refresh(self):
        try:
            mtime = self._path.stat().st_mtime
        except OSError:
            return
        if self._mtime == mtime:
            return
        if self._mtime is not None:
            print('Symbols config file changed — reloading.')
        self._mtime = mtime
        try:
            with open(self._path, 'rb') as f:
                self._config = tomllib.load(f) or {}
            print('Symbol config loaded.')
        except Exception as exc:
            print(f'Failed to parse TOML config: {exc}')


# ───────────────────────────────────────
# ScripMaster
# ───────────────────────────────────────
class ScripMaster:
    def __init__(self, refresh_master_scrip: bool = False):
        self._eq_index:             Optional[dict] = None
        self._opt_index:            Optional[dict] = None
        self._expiry_index:         Optional[dict] = None
        self._underlying_secid_map: Optional[dict] = None
        self._ensure_loaded(refresh_master_scrip)

    # ── Loading ───────────────────────────────────────────────────────────────
    def _ensure_loaded(self, refresh_master_scrip: bool = False):
        if self._eq_index is not None:
            return
        if not LOCAL_CSV.exists() or refresh_master_scrip:
            print(f'Rebuild {LOCAL_CSV}. Downloading segments...')
            raw_df = self._download_segments()
            if raw_df is None:
                print('Failed to download all scrip master segments.')
                return
            self._save_and_index(raw_df)
        else:
            self._index_from_csv()

    def _download_segments(self) -> Optional[pd.DataFrame]:
        import io
        frames = []
        for segment in INSTRUMENT_SEGMENTS:
            url = INSTRUMENT_URL.format(segment=segment)
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                df = pd.read_csv(
                    io.StringIO(resp.text),
                    usecols=lambda c: c in SCRIP_COLS,
                    low_memory=False,
                )
                if 'EXCH_ID' in df.columns and 'INSTRUMENT' in df.columns:
                    df = df[df['EXCH_ID'].isin(FILTER_EXCH) & df['INSTRUMENT'].isin(FILTER_SEG)]
                print(f'Downloaded and filtered {segment}: {len(df)} rows')
                frames.append(df)
            except Exception as exc:
                print(f'Failed to download {segment} from {url}: {exc}')

        if not frames:
            return None
        combined = pd.concat(frames, ignore_index=True)
        print(f'Combined segments: {len(combined)} total rows')
        return combined

    def _save_and_index(self, df: pd.DataFrame):
        missing = [c for c in SCRIP_COLS if c not in df.columns]
        if missing:
            print(f'Downloaded data is missing columns: {missing}')
            return

        today_str    = _get_today_str()
        # FIX #3 — use .isin() instead of two equality comparisons OR'd together
        is_eq        = df['INSTRUMENT'].isin(EQ_INDEX_INSTR)
        expiry_dates = df['SM_EXPIRY_DATE'].astype(str).str.split().str[0]
        df           = df[is_eq | (~is_eq & (expiry_dates >= today_str))].copy()

        df.to_csv(LOCAL_CSV, index=False)
        print(f'Saved filtered scrip master → {LOCAL_CSV}')

        eq_index, opt_index, expiry_index, under_map = {}, {}, {}, {}
        # FIX #1 — pass today_str so _fold_chunk doesn't recompute per-chunk
        self._fold_chunk(df, eq_index, opt_index, expiry_index, under_map, today_str)
        self._commit_indexes(eq_index, opt_index, expiry_index, under_map)
        print(f'Scrip master indexed (equity={len(eq_index)}, options={len(opt_index)} keys).')

    def _index_from_csv(self):
        eq_index, opt_index, expiry_index, under_map = {}, {}, {}, {}
        # FIX #1 — compute once before the chunk loop
        today_str = _get_today_str()
        try:
            with pd.read_csv(LOCAL_CSV, usecols=SCRIP_COLS, chunksize=50_000, low_memory=False) as reader:
                for chunk in reader:
                    self._fold_chunk(chunk, eq_index, opt_index, expiry_index, under_map, today_str)
        except FileNotFoundError:
            print(f'Error: {LOCAL_CSV} missing, unable to load scrip master.')
            return
        except Exception as exc:
            print(f'Error loading scrip master: {exc}')
            return
        self._commit_indexes(eq_index, opt_index, expiry_index, under_map)
        print(f'Scrip master loaded (equity={len(eq_index)}, options={len(opt_index)} keys).')

    # FIX #2 — single method replaces the duplicated 4-line assignment block
    def _commit_indexes(self, eq_index: dict, opt_index: dict, expiry_index: dict, under_map: dict):
        """Atomically commit all four index dicts to instance state."""
        self._eq_index             = eq_index
        self._opt_index            = opt_index
        self._expiry_index         = expiry_index
        self._underlying_secid_map = under_map

    @staticmethod
    def _fold_chunk(
        chunk: pd.DataFrame,
        eq_index: dict, opt_index: dict, expiry_index: dict, under_secid_map: dict,
        today_str: str,                      # FIX #1 — received, not recomputed
    ) -> None:
        if chunk.empty:
            return

        expiry_strs = chunk['SM_EXPIRY_DATE'].astype(str).str.split().str[0]
        # FIX #3 — .isin() instead of two == comparisons
        eq_mask     = chunk['INSTRUMENT'].isin(EQ_INDEX_INSTR)

        for row in chunk[eq_mask].itertuples(index=False):
            eq_index[(row.EXCH_ID, row.UNDERLYING_SYMBOL)] = (str(row.SECURITY_ID), int(row.LOT_SIZE))

        for row, exp in zip(chunk[~eq_mask].itertuples(index=False), expiry_strs[~eq_mask]):
            if exp < today_str:
                continue

            base_key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL)
            expiry_index.setdefault(base_key, set()).add(exp)

            strike, opt_type = None, None
            if row.INSTRUMENT in OPT_SEGMENTS:
                strike   = float(row.STRIKE_PRICE)
                opt_type = str(row.OPTION_TYPE).strip().upper()

            key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL, exp, strike, opt_type)
            if key not in opt_index:
                str_sec_id = str(row.SECURITY_ID)
                opt_index[key]             = (str_sec_id, int(row.LOT_SIZE))
                under_secid_map[str_sec_id] = str(int(row.UNDERLYING_SECURITY_ID))

    # ── Lookup ────────────────────────────────────────────────────────────────
    def lookup(self, inst: Instrument, *, silent: bool = False) -> tuple[Optional[str], int]:
        self._ensure_loaded()

        if inst.seg in EQ_INDEX_INSTR:
            result = self._eq_index.get((inst.exch, inst.symb)) if self._eq_index else None
            if result is None and not silent:
                print(f'Error: {inst.symb} ({inst.exch}/{inst.seg}) not found in scrip master.')
            return result or (None, 0)

        valid_expiries = self._expiry_index.get((inst.exch, inst.seg, inst.symb))
        if not valid_expiries:
            if not silent:
                print(f'Error: {inst.symb} ({inst.exch}/{inst.seg}) no expiries found in master.')
            return None, 0

        date_key = inst.expiry_date
        if not date_key or date_key not in valid_expiries:
            date_key = sorted(valid_expiries)[0]
            inst.expiry_date = date_key

        result = self._opt_index.get((inst.exch, inst.seg, inst.symb, date_key, inst.strike, inst.opt_type))
        if result:
            return result
        if not silent:
            print(f'Error: {inst.symb} {inst.opt_type} {inst.strike} EXP: {date_key} not found.')
        return None, 0

    def lookup_with_fallback(self, inst: Instrument) -> tuple[Optional[str], int]:
        sec_id, lot_size = self.lookup(inst, silent=True)
        if sec_id is not None:
            return sec_id, lot_size

        if inst.seg not in OPT_SEGMENTS:
            print(f'Error: {inst.symb} ({inst.exch}/{inst.seg}) not found in scrip master. Skipping.')
            return None, 0

        fb_strike = _get_fallback_strike(inst.symb, inst.strike, inst.opt_type)
        if fb_strike:
            original_strike = inst.strike
            inst.strike     = fb_strike
            sec_id, lot_size = self.lookup(inst, silent=True)
            if sec_id is not None:
                print(f'[fallback] {inst.symb} strike {original_strike} → {fb_strike}')
                return sec_id, lot_size
            inst.strike = original_strike

        print(f'Error: {inst.symb} {inst.strike} {inst.opt_type} not found in scrip master. Skipping.')
        return None, 0


# ───────────────────────────────────────
# Core Dhan API class
# ───────────────────────────────────────
class DhanTrader:
    """Object-oriented wrapper managing state, session, config, orders, and cleanups."""

    class PriceCondition(Enum):
        GREATER_THAN = 'GREATER_THAN'
        LESS_THAN    = 'LESS_THAN'

    def __init__(self, refresh_master_scrip: bool = False):
        self.cfg   = SymbolsConfig(SYMBOLS_CONFIG)
        self._defaults_config = None
        self._set_defaults_config()
        self.scrip = ScripMaster(refresh_master_scrip)
        self.traded_this_scan: set = set()

        self.proxy_manager = SSHProxyManager()
        self.session       = requests.Session()
        self._apply_proxy()

        self.client_id, self.access_token = self._load_credentials(ACCESS_FILE_PATH)
        self.api_headers = {
            'access-token': self.access_token,
            'Content-Type': 'application/json',
            'Accept':       'application/json',
        }

        self.entry_perc      = self.cfg.get('entry_price_perc', 0.1)
        self.limit_perc      = self.cfg.get('limit_price_perc', 0.2)
        self.target_perc     = self.cfg.get('target_perc', 4.0)
        self.stop_loss_perc  = self.cfg.get('stop_loss_perc', 0.7)
        self.stop_trail_perc = self.cfg.get('stop_trail_perc', 0.5)

    # ── Session Setup ─────────────────────────────────────────────────────────
    def _apply_proxy(self):
        try:
            proxy_cfg  = self.proxy_manager.config.get('proxy', {})
            proxy_host = proxy_cfg.get('proxy_host', '')
            port       = proxy_cfg.get('port', 0)
            if proxy_host:
                proxy_url = f'socks5h://{proxy_host}:{port}'
                self.session.proxies = {'http': proxy_url, 'https': proxy_url}
                print(f'[*] Proxy applied: {proxy_url}')
            else:
                print('[!] Proxy Config Not Found.')
        except Exception as exc:
            print(f'[!] Failed to load proxy config: {exc}')

    def _load_credentials(self, path: Path) -> tuple[str, str]:
        try:
            with open(path, 'rb') as f:
                data = tomllib.load(f)
            client_id    = data.get('CLIENT_ID', '').strip()
            access_token = data.get('ACCESS_TOKEN', '').strip()
            if not client_id or not access_token:
                raise ValueError(f'{path} must contain CLIENT_ID and ACCESS_TOKEN.')
            return client_id, access_token
        except Exception as exc:
            print(f'Error reading credentials from {path}: {exc}')
            return '', ''

    def begin_session(self):
        self.cfg.refresh()
        self.traded_this_scan.clear()

    def _defaults(self) -> dict:
        return self._defaults_config
    
    def _set_defaults_config(self):
        self._defaults_config = MappingProxyType({
            'expiry':       self.cfg.get('def_expiry_date', ''),
            'quant':        self.cfg.get('def_quantity', 1),
            'trade_amount': self.cfg.get('def_trade_amount', 10000),
            'order_mode':   self.cfg.get('def_order_mode', ''),
            'place_order_mode': self.cfg.get('place_order_mode', 'MARKET')
        })
   

    # ── Instrument Resolution ─────────────────────────────────────────────────
    def _compute_price_levels(self, raw_entry: float, signal: str, opt_bump: bool = False) -> PriceLevels:
        # FIX #9 — compute inv once instead of repeating the ternary twice
        inv        = _invert_signal(signal)
        entry      = _adjust_price(raw_entry, self.entry_perc,     signal, opt_bump)
        limit      = _adjust_price(entry,     self.limit_perc,     signal, opt_bump)
        stop_loss  = _adjust_price(entry,     self.stop_loss_perc, inv,    opt_bump)
        stop_limit = _adjust_price(stop_loss, self.limit_perc,     inv,    opt_bump)
        target     = _adjust_price(entry,     self.target_perc,    signal, opt_bump)
        trail_mult = self.stop_trail_perc * (10 if opt_bump else 1)
        trail      = math.ceil(entry * trail_mult / 100)
        return PriceLevels(entry, limit, stop_loss, stop_limit, target, trail)

    def _build_opt_instrument(
        self, sym_data: dict, base_symb: str, expiry: str,
        signal: str, entry_val: float, exch: str, seg: str, def_quant: int,
    ) -> Instrument:
        expiry = sym_data.get('expiry_date', expiry)
        expiry = str(expiry).split()[0] if expiry else None

        strike, opt_type = None, None
        if seg in OPT_SEGMENTS:
            opt_type    = _signal_to_opt(signal)
            # FIX #12 — flatten the nested get() chain
            sig_key     = 'call_strike' if signal == 'BUY' else 'put_strike'
            auto_strike = get_price_strike(base_symb, entry_val, signal)
            strike      = float(sym_data.get(sig_key) or sym_data.get('strike') or auto_strike)

        return Instrument(
            symb=sym_data.get('symbol', base_symb),
            exch=exch, seg=seg, expiry_date=expiry,
            signal='BUY', quant=sym_data.get('quantity', def_quant),
            strike=strike, opt_type=opt_type, entry_val=entry_val,
        )

    def _resolve_opt_section(
        self, section: dict, symb: str, signal: str, entry_val: float,
        exch: str, seg: str, dfl: dict, *, sym_data: Optional[dict] = None,
    ) -> Optional[Instrument]:
        sect_cfg = section.get('config', {})
        if sect_cfg.get('order_mode', dfl['order_mode']) not in ('OPT', 'FUT'):
            return None
        if sym_data is None:
            sym_data = section.get('symbols', {}).get(symb) or {}
        expiry = sect_cfg.get('expiry_date', dfl['expiry'])
        return self._build_opt_instrument(
            sym_data, symb, expiry, signal, entry_val, exch=exch, seg=seg, def_quant=dfl['quant']
        )

    def _resolve_nse(self, symb: str, signal: str, entry_val: float, quant: int) -> Optional[Instrument]:
        dfl     = self._defaults()
        nse_cfg = self.cfg.get('nse', {})

        nse_indices = nse_cfg.get('indices', {})
        if symb in nse_indices.get('symbols', {}):
            ord_mode = nse_indices['symbols'][symb].get(
                'order_mode', nse_indices.get('config', {}).get('order_mode', dfl['order_mode'])
            )
            if ord_mode in ('OPT', 'FUT'):
                seg = 'OPTIDX' if ord_mode == 'OPT' else 'FUTIDX'
                return self._resolve_opt_section(nse_indices, symb, signal, entry_val, exch='NSE', seg=seg, dfl=dfl)
        else:
            nse_stocks = nse_cfg.get('stocks', {})
            stk_cfg    = nse_stocks.get('config', {})
            ord_mode   = stk_cfg.get('order_mode', dfl['order_mode'])
            if ord_mode == 'EQ':
                return Instrument(
                    symb=symb, exch='NSE', seg='EQUITY', signal=signal, quant=quant,
                    entry_val=entry_val, trade_amount=stk_cfg.get('trade_amount', dfl['trade_amount']),
                )
            if ord_mode in ('OPT', 'FUT'):
                seg = 'OPTSTK' if ord_mode == 'OPT' else 'FUTSTK'
                return self._resolve_opt_section(nse_stocks, symb, signal, entry_val, exch='NSE', seg=seg, dfl=dfl, sym_data={})
        return None

    def _resolve_mcx(self, symb: str, signal: str, entry_val: float) -> Optional[Instrument]:
        dfl      = self._defaults()
        mcx_comm = self.cfg.get('mcx', {}).get('comm', {})
        if symb not in mcx_comm.get('symbols', {}):
            return None
        ord_mode = mcx_comm['symbols'][symb].get(
            'order_mode', mcx_comm.get('config', {}).get('order_mode', dfl['order_mode'])
        )
        if ord_mode in ('OPT', 'FUT'):
            seg = 'OPTFUT' if ord_mode == 'OPT' else 'FUTCOM'
            return self._resolve_opt_section(mcx_comm, symb, signal, entry_val, exch='MCX', seg=seg, dfl=dfl)
        return None

    def resolve_instrument(self, symb: str, exch: str, signal: str, quant: int, entry_val: float) -> Optional[Instrument]:
        match exch:
            case 'NSE': inst = self._resolve_nse(symb, signal, entry_val, quant)
            case 'MCX': inst = self._resolve_mcx(symb, signal, entry_val)
            case _:     inst = None
        if inst is None:
            print(f'No valid segment configured for {symb} on {exch}. Skipping.')
        return inst

    def get_instr_data(self, inst: Instrument) -> tuple[str, str]:
        if inst.seg in OPT_SEGMENTS:
            display_symb = f'{inst.symb} {inst.strike} {inst.opt_type} {inst.expiry_date}'
        elif inst.seg in FUT_SEGMENTS:
            display_symb = f'{inst.symb} Future {inst.expiry_date}'
        else:
            display_symb = f'{inst.symb} {inst.seg}'
        exchange_seg = 'IDX_I' if inst.seg == 'INDEX' else f'{inst.exch}_{SEG_EXCHANGE_SUFFIX[inst.seg]}'
        return display_symb, exchange_seg

    # ── HTTP Primitives ───────────────────────────────────────────────────────
    # FIX #5 — single retry wrapper replaces three identical try/except blocks
    def _request_with_retry(
        self, method: str, url: str, label: str = '', retry: bool = True, **kwargs,
    ) -> Optional[requests.Response]:
        """Central retry-on-network-failure wrapper for all HTTP calls."""
        try:
            return self.session.request(method, url, headers=self.api_headers, timeout=10, **kwargs)
        except RequestException as exc:
            print(f'[✗] {label} Network error: {exc}')
            if retry:
                print(f'[!] Restarting SSH proxy and retrying {label}...')
                self.proxy_manager.restart()
                time.sleep(2)
                return self._request_with_retry(method, url, label=f'{label} (Retry)', retry=False, **kwargs)
            print(f'[✗] {label} permanently failed due to network error.')
            return None

    def _post_order(self, url: str, payload: dict, label: str = ''):
        resp = self._request_with_retry('POST', url, label=label, json=payload)
        if resp is None:
            return
        if resp.status_code != 200:
            print(f'[✗] {label} Order failed ({resp.status_code}): {resp.text}')
        else:
            print(f'[✓] {label} Order placed successfully.')

    # ── Payload Builders ──────────────────────────────────────────────────────
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

    # FIX #4 — extracted from place_alert_order and place_trigger_alert_order
    def _build_alert_payload(
        self,
        alert_exchange_seg: str, alert_sec_id: str,
        operator: str, compare_price: float,
        exp_date: str, note: str,
        orders: list[dict],
    ) -> dict:
        """Builds the common alert/conditional order payload envelope."""
        return {
            'dhanClientId': self.client_id,
            'condition': {
                'comparisonType': 'PRICE_WITH_VALUE',
                'exchangeSegment': alert_exchange_seg,
                'securityId':      alert_sec_id,
                'operator':        operator,
                'comparingValue':  compare_price,
                'expDate':         exp_date,
                'frequency':       'ONCE',
                'userNote':        note,
            },
            'orders': orders,
        }

    # ── Order Placement ───────────────────────────────────────────────────────
    # FIX #7 — signature unified to (sec_id, lot_size, inst) like place_market_order
    def place_super_order(self, sec_id: str, lot_size: int, inst: Instrument):
        display_symb, exchange_seg = self.get_instr_data(inst)
        base        = self._base_payload(inst.signal, exchange_seg, sec_id)
        levels      = self._compute_price_levels(inst.entry_val, inst.signal)
        total_quant = compute_quantity(inst.trade_amount, levels.entry, lot_size, inst.quant)
        payload = base | {
            'orderType':     'LIMIT',
            'quantity':      total_quant,
            'price':         levels.limit,
            'stopLossPrice': levels.stop_loss,
            'trailingJump':  levels.trail,
        }
        print(f'##### SUPER | {display_symb} | {sec_id} | {inst.signal} | qty={total_quant} | entry={levels.entry}')
        self._post_order(SUPER_ORDER_URL, payload, label='SUPER')

    def place_market_order(self, sec_id: str, lot_size: int, inst: Instrument):
        display_symb, exchange_seg = self.get_instr_data(inst)
        payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {
            'quantity': inst.quant * lot_size,
        }
        print(f'##### MARKET | {display_symb} | {sec_id} | {inst.signal} | qty={inst.quant * lot_size}')
        self._post_order(ORDER_URL, payload, label='MARKET')

    # FIX #6 — now uses _base_payload instead of a hand-rolled dict
    def place_forever_order(
        self, sec_id: str, signal: str, exchange_seg: str, quant: int,
        trigger_price: float, trigger_price1: float = 0,
        is_oco: bool = False, product_type: str = 'CNC',
    ):
        """Places a GTT/Forever order (SINGLE or OCO)."""
        payload = self._base_payload(signal, exchange_seg, sec_id) | {
            'correlationId':     f'cond_{self.client_id}',
            'orderFlag':         'OCO' if is_oco else 'SINGLE',
            'productType':       product_type,
            'validity':          'FOREVER',
            'quantity':          quant,
            'disclosedQuantity': 0,
            'triggerPrice':      trigger_price,
            'price1':            0,
            'triggerPrice1':     trigger_price1,
            'quantity1':         quant if is_oco else 0,
        }
        mode    = 'OCO' if is_oco else 'SINGLE'
        log_msg = f'##### FOREVER {mode} | {sec_id} | {signal} | qty={quant} | trig1={trigger_price}'
        if is_oco:
            log_msg += f' | trig2={trigger_price1}'
        print(log_msg)
        self._post_order(FOREVER_ORDER_URL, payload, label='FOREVER')

    # FIX #8 — delegates to place_forever_order instead of duplicating payload logic
    def place_trigger_forever_order(self, sec_id: str, lot_size: int, inst: Instrument):
        """Places a price-level-computed GTT/Forever order."""
        display_symb, exchange_seg = self.get_instr_data(inst)
        levels      = self._compute_price_levels(inst.entry_val, inst.signal)
        total_quant = compute_quantity(inst.trade_amount, levels.entry, lot_size, inst.quant)
        print(f'##### FOREVER | {display_symb} | {sec_id} | {inst.signal} | qty={total_quant} | trigger={levels.entry}')
        self.place_forever_order(
            sec_id=sec_id, signal=inst.signal, exchange_seg=exchange_seg,
            quant=total_quant, trigger_price=levels.entry, product_type='CNC',
        )

    # FIX #4 — uses _build_alert_payload instead of an inline dict
    def place_trigger_alert_order(self, sec_id: str, lot_size: int, inst: Instrument, fno_signal: str = None):
        """Places an Alert-triggered order; for FNO, condition tracks the underlying instrument."""
        display_symb, exchange_seg = self.get_instr_data(inst)
        alert_signal = fno_signal or inst.signal
        levels       = self._compute_price_levels(inst.entry_val, alert_signal)
        total_quant  = compute_quantity(inst.trade_amount, levels.entry, lot_size, inst.quant)

        ord_payload = self._base_payload(inst.signal, exchange_seg, sec_id) | {'quantity': total_quant}
        condition   = (DhanTrader.PriceCondition.GREATER_THAN if alert_signal == 'BUY'
                       else DhanTrader.PriceCondition.LESS_THAN).value

        alert_sec_id    = sec_id
        alert_exch_seg  = exchange_seg
        alert_disp_symb = display_symb

        if fno_signal:
            parent_seg   = UNDERLYING_SEG_MAP.get(inst.seg, inst.seg)
            parent_instr = Instrument(symb=inst.symb, exch=inst.exch, seg=parent_seg)
            alert_sec_id, _ = self.scrip.lookup(parent_instr)
            if not alert_sec_id:
                print(f'Underlying {inst.symb} {inst.exch} {parent_seg} does Not Exist. Skipping')
                return
            alert_disp_symb, alert_exch_seg = self.get_instr_data(parent_instr)

        payload = self._build_alert_payload(
            alert_exchange_seg=alert_exch_seg,
            alert_sec_id=alert_sec_id,
            operator=condition,
            compare_price=levels.entry,
            exp_date=_year_end_str(),
            note='Main Order',
            orders=[ord_payload],
        )
        print(
            f'##### ALERT | Alert {alert_disp_symb} | {alert_sec_id} | {condition} {levels.entry}\n'
            f'Order {display_symb} | {sec_id} | {inst.signal} | qty={total_quant}'
        )
        self._post_order(ALERT_ORDER_URL, payload, label='ALERT')

    # FIX #4 — uses _build_alert_payload instead of an inline dict
    def place_alert_order(self, display_symb: str, exchange: str, signal: str, quant: int,
                          alert_price: float, condition: str):
        """Places a Conditional Alert order triggered by market data."""
        inst = self.resolve_instrument(display_symb, exchange, signal, quant, alert_price)
        if inst is None:
            return

        sec_id, lot_size = self.scrip.lookup_with_fallback(inst)
        exchange_seg     = f'{inst.exch}_{SEG_EXCHANGE_SUFFIX[inst.seg]}'
        ord_payload      = self._base_payload(signal, exchange_seg, sec_id) | {
            'quantity': quant * lot_size,
        }
        payload = self._build_alert_payload(
            alert_exchange_seg=exchange_seg,
            alert_sec_id=sec_id,
            operator=condition,
            compare_price=alert_price,
            exp_date=_get_today_str(),
            note='Triggr order Value',
            orders=[ord_payload],
        )
        print(f'##### ALERT | {display_symb} | {exchange_seg} | {sec_id} | {signal} | qty={quant} | cond={condition} @ {alert_price}')
        self._post_order(ALERT_ORDER_URL, payload, label='ALERT')



    def place_order(self, sec_id, lot_size, inst, signal):
        place_order_mode = self._defaults_config.get('place_order_mode')
        
        # MCX FNO always uses market order, so handle this case up front.
        if inst.seg in FNO_SEGMENTS and inst.exch == 'MCX':
            self.place_market_order(sec_id, lot_size, inst)
            return

        # NSE FNO (non-MCX) - switch logic based on place_order_mode
        if inst.seg in FNO_SEGMENTS:
            match place_order_mode:
                case 'ALERT':
                    self.place_trigger_alert_order(sec_id, lot_size, inst, fno_signal=signal)
                case _:
                    self.place_market_order(sec_id, lot_size, inst)
            return

        # NON-FNO (EQ or INDEX)
        match place_order_mode:
            case 'ALERT':
                self.place_trigger_alert_order(sec_id, lot_size, inst)
            case 'SUPER':
                self.place_super_order(sec_id, lot_size, inst)
            case 'FOREVER':
                self.place_trigger_forever_order(sec_id, lot_size, inst)
            case _:
                self.place_market_order(sec_id, lot_size, inst)




    # ── Fire Trade Router ─────────────────────────────────────────────────────
    def fire_trade(self, symb: str, exch: str, signal: str, quant: int = 1, entry_val: float = 0):
        trade_key = f'{exch}:{symb}:{signal}'
        if trade_key in self.traded_this_scan:
            print(f'[skip] {trade_key} already traded this scan cycle.')
            return
        self.traded_this_scan.add(trade_key)

        inst = self.resolve_instrument(symb, exch, signal, quant, entry_val)
        if inst is None:
            return

        sec_id, lot_size = self.scrip.lookup_with_fallback(inst)
        if sec_id is None:
            return

        self.place_order(sec_id, lot_size, inst, signal)


  

    # ── Positions & Super-Order Cleanup ───────────────────────────────────────
    # FIX #5 — all three GET/DELETE methods now use _request_with_retry
    def get_active_positions(self) -> set[str]:
        resp = self._request_with_retry('GET', POSITIONS_URL, label='GET Positions')
        if resp is None or resp.status_code != 200:
            print(f'[✗] Failed to fetch positions: {getattr(resp, "status_code", "N/A")}')
            return set()
        active = {
            pos['tradingSymbol'] for pos in resp.json()
            if pos.get('netQty', 0) != 0 and pos.get('tradingSymbol')
        }
        print(f'\n[✓] Total Active Position Symbols: {len(active)}')
        return active

    def get_active_super_orders(self) -> set[tuple[str, str, str]]:
        resp = self._request_with_retry('GET', SUPER_ORDER_URL, label='GET Super Orders')
        if resp is None or resp.status_code != 200:
            print(f'[✗] Failed to fetch super orders: {getattr(resp, "status_code", "N/A")}')
            return set()

        active_orders   = set()
        active_statuses = {'PENDING', 'PART_TRADED', 'TRADED'}
        for order in resp.json():
            status = order.get('orderStatus', '')
            symbol = order.get('tradingSymbol', '')
            oid    = order.get('orderId', '')
            if status in {'PENDING', 'PART_TRADED'}:
                active_orders.add((symbol, oid, 'ENTRY_LEG'))
            if status in active_statuses:
                for leg in order.get('legDetails', []):
                    if leg.get('orderStatus') == 'PENDING':
                        active_orders.add((symbol, oid, leg.get('legName', '')))

        print(f'[✓] Total Active Super Orders: {len(active_orders)}')
        return active_orders

    def cancel_super_order(self, order_id: str, order_leg: str = 'ENTRY_LEG') -> bool:
        url  = f'{SUPER_ORDER_URL}/{order_id}/{order_leg}'
        resp = self._request_with_retry('DELETE', url, label=f'Cancel {order_id}')
        if resp is None:
            return False
        if resp.status_code in (200, 202):
            status = (resp.json() if resp.text else {}).get('orderStatus', 'CANCELLED')
            print(f'  [✓] Cancelled Super Order: {order_id} | Status: {status}')
            return True
        print(f'  [✗] Failed to cancel {order_id}: {resp.status_code}')
        return False

    # FIX #10 — uses _request_with_retry instead of its own try/except
    def get_alert_orders(self):
        resp = self._request_with_retry('GET', ALERT_ORDER_URL, label='GET Alert Orders')
        if resp is None or resp.status_code != 200:
            print(f'[✗] Failed to get Alert Orders: {getattr(resp, "status_code", "N/A")}')
            return
        print(resp.json())

    # FIX #13 — list comprehension replaces loop + append
    def clean_orphaned_orders(self):
        print('\n─── Starting Cleanup Cycle ───')
        active_symbols      = self.get_active_positions()
        active_super_orders = self.get_active_super_orders()

        cancelled = [
            symb for symb, oid, leg in active_super_orders
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

    trader.fire_trade('TCS',   'NSE', 'BUY',  entry_val=2400,  quant=1)
    trader.fire_trade('TCS',   'NSE', 'SELL', entry_val=2200,  quant=1)
    trader.fire_trade('NIFTY', 'NSE', 'BUY',  entry_val=24000, quant=1)
    trader.fire_trade('NIFTY', 'NSE', 'SELL', entry_val=23000, quant=1)

    # trader.fire_trade('GOLD', 'MCX', 'BUY', entry_val=160000, quant=1)

    trader.get_alert_orders()

    # Run cleanup independently
    # trader.clean_orphaned_orders()