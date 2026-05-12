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


# ───────────────────────────────────────
# Paths & Pure Constants
# ───────────────────────────────────────
BASE_DIR         = Path(__file__).parent
SYMBOLS_CONFIG   = BASE_DIR / 'symbols_config.toml'
LOCAL_CSV        = Path(OUT_DIR) / 'scrip_master.csv'
ACCESS_FILE_PATH = BASE_DIR / 'access_token.toml'

ORDER_URL        = 'https://api.dhan.co/v2/orders'
POSITIONS_URL    = 'https://api.dhan.co/v2/positions'
SUPER_ORDER_URL  = 'https://api.dhan.co/v2/super/orders'
INSTRUMENT_SEGMENTS = ['NSE_EQ', 'NSE_FNO', 'MCX_COMM']
INSTRUMENT_URL   = 'https://api.dhan.co/v2/instrument/{segment}'

FILTER_SEG  = frozenset({'EQUITY', 'OPTSTK', 'OPTIDX', 'OPTFUT'})
FILTER_EXCH = frozenset({'NSE', 'MCX'})
SCRIP_COLS  = [
    'EXCH_ID', 'INSTRUMENT',
    'UNDERLYING_SYMBOL','SECURITY_ID',
    'LOT_SIZE', 'SM_EXPIRY_DATE', 'STRIKE_PRICE','OPTION_TYPE'
]

SEG_EXCHANGE_SUFFIX = {
    'EQUITY': 'EQ',
    'OPTFUT': 'COMM',
    'OPTIDX': 'FNO',
    'OPTSTK': 'FNO',
}

OPT_SEGMENTS = frozenset({'OPTSTK', 'OPTIDX', 'OPTFUT'})

# ───────────────────────────────────────
# Pure utility helpers
# ───────────────────────────────────────
def _signal_to_opt(signal: str) -> str:
    """'BUY' → 'CE', 'SELL' → 'PE'."""
    return 'CE' if signal == 'BUY' else 'PE'

def _adjust_price(base: float, perc: float, signal: str) -> float:
    if signal == 'BUY':
        return math.ceil(base * (1 + perc / 100))
    return math.floor(base * (1 - perc / 100))


def _get_today_str() -> str:
    """Returns today's date in IST as 'YYYY-MM-DD' for fast string comparison."""
    return datetime.now().strftime('%Y-%m-%d')


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
    orig_step = get_strike_interval(base, strike)
    fb_step   = _next_round_step(orig_step)
    
    if fb_step is None:
        return None

    if opt_type == 'CE':
        new_strike = math.floor(strike / fb_step) * fb_step
    else:  
        new_strike = math.ceil(strike / fb_step) * fb_step

    return float(new_strike) if new_strike != strike else None

# ───────────────────────────────────────
# Price levels & Data Classes
# ───────────────────────────────────────
@dataclass
class PriceLevels:
    entry:     float
    limit:     float
    stop_loss: float
    target:    float
    trail:     float

def compute_quantity(trade_amount: float, price: float,
                     lot_size: int, base_quant: int) -> int:
    if trade_amount > 0 and price > 0:
        lots = int(trade_amount // (price * lot_size)) + 1
        return lots * lot_size
    return base_quant * lot_size

@dataclass
class Instrument:
    symb:         str   
    exch:         str
    seg:          str
    expiry_date:  str   = ''
    signal:       str   = ''
    quant:        int   = 1
    entry_val:    float = 0.0
    trade_amount: float = 0.0
    strike:       float = 0.0
    opt_type:     str   = ''

# ───────────────────────────────────────
# Config Managers
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
    def __init__(self, refresh_master_scrip=False):
        self._eq_index:     Optional[dict] = None  
        self._opt_index:    Optional[dict] = None  
        self._expiry_index: Optional[dict] = None  
        self._ensure_loaded(refresh_master_scrip)

    def _ensure_loaded(self, refresh_master_scrip=False):
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
                    low_memory=False
                )
                
                if 'EXCH_ID' in df.columns and 'INSTRUMENT' in df.columns:
                    mask = df['EXCH_ID'].isin(FILTER_EXCH) & df['INSTRUMENT'].isin(FILTER_SEG)
                    df = df[mask]

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

        # ─── OPTIMIZATION: Filter out expired options before saving ───
        today_str = _get_today_str()
        is_eq = df['INSTRUMENT'] == 'EQUITY'
        
        # Extract just the 'YYYY-MM-DD' part for comparison
        expiry_dates = df['SM_EXPIRY_DATE'].astype(str).str.split().str[0]
        
        # Create a mask: Keep ALL Equity OR Keep Options that expire >= today
        valid_mask = is_eq | (~is_eq & (expiry_dates >= today_str))
        
        # Apply the mask to drop expired rows instantly using C-backend
        df = df[valid_mask].copy()
        # ──────────────────────────────────────────────────────────────

        df.to_csv(LOCAL_CSV, index=False)
        print(f'Saved filtered scrip master → {LOCAL_CSV}')
        
        eq_index, opt_index, expiry_index = {}, {}, {}
        self._fold_chunk(df, eq_index, opt_index, expiry_index)
        self._eq_index     = eq_index
        self._opt_index    = opt_index
        self._expiry_index = expiry_index
        print(f'Scrip master indexed (equity={len(eq_index)}, options={len(opt_index)} keys).')

    def _save_and_index1(self, df: pd.DataFrame):
        missing = [c for c in SCRIP_COLS if c not in df.columns]
        if missing:
            print(f'Downloaded data is missing columns: {missing}')
            return

        df.to_csv(LOCAL_CSV, index=False)
        print(f'Saved filtered scrip master → {LOCAL_CSV}')
        
        eq_index, opt_index, expiry_index = {}, {}, {}
        self._fold_chunk(df, eq_index, opt_index, expiry_index)
        self._eq_index     = eq_index
        self._opt_index    = opt_index
        self._expiry_index = expiry_index
        print(f'Scrip master indexed (equity={len(eq_index)}, options={len(opt_index)} keys).')

    def _index_from_csv(self):
        eq_index, opt_index, expiry_index = {}, {}, {}
        try:
            with pd.read_csv(LOCAL_CSV, usecols=SCRIP_COLS, chunksize=50_000, low_memory=False) as reader:
                for chunk in reader:
                    self._fold_chunk(chunk, eq_index, opt_index, expiry_index)
        except FileNotFoundError:
            print(f'Error: {LOCAL_CSV} missing, unable to load scrip master.')
            return
        except Exception as exc:
            print(f'Error loading scrip master: {exc}')
            return
            
        self._eq_index     = eq_index
        self._opt_index    = opt_index
        self._expiry_index = expiry_index
        print(f'Scrip master loaded (equity={len(eq_index)}, options={len(opt_index)} keys).')

    @staticmethod
    def _fold_chunk(chunk: pd.DataFrame, eq_index: dict, opt_index: dict, expiry_index: dict) -> None:
        if chunk.empty:
            return
        
        expiry_strs = chunk['SM_EXPIRY_DATE'].astype(str).str.split().str[0]
        eq_mask     = chunk['INSTRUMENT'] == 'EQUITY'
        today_str   = _get_today_str() # Calculate once per chunk

        for row in chunk[eq_mask].itertuples(index=False):
            eq_index[(row.EXCH_ID, row.UNDERLYING_SYMBOL)] = (str(row.SECURITY_ID), int(row.LOT_SIZE))

        for row, exp in zip(chunk[~eq_mask].itertuples(index=False), expiry_strs[~eq_mask]):
            # FAST O(1) STRING COMPARISON
            if exp < today_str:
                continue  # Skip expired options completely!

            base_key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL)
            expiry_index.setdefault(base_key, set()).add(exp)

            key = (
                row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL, 
                exp, float(row.STRIKE_PRICE), str(row.OPTION_TYPE).strip().upper()
            )
            
            if key not in opt_index:
                opt_index[key] = (str(row.SECURITY_ID), int(row.LOT_SIZE))



    def lookup(self, inst: Instrument, *, silent: bool = False) -> tuple[Optional[str], int]:

        self._ensure_loaded()

        if inst.seg == 'EQUITY':
            result = self._eq_index.get((inst.exch, inst.symb)) if self._eq_index else None
            if result is None:
                if not silent:
                    print(f'Error: {inst.symb} ({inst.exch}/{inst.seg}) not found in scrip master.')
                return None, 0

            return result

        date_key = inst.expiry_date
        
        # If no date provided, OR the provided date is already expired -> Grab the nearest valid one
        valid_expiries = self._expiry_index.get((inst.exch, inst.seg, inst.symb))
        
        if not valid_expiries:
            if not silent:
                print(f'Error: {inst.symb} ({inst.exch}/{inst.seg}) no expiries found in master.')
            return None, 0

        # Since the set only contains valid dates (filtered at load), [0] is the nearest future expiry
        if not date_key or date_key not in valid_expiries:
            date_key = sorted(valid_expiries)[0]
            inst.expiry_date = date_key

        key = (inst.exch, inst.seg, inst.symb, date_key, float(inst.strike), inst.opt_type)
        result = self._opt_index.get(key)
        
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
            inst.strike = fb_strike 
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

    def __init__(self, refresh_master_scrip=False):
        self.cfg     = SymbolsConfig(SYMBOLS_CONFIG)
        self.scrip   = ScripMaster(refresh_master_scrip)
        self.traded_this_scan: set = set()
        
        self.proxy_manager = SSHProxyManager()
        
        # Initialize Session
        self.session = requests.Session()
        
        # Apply Proxy if configured
        self._apply_proxy()

        # State: Credentials & Headers
        self.client_id, self.access_token = self._load_credentials(ACCESS_FILE_PATH)
        self.api_headers = {
            'access-token': self.access_token,
            'Content-Type': 'application/json',
            'Accept':       'application/json',
        }

        # State: Trading Adjustments (Loaded dynamically)
        self.entry_perc      = self.cfg.get('entry_price_perc', 0.1)
        self.limit_perc      = self.cfg.get('limit_price_perc', 0.2)
        self.target_perc     = self.cfg.get('target_perc', 4.0)
        self.stop_loss_perc  = self.cfg.get('stop_loss_perc', 0.7)
        self.stop_trail_perc = self.cfg.get('stop_trail_perc', 0.5)

    def _apply_proxy(self):
        """Loads and applies proxy settings from proxy_config.toml if enabled."""
        try:
            proxy_cfg = self.proxy_manager.config.get("proxy", {})
            proxy_host = proxy_cfg.get("proxy_host", '')
            port = proxy_cfg.get("port", 0)
            
            if proxy_host:
                proxy_url = f"socks5h://{proxy_host}:{port}"
                self.session.proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                print(f"[*] Proxy applied: {proxy_url}")
            else:
                print(f"[!] Proxy Config Not Found.")
        except Exception as exc:
            print(f"[!] Failed to load proxy config: {exc}")

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
        return {
            'expiry':       self.cfg.get('def_expiry_date', ''),
            'quant':        self.cfg.get('def_quantity', 1),
            'trade_amount': self.cfg.get('def_trade_amount', 10000),
            'order_mode':   self.cfg.get('def_order_mode', ''),
        }

    # ── Instrument Resolution ──────────────────────────────────────────────
    def _compute_price_levels(self, raw_entry: float, signal: str) -> PriceLevels:
        entry     = _adjust_price(raw_entry, self.entry_perc, signal)
        limit     = _adjust_price(entry, self.limit_perc, signal)
        stop_loss = _adjust_price(entry, self.stop_loss_perc, 'SELL' if signal == 'BUY' else 'BUY')
        target    = _adjust_price(entry, self.target_perc, signal)
        trail     = math.ceil(entry * self.stop_trail_perc / 100)
        return PriceLevels(entry, limit, stop_loss, target, trail)

    def _build_opt_instrument(
        self, sym_data: dict, base_symb: str, expiry: str,
        signal: str, entry_val: float, exch: str, seg: str, def_quant: int
    ) -> Instrument:
        expiry   = sym_data.get('expiry_date', expiry)
        opt_type = _signal_to_opt(signal)

        auto_strike = get_price_strike(base_symb, entry_val, signal)
        strike = sym_data.get('strike', auto_strike)
        sig_key = 'call_strike' if signal == 'BUY' else 'put_strike'
        strike = sym_data.get(sig_key, strike)

        expiry = str(expiry).split()[0] if expiry else None

        return Instrument(
            symb=sym_data.get('symbol', base_symb), 
            exch=exch, 
            seg=seg,
            expiry_date=expiry, 
            signal='BUY', 
            quant=sym_data.get('quantity', def_quant),
            strike=float(strike),
            opt_type=opt_type
        )

    def _resolve_opt_section(
        self, section: dict, symb: str, signal: str, entry_val: float,
        exch: str, seg: str, dfl: dict, *, sym_data: Optional[dict] = None
    ) -> Optional[Instrument]:
        sect_cfg = section.get('config', {})

        if sect_cfg.get('order_mode', dfl['order_mode']) != 'OPT':
            return None

        if sym_data is None:
            sym_data = section.get('symbols', {}).get(symb) or {}

        expiry = sect_cfg.get('expiry_date', dfl['expiry'])
        return self._build_opt_instrument(
            sym_data, symb, expiry, signal, entry_val,
            exch=exch, seg=seg, def_quant=dfl['quant'],
        )

    def _resolve_nse(self, symb: str, signal: str,
                     entry_val: float, quant: int) -> Optional[Instrument]:
        dfl = self._defaults()
        nse_cfg = self.cfg.get('nse', {})

        nse_indices = nse_cfg.get('indices', {})
        if symb in nse_indices.get('symbols', {}):
            return self._resolve_opt_section(
                nse_indices, symb, signal, entry_val,
                exch='NSE', seg='OPTIDX', dfl=dfl,
            )

        nse_stocks = nse_cfg.get('stocks', {})
        stk_cfg    = nse_stocks.get('config', {})
        ord_mode   = stk_cfg.get('order_mode', dfl['order_mode'])

        if ord_mode == 'EQ':
            return Instrument(
                symb=symb, exch='NSE', seg='EQUITY',
                signal=signal, quant=quant, entry_val=entry_val,
                trade_amount=stk_cfg.get('trade_amount', dfl['trade_amount']),
            )

        if ord_mode == 'OPT':
            return self._resolve_opt_section(
                nse_stocks, symb, signal, entry_val,
                exch='NSE', seg='OPTSTK', dfl=dfl, sym_data={},
            )
        return None

    def _resolve_mcx(self, symb: str, signal: str,
                     entry_val: float) -> Optional[Instrument]:
        dfl = self._defaults()
        mcx_cfg = self.cfg.get('mcx', {})
        mcx_comm = mcx_cfg.get('comm', {})
        
        if symb not in mcx_comm.get('symbols', {}):
            return None
        return self._resolve_opt_section(
            mcx_comm, symb, signal, entry_val,
            exch='MCX', seg='OPTFUT', dfl=dfl,
        )

    def resolve_instrument(self, symb: str, exch: str,
                           signal: str, quant: int,
                           entry_val: float) -> Optional[Instrument]:
        match exch:
            case 'NSE':
                inst = self._resolve_nse(symb, signal, entry_val, quant)
            case 'MCX':
                inst = self._resolve_mcx(symb, signal, entry_val)
            case _:
                inst = None

        if inst is None:
            print(f'No valid segment configured for {symb} on {exch}. Skipping.')
        return inst

    # ── Core Trading Operations ──────────────────────────────────────────────
    def _post_order(self, url: str, payload: dict, label: str = '', retry: bool = True):
        try:
            resp = self.session.post(url, json=payload, headers=self.api_headers, timeout=10)
            
            if resp.status_code != 200:
                print(f'[✗] {label} Order failed ({resp.status_code}): {resp.text}')
            else:
                print(f'[✓] {label} Order placed successfully.')

        except RequestException as exc:
            print(f'[✗] {label} Network error: {exc}')
            
            if retry:
                print(f'[!] Attempting to restart SSH proxy and retry {label} order...')
                self.proxy_manager.restart()
                time.sleep(2) 
                self._post_order(url, payload, label=f"{label} (Retry)", retry=False)
            else:
                print(f'[✗] {label} Order permanently failed due to network error.')

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
            'targetPrice': 0,
            'stopLossPrice': 0
        }

    def place_super_order(
        self, display_symb: str, exchange_seg: str, lot_size: int,
        sec_id: str, signal: str, quant: int, entry_val: float, trade_amount: float,
    ):
        base = self._base_payload(signal, exchange_seg, sec_id)
        levels = self._compute_price_levels(entry_val, signal)
        total_quant = compute_quantity(trade_amount, levels.entry, lot_size, quant)
        
        print(f'##### SUPER | {display_symb} | {sec_id} | {signal} | '
              f'qty={total_quant} | entry={levels.entry} | '
              f'amt={round(total_quant * levels.entry)}')

        payload = base | {
            'orderType':     'LIMIT',
            'quantity':      total_quant,
            'price':         levels.limit,
            'stopLossPrice': levels.stop_loss,
            'trailingJump':  levels.trail,
        }
        self._post_order(SUPER_ORDER_URL, payload, label='SUPER')

    def place_market_order(self, display_symb, signal, exchange_seg, sec_id, quant, lot_size):
        base = self._base_payload(signal, exchange_seg, sec_id)
        payload = base | {'quantity': quant * lot_size}
        print(f'##### MARKET | {display_symb} | {sec_id} | {signal} | qty={quant * lot_size}')
        self._post_order(ORDER_URL, payload, label='MARKET')

    def fire_trade(self, symb: str, exch: str, signal: str,
                   quant: int = 1, entry_val: float = 0):
        trade_key = f'{exch}:{symb}:{signal}'
        if trade_key in self.traded_this_scan:
            print(f'[skip] {trade_key} already traded this scan cycle.')
            return
        self.traded_this_scan.add(trade_key)

        inst = self.resolve_instrument(symb, exch, signal, quant, entry_val)
        if inst is None:
            return

        exchange_seg = f'{inst.exch}_{SEG_EXCHANGE_SUFFIX[inst.seg]}'
        sec_id, lot_size = self.scrip.lookup_with_fallback(inst)
        if sec_id is None:
            return

        if inst.seg in OPT_SEGMENTS:
            display_symb = f"{inst.symb} {inst.strike} {inst.opt_type} {inst.expiry_date}"
        else:
            display_symb = inst.symb

        if inst.entry_val == 0:
            self.place_market_order(display_symb, inst.signal, exchange_seg, sec_id, inst.quant, lot_size)
        else:
            self.place_super_order(
                display_symb, exchange_seg, lot_size, sec_id,
                inst.signal, inst.quant, inst.entry_val, inst.trade_amount,
            )

    # ── Super Order Cleanup Operations ───────────────────────────────────────
    def get_active_positions(self, retry: bool = True) -> set[str]:
        """Fetches active positions (netQty != 0) and returns their symbols."""
        try:
            response = self.session.get(POSITIONS_URL, headers=self.api_headers, timeout=10)
            if response.status_code != 200:
                print(f"[✗] Failed to fetch positions: {response.status_code}")
                return set()
                
            positions = response.json()
            active_symbols = {
                pos.get("tradingSymbol", "") 
                for pos in positions 
                if pos.get("netQty", 0) != 0 and pos.get("tradingSymbol")
            }
            print(f"\n[✓] Total Active Position Symbols: {len(active_symbols)}")
            return active_symbols
            
        except RequestException as e:
            print(f"[✗] Network error fetching positions: {e}")
            if retry:
                print('[!] Attempting to restart SSH proxy and retry fetching positions...')
                self.proxy_manager.restart()
                time.sleep(2)
                return self.get_active_positions(retry=False)
            return set()

    def get_active_super_orders(self, retry: bool = True) -> set[tuple[str, str, str]]:
        """Fetches super orders and returns a set of tuples for active legs: (symbol, order_id, leg_name)."""
        try:
            response = self.session.get(SUPER_ORDER_URL, headers=self.api_headers, timeout=10)
            if response.status_code != 200:
                print(f"[✗] Failed to fetch super orders: {response.status_code}")
                return set()
                
            super_orders = response.json()
            active_orders = set()
            active_statuses = {"PENDING", "PART_TRADED", "TRADED"}

            for order in super_orders:
                status = order.get("orderStatus", "")
                symbol = order.get("tradingSymbol", "")
                oid    = order.get('orderId', '')

                if status in {"PENDING", "PART_TRADED"}:
                    active_orders.add((symbol, oid, 'ENTRY_LEG'))

                if status in active_statuses:
                    for ord_leg in order.get('legDetails', []):
                        if ord_leg.get('orderStatus', '') == 'PENDING':
                            active_orders.add((symbol, oid, ord_leg.get('legName', '')))

            print(f"[✓] Total Active Super Orders: {len(active_orders)}")
            return active_orders
            
        except RequestException as e:
            print(f"[✗] Network error fetching super orders: {e}")
            if retry:
                print('[!] Attempting to restart SSH proxy and retry fetching super orders...')
                self.proxy_manager.restart()
                time.sleep(2)
                return self.get_active_super_orders(retry=False)
            return set()

    def cancel_super_order(self, order_id: str, order_leg: str = "ENTRY_LEG", retry: bool = True) -> bool:
        """Cancels a specific leg of a super order."""
        url = f"{SUPER_ORDER_URL}/{order_id}/{order_leg}"
        try:
            response = self.session.delete(url, headers=self.api_headers, timeout=10)
            if response.status_code in (200, 202):
                result = response.json() if response.text else {}
                status = result.get('orderStatus', 'CANCELLED')
                print(f"  [✓] Cancelled Super Order: {order_id} | Status: {status}")
                return True
            else:
                print(f"  [✗] Failed to cancel {order_id}: {response.status_code}")
                return False
                
        except RequestException as e:
            print(f"  [✗] Network error cancelling {order_id}: {e}")
            if retry:
                print(f'[!] Attempting to restart SSH proxy and retry cancelling {order_id}...')
                self.proxy_manager.restart()
                time.sleep(2)
                return self.cancel_super_order(order_id, order_leg, retry=False)
            return False

    def clean_orphaned_orders(self):
        """Orchestrates canceling super orders whose symbols are no longer in active positions."""
        print(f"\n─── Starting Cleanup Cycle ───")
        active_symbols = self.get_active_positions()
        active_super_orders = self.get_active_super_orders()

        cancelled_symb = []

        for (symb, oid, legname) in active_super_orders:
            if symb not in active_symbols:
                if self.cancel_super_order(oid, legname):
                    cancelled_symb.append(symb)
        
        if cancelled_symb:
            print(f"\n[!] Cleanup Complete. Cancelled {len(cancelled_symb)} orphaned orders.")
            print(f"    Symbols: {cancelled_symb}")
        else:
            print("\n[✓] Cleanup Complete. No orphaned orders found.")


# ───────────────────────────────────────
# Test Execution
# ───────────────────────────────────────
if __name__ == '__main__':
    trader = DhanTrader()
    trader.begin_session()
    trader.fire_trade('NIFTY', 'NSE', 'BUY', entry_val=23333,quant=15)
    
    # Run cleanup independently
    trader.clean_orphaned_orders()