"""
dhan_trade.py — Dhan HQ automated order placement
(Object-Oriented).
"""

import os
import math
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
from utils.data.paths import OUT_DIR
import tomllib

# ───────────────────────────────────────
# Proxy (set before any network call)
# ───────────────────────────────────────
os.environ.update({
    'HTTP_PROXY':  'socks5h://localhost:9090',
    'HTTPS_PROXY': 'socks5h://localhost:9090',
    'ALL_PROXY':   'socks5h://localhost:9090',
})

# ───────────────────────────────────────
# Paths & constants
# ───────────────────────────────────────
BASE_DIR         = Path(__file__).parent
SYMBOLS_CONFIG   = BASE_DIR / 'symbols_config.toml'
LOCAL_CSV        = Path(OUT_DIR) / 'scrip_master.csv'
ACCESS_FILE_PATH = BASE_DIR / 'access_token.toml'

ORDER_URL        = 'https://api.dhan.co/v2/orders'
SUPER_ORDER_URL  = 'https://api.dhan.co/v2/super/orders'

ENTRY_PRICE_PERC = 0.1
LIMIT_PRICE_PERC = 0.2
TARGET_PERC      = 4.0
STOP_LOSS_PERC   = 0.7
STOP_TRAIL_PERC  = 0.5

FILTER_SEG  = frozenset({'EQUITY', 'OPTSTK', 'OPTIDX', 'OPTFUT'})
FILTER_EXCH = frozenset({'NSE', 'MCX'})
SCRIP_COLS  = [
    'SEM_EXM_EXCH_ID', 'SEM_INSTRUMENT_NAME',
    'SEM_TRADING_SYMBOL', 'SEM_SMST_SECURITY_ID',
    'SEM_LOT_UNITS', 'SEM_EXPIRY_DATE'
]

SEG_EXCHANGE_SUFFIX = {
    'EQUITY': 'EQ',
    'OPTFUT': 'COMM',
    'OPTIDX': 'FNO',
    'OPTSTK': 'FNO',
}

# ───────────────────────────────────────
# Credentials Initialization
# ───────────────────────────────────────
def _load_credentials(path: Path) -> tuple[str, str]:
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
            client_id = data.get("CLIENT_ID", "").strip()
            access_token = data.get("ACCESS_TOKEN", "").strip()
            if not client_id or not access_token:
                raise ValueError(f"{path} must contain CLIENT_ID "
                                "and ACCESS_TOKEN fields.")
            return client_id, access_token
    except Exception as exc:
        print(f"Error reading credentials from {path}: {exc}")
        return "", ""

CLIENT_ID, ACCESS_TOKEN = _load_credentials(ACCESS_FILE_PATH)

try:
    from dhanhq import dhanhq, DhanContext  # noqa: E402
    if CLIENT_ID and ACCESS_TOKEN:
        _dhan_ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        _dhan_api = dhanhq(_dhan_ctx)
    else:
        _dhan_api = None
except ImportError:
    _dhan_api = None

API_HEADERS = {
    'access-token': ACCESS_TOKEN,
    'Content-Type': 'application/json',
    'Accept':       'application/json',
}

# ───────────────────────────────────────
# SymbolsConfig & ScripMaster
# ───────────────────────────────────────
class SymbolsConfig:
    def __init__(self, path: Path):
        self._path: Path = path
        self._mtime: Optional[float] = None
        self._config: dict = {}

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
            with open(self._path, "rb") as f:
                self._config = tomllib.load(f) or {}
            print('Symbol config loaded.')
        except Exception as exc:
            print(f'Failed to parse TOML config: {exc}')


class ScripMaster:
    def __init__(self):
        self._df: Optional[pd.DataFrame] = None

    def _ensure_loaded(self):
        if self._df is not None:
            return
        if not LOCAL_CSV.exists():
            print(f'{LOCAL_CSV} not found. Downloading...')
            if _dhan_api:
                _dhan_api.fetch_security_list('compact', str(LOCAL_CSV))
            self._load_and_save(save_filtered=True)
        else:
            self._load_and_save(save_filtered=False)

    def _load_and_save(self, *, save_filtered: bool):
        chunks = []
        try:
            with pd.read_csv(LOCAL_CSV, usecols=SCRIP_COLS,
                             chunksize=50_000, low_memory=False) as reader:
                for chunk in reader:
                    mask = (
                        chunk['SEM_EXM_EXCH_ID'].isin(FILTER_EXCH) &
                        chunk['SEM_INSTRUMENT_NAME'].isin(FILTER_SEG)
                    )
                    filtered = chunk.loc[mask]
                    if not filtered.empty:
                        chunks.append(filtered)
        except FileNotFoundError:
            print(f"Error: {LOCAL_CSV} missing, unable to load "
                  "scrip master.")
            return
        if not chunks:
            return
        df = pd.concat(chunks, ignore_index=True)
        df['_EXPIRY_DATE_STR'] = (
            df['SEM_EXPIRY_DATE'].astype(str).str.split().str[0]
        )
        df.set_index([
            'SEM_EXM_EXCH_ID', 'SEM_INSTRUMENT_NAME',
            'SEM_TRADING_SYMBOL'
        ], inplace=True)
        df.sort_index(inplace=True)
        self._df = df
        if save_filtered:
            df.reset_index().to_csv(LOCAL_CSV, index=False)
            print(f'Saved filtered scrip master to {LOCAL_CSV}.')
        print('Scrip master loaded.')

    def lookup(self, symbol: str, exch: str, seg: str,
               expiry_date: str = '') -> tuple[Optional[str], int]:
        self._ensure_loaded()
        if self._df is None:
            return None, 0
        try:
            rows = self._df.loc[(exch, seg, symbol)]
        except KeyError:
            print(f'Error: {symbol} ({exch}/{seg}) not found in '
                  'scrip master. Skipping.')
            return None, 0

        if isinstance(rows, pd.Series):
            rows = rows.to_frame().T

        if expiry_date:
            date_key = str(expiry_date).split()[0]
            rows = rows[rows['_EXPIRY_DATE_STR'] == date_key]

        if rows.empty:
            print(f'Error: {symbol} with expiry {expiry_date} not '
                  'found. Skipping.')
            return None, 0

        row = rows.iloc[0]
        return str(row['SEM_SMST_SECURITY_ID']), int(row['SEM_LOT_UNITS'])

# ───────────────────────────────────────
# Pure Utility Functions
# ───────────────────────────────────────
def _round_strike(price: float) -> int:
    if price == 0:
        return 0
    digits = len(str(abs(int(price))))
    step = 10 ** (1 if digits <= 3 else (2 if digits <= 5 else 3))
    return int((price // step) * step)

def _adjust_price(base: float, perc: float, signal: str) -> float:
    if signal == 'BUY':
        return math.ceil(base * (1 + perc / 100))
    return math.floor(base * (1 - perc / 100))

@dataclass
class PriceLevels:
    entry: float
    limit: float
    stop_loss: float
    target: float
    trail: float

def compute_price_levels(raw_entry: float, signal: str) -> PriceLevels:
    entry = _adjust_price(raw_entry, ENTRY_PRICE_PERC, signal)
    limit = _adjust_price(entry, LIMIT_PRICE_PERC, signal)
    stop_loss = _adjust_price(entry, STOP_LOSS_PERC,
                             'SELL' if signal == 'BUY' else 'BUY')
    target = _adjust_price(entry, TARGET_PERC, signal)
    trail = math.ceil(entry * STOP_TRAIL_PERC / 100)
    return PriceLevels(entry, limit, stop_loss, target, trail)

def compute_quantity(trade_amount: float, price: float,
                    lot_size: int, base_quant: int) -> int:
    if trade_amount > 0 and price > 0:
        lots = int(trade_amount // (price * lot_size)) + 1
        return lots * lot_size
    return base_quant * lot_size

def _signal_to_opt(signal: str) -> str:
    return 'CE' if signal == 'BUY' else 'PE'

def _format_expiry(expiry_date, fmt: str) -> str:
    if not expiry_date:
        return ''
    date_part = str(expiry_date).split()[0]
    try:
        return datetime.strptime(date_part, '%Y-%m-%d').strftime(fmt)
    except ValueError:
        return ''

def build_option_symbol(base: str, expiry_date, strike: int,
                       opt_type: str, fmt: str) -> str:
    return f'{base}-{_format_expiry(expiry_date, fmt)}-{strike}-{opt_type}'

@dataclass
class Instrument:
    symb: str
    exch: str
    seg: str
    expiry_date: str = ''
    signal: str = ''
    quant: int = 1
    entry_val: float = 0.0
    trade_amount: float = 0.0

# ───────────────────────────────────────
# Core Dhan API Class
# ───────────────────────────────────────
class DhanTrader:
    """Object-oriented wrapper managing state, session, config,
    and orders."""

    def __init__(self):
        self.session = requests.Session()
        self.cfg = SymbolsConfig(SYMBOLS_CONFIG)
        self.scrip = ScripMaster()
        self.traded_this_scan = set()

    def begin_session(self):
        """Prep the trader for a new minute/cycle."""
        self.cfg.refresh()
        self.traded_this_scan.clear()

    def _resolve_nse(self, symb: str, signal: str, entry_val: float,
                     quant: int) -> Optional[Instrument]:
        
        def_expiry = self.cfg.get('def_expiry_date', '')
        def_quant = self.cfg.get('def_quantity', 1)
        def_trade_amount = self.cfg.get('def_trade_amount', 10000)
        def_order_mode = self.cfg.get('def_order_mode', '')


        nse_index = self.cfg.get('nse_index', {})

        if symb in nse_index.get('symbols',{}):
            cur_config = nse_index.get('config',{})
            ord_mode = cur_config.get('order_mode',def_order_mode)
            expiry = cur_config.get('expiry_date',def_expiry)


            if ord_mode != 'OPT':
                return None


            sym_data = nse_index['symbols'][symb] or {}
            expiry = sym_data.get('expiry_date', expiry)
            opt_type = _signal_to_opt(signal)
            strike = sym_data.get('strike', _round_strike(entry_val))
            sig_strk = 'call_strike' if signal == 'BUY' else 'put_strike'
            strike = sym_data.get(sig_strk, strike)
            final_symb = build_option_symbol(
                sym_data.get('symbol', symb), expiry, strike,
                opt_type, '%b%Y')
            return Instrument(
                symb=final_symb, exch='NSE', seg='OPTIDX',
                expiry_date=expiry, signal='BUY',
                quant=sym_data.get('quantity', def_quant)
            )


        nse_stocks = self.cfg.get('nse_stocks', {})
        cur_config = nse_stocks.get('config',{})
        ord_mode = cur_config.get('order_mode',def_order_mode)
        expiry = cur_config.get('expiry_date',def_expiry)
        trade_amount = cur_config.get('trade_amount',def_trade_amount)

        if ord_mode == 'EQ':
            return Instrument(
                symb=symb, exch='NSE', seg='EQUITY', signal=signal,
                quant=quant, entry_val=entry_val,
                trade_amount=trade_amount
            )
        if ord_mode == 'OPT':
            final_symb = build_option_symbol(
                symb, expiry, _round_strike(entry_val),
                _signal_to_opt(signal), '%b%Y'
            )
            return Instrument(
                symb=final_symb, exch='NSE', seg='OPTSTK',
                expiry_date=expiry, signal='BUY', quant=def_quant
            )

            
        return None



    def _resolve_mcx(self, symb: str, signal: str,
                     entry_val: float) -> Optional[Instrument]:

 

        def_expiry = self.cfg.get('def_expiry_date', '')
        def_quant = self.cfg.get('def_quantity', 1)
        def_trade_amount = self.cfg.get('def_trade_amount', 10000)
        def_order_mode = self.cfg.get('def_order_mode', '')


        mcx_comm = self.cfg.get('mcx_comm', {})
        mcx_data = mcx_comm.get('symbols', {})

        if symb in mcx_data:
            cur_config = mcx_comm.get('config',{})
            ord_mode = cur_config.get('order_mode',def_order_mode)
            expiry = cur_config.get('expiry_date',def_expiry)


            if ord_mode != 'OPT':
                return None


            sym_data = mcx_data[symb] or {}
            expiry = sym_data.get('expiry_date', expiry)
            opt_type = _signal_to_opt(signal)
            strike = sym_data.get('strike', _round_strike(entry_val))
            sig_strk = 'call_strike' if signal == 'BUY' else 'put_strike'
            strike = sym_data.get(sig_strk, strike)


            final_symb = build_option_symbol(
                sym_data.get('symbol', symb), expiry, strike,
                _signal_to_opt(signal), '%d%b%Y'
            )
            return Instrument(
                symb=final_symb, exch='MCX', seg='OPTFUT',
                expiry_date=expiry, signal='BUY',
                quant=sym_data.get('quantity',
                                self.cfg.get('def_quantity', 1))
            )
        
        return None

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
            print(f'No valid segment configured for {symb} on '
                  f'{exch}. Skipping.')
        return inst

    def _post_order(self, url: str, payload: dict, label: str = ''):
        try:
            resp = self.session.post(
                url, json=payload, headers=API_HEADERS
            )
            if resp.status_code != 200:
                print(f'[✗] {label} Order failed ({resp.status_code}): '
                      f'{resp.text}\nPayload: {payload}')
        except requests.RequestException as exc:
            print(f'[✗] {label} Network error: {exc}')

    def place_super_order(
        self, symb: str, exchange_seg: str, lot_size: int,
        sec_id: str, signal: str, quant: int, entry_val: float,
        trade_amount: float
    ):
        if entry_val == 0:
            payload = {
                'dhanClientId': CLIENT_ID,
                'correlationId': f'auto_{CLIENT_ID}',
                'transactionType': signal,
                'exchangeSegment': exchange_seg,
                'productType': 'INTRADAY',
                'orderType': 'MARKET',
                'validity': 'DAY',
                'securityId': sec_id,
                'quantity': quant * lot_size,
                'price': 0
            }
            print(f'##### MARKET | {symb} | {sec_id} | {signal} | '
                  f'qty={quant * lot_size}')
            self._post_order(ORDER_URL, payload, label='MARKET')
            return

        levels = compute_price_levels(entry_val, signal)
        total_quant = compute_quantity(
            trade_amount, levels.entry, lot_size, quant
        )
        print(f'##### SUPER | {symb} | {sec_id} | {signal} | '
              f'qty={total_quant} | entry={levels.entry} | '
              f'amt={round(total_quant * levels.entry)}')

        payload = {
            'dhanClientId': CLIENT_ID,
            'correlationId': f'auto_{CLIENT_ID}',
            'transactionType': signal,
            'exchangeSegment': exchange_seg,
            'productType': 'INTRADAY',
            'orderType': 'LIMIT',
            'validity': 'DAY',
            'securityId': sec_id,
            'quantity': total_quant,
            'price': levels.limit,
            'stopLossPrice': levels.stop_loss,
            'trailingJump': levels.trail
        }
        self._post_order(SUPER_ORDER_URL, payload, label='SUPER')

    def fire_trade(self, symb: str, exch: str, signal: str,
                   quant: int = 1, entry_val: float = 0):
        """Public method to fire a trade for a single symbol."""
        trade_key = f'{exch}:{symb}:{signal}'
        if trade_key in self.traded_this_scan:
            print(f'[skip] {trade_key} already traded this '
                  'scan cycle.')
            return
        self.traded_this_scan.add(trade_key)
        inst = self.resolve_instrument(
            symb, exch, signal, quant, entry_val
        )
        if inst is None:
            return

        exchange_seg = (f'{inst.exch}_{SEG_EXCHANGE_SUFFIX[inst.seg]}')
        sec_id, lot_size = self.scrip.lookup(
            inst.symb, inst.exch, inst.seg, inst.expiry_date
        )
        if sec_id is None:
            return

        self.place_super_order(
            inst.symb, exchange_seg, lot_size, sec_id,
            inst.signal, inst.quant, inst.entry_val, inst.trade_amount
        )

# ───────────────────────────────────────
# Test Execution
# ───────────────────────────────────────
if __name__ == '__main__':
    trader = DhanTrader()
    trader.begin_session()
    trader.fire_trade('RELIANCE', 'NSE', 'BUY', quant=15)
