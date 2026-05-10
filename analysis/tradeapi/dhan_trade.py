"""
dhan_trade.py — Dhan HQ automated order placement (Object-Oriented).
"""

import os
import math
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from utils.data.paths import OUT_DIR
import tomllib
from tradeapi.price_strike_calc import get_price_strike, get_strike_interval

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
    'SEM_LOT_UNITS', 'SEM_EXPIRY_DATE',
]

SEG_EXCHANGE_SUFFIX = {
    'EQUITY': 'EQ',
    'OPTFUT': 'COMM',
    'OPTIDX': 'FNO',
    'OPTSTK': 'FNO',
}

OPT_SEGMENTS = frozenset({'OPTSTK', 'OPTIDX', 'OPTFUT'})

# ───────────────────────────────────────
# Credentials
# ───────────────────────────────────────
def _load_credentials(path: Path) -> tuple[str, str]:
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
# Pure utility helpers
# ───────────────────────────────────────
def _signal_to_opt(signal: str) -> str:
    """'BUY' → 'CE', 'SELL' → 'PE'."""
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


def _adjust_price(base: float, perc: float, signal: str) -> float:
    if signal == 'BUY':
        return math.ceil(base * (1 + perc / 100))
    return math.floor(base * (1 - perc / 100))


# ───────────────────────────────────────
# Fallback strike helpers
# ───────────────────────────────────────

# Standard exchange strike steps in ascending order — shared with price_strike_calc
_FALLBACK_STEPS = (1, 2, 5, 10, 20, 25, 50, 100, 200, 500, 1_000, 5_000)


def _next_round_step(current_step: int) -> Optional[int]:
    """
    Return the first standard step strictly larger than *current_step*.

    Used to escalate to a rounder grid when an auto-calculated strike
    is not listed in the scrip master.

    Examples
    --------
    50  → 100   (stock/NIFTY step=50 → try 100)
    100 → 200   (BANKNIFTY step=100 → try 200)
    20  → 25    (SBIN step=20 → try 25)
    """
    for s in _FALLBACK_STEPS:
        if s > current_step:
            return s
    return None


def _parse_option_symbol(symbol: str) -> Optional[tuple[str, str, int, str]]:
    """
    Parse 'BASE-expiry-STRIKE-TYPE' from the right.

    Splits on the rightmost 3 hyphens so base names with hyphens
    (e.g. 'M&M-FIN') are handled correctly.

    Returns (base, expiry_str, strike_int, opt_type) or None.
    """
    parts = symbol.rsplit('-', 3)
    if len(parts) != 4:
        return None
    base, expiry, strike_str, opt_type = parts
    if opt_type not in ('CE', 'PE'):
        return None
    try:
        return base, expiry, int(strike_str), opt_type
    except ValueError:
        return None


def _build_fallback_symbol(option_symbol: str) -> Optional[str]:
    """
    Return a new option symbol with a rounder strike, or None if
    no better alternative exists.

    Algorithm
    ---------
    1. Parse base, expiry, strike, opt_type from the symbol string.
    2. Re-derive the instrument's standard step via get_strike_interval
       (using the strike as a close proxy for entry price).
    3. Escalate to the next standard step with _next_round_step.
    4. Round CE strikes DOWN, PE strikes UP to the new step.
    5. Return None if the strike is unchanged (already on that grid).

    Examples
    --------
    'ASIANPAINT-May2026-2550-CE' → 'ASIANPAINT-May2026-2500-CE'  (50→100)
    'TVSMOTOR-May2026-3650-PE'   → 'TVSMOTOR-May2026-3700-PE'    (50→100)
    'CRUDEOIL-19May2026-6475-PE' → 'CRUDEOIL-19May2026-6500-PE'  (50→100)
    'NATURALGAS-May2026-185-CE'  → 'NATURALGAS-May2026-180-CE'   (10→20)
    'NIFTY-May2026-22500-CE'     → None  (22500 % 100 == 0, unchanged)
    """
    parsed = _parse_option_symbol(option_symbol)
    if parsed is None:
        return None
    base, expiry, strike, opt_type = parsed

    orig_step = get_strike_interval(base, strike)   # strike ≈ entry price
    fb_step   = _next_round_step(orig_step)
    if fb_step is None:
        return None

    if opt_type == 'CE':
        new_strike = int(math.floor(strike / fb_step) * fb_step)
    else:  # PE
        new_strike = int(math.ceil(strike / fb_step) * fb_step)

    if new_strike == strike:
        return None

    return f'{base}-{expiry}-{new_strike}-{opt_type}'


# ───────────────────────────────────────
# Price levels
# ───────────────────────────────────────
@dataclass
class PriceLevels:
    entry:     float
    limit:     float
    stop_loss: float
    target:    float
    trail:     float


def compute_price_levels(raw_entry: float, signal: str) -> PriceLevels:
    entry     = _adjust_price(raw_entry, ENTRY_PRICE_PERC, signal)
    limit     = _adjust_price(entry, LIMIT_PRICE_PERC, signal)
    stop_loss = _adjust_price(entry, STOP_LOSS_PERC,
                              'SELL' if signal == 'BUY' else 'BUY')
    target    = _adjust_price(entry, TARGET_PERC, signal)
    trail     = math.ceil(entry * STOP_TRAIL_PERC / 100)
    return PriceLevels(entry, limit, stop_loss, target, trail)


def compute_quantity(trade_amount: float, price: float,
                     lot_size: int, base_quant: int) -> int:
    if trade_amount > 0 and price > 0:
        lots = int(trade_amount // (price * lot_size)) + 1
        return lots * lot_size
    return base_quant * lot_size


# ───────────────────────────────────────
# Instrument dataclass
# ───────────────────────────────────────
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


# ───────────────────────────────────────
# SymbolsConfig — hot-reloads on file change
# ───────────────────────────────────────
class SymbolsConfig:
    def __init__(self, path: Path):
        self._path:   Path           = path
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
# ScripMaster — loads & indexes Dhan CSV
# ───────────────────────────────────────
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
            print(f'Error: {LOCAL_CSV} missing, unable to load scrip master.')
            return
        if not chunks:
            return
        df = pd.concat(chunks, ignore_index=True)
        df['_EXPIRY_DATE_STR'] = (
            df['SEM_EXPIRY_DATE'].astype(str).str.split().str[0]
        )
        df.set_index(
            ['SEM_EXM_EXCH_ID', 'SEM_INSTRUMENT_NAME', 'SEM_TRADING_SYMBOL'],
            inplace=True,
        )
        df.sort_index(inplace=True)
        self._df = df
        if save_filtered:
            df.reset_index().to_csv(LOCAL_CSV, index=False)
            print(f'Saved filtered scrip master to {LOCAL_CSV}.')
        print('Scrip master loaded.')

    def lookup(self, symbol: str, exch: str, seg: str,
               expiry_date: str = '', *, silent: bool = False
               ) -> tuple[Optional[str], int]:
        """
        Look up (security_id, lot_size) for an instrument.

        Parameters
        ----------
        silent : suppress the 'not found' error log (used during
                 fallback retries so only one final message is printed).
        """
        self._ensure_loaded()
        if self._df is None:
            return None, 0
        try:
            rows = self._df.loc[(exch, seg, symbol)]
        except KeyError:
            if not silent:
                print(f'Error: {symbol} ({exch}/{seg}) not found in '
                      'scrip master. Skipping.')
            return None, 0

        if isinstance(rows, pd.Series):
            rows = rows.to_frame().T

        if expiry_date:
            date_key = str(expiry_date).split()[0]
            rows = rows[rows['_EXPIRY_DATE_STR'] == date_key]

        if rows.empty:
            if not silent:
                print(f'Error: {symbol} with expiry {expiry_date} not found. Skipping.')
            return None, 0

        row = rows.iloc[0]
        return str(row['SEM_SMST_SECURITY_ID']), int(row['SEM_LOT_UNITS'])

    def lookup_with_fallback(self, symbol: str, exch: str, seg: str,
                              expiry_date: str = '') -> tuple[Optional[str], int, str]:
        """
        Look up an instrument, automatically retrying once with a
        rounder strike when the first lookup fails (option segments only).

        Returns (security_id, lot_size, resolved_symbol).
        resolved_symbol may differ from symbol when a fallback was used.
        """
        sec_id, lot_size = self.lookup(symbol, exch, seg, expiry_date, silent=True)
        if sec_id is not None:
            return sec_id, lot_size, symbol

        # Only attempt fallback for option symbols
        if seg not in OPT_SEGMENTS:
            print(f'Error: {symbol} ({exch}/{seg}) not found in scrip master. Skipping.')
            return None, 0, symbol

        fb_symbol = _build_fallback_symbol(symbol)
        if fb_symbol:
            sec_id, lot_size = self.lookup(fb_symbol, exch, seg, expiry_date, silent=True)
            if sec_id is not None:
                print(f'[fallback] {symbol} → {fb_symbol}')
                return sec_id, lot_size, fb_symbol

        # Both attempts failed
        print(f'Error: {symbol} ({exch}/{seg}) not found in scrip master. Skipping.')
        return None, 0, symbol


# ───────────────────────────────────────
# Core Dhan API class
# ───────────────────────────────────────
class DhanTrader:
    """Object-oriented wrapper managing state, session, config, and orders."""

    def __init__(self):
        self.session = requests.Session()
        self.cfg     = SymbolsConfig(SYMBOLS_CONFIG)
        self.scrip   = ScripMaster()
        self.traded_this_scan: set = set()

    # ── Session ──────────────────────────
    def begin_session(self):
        """Prepare the trader for a new scan cycle."""
        self.cfg.refresh()
        self.traded_this_scan.clear()

    # ── Config helpers ───────────────────
    def _defaults(self) -> dict:
        """Read top-level defaults from config (single access point)."""
        return {
            'expiry':       self.cfg.get('def_expiry_date', ''),
            'quant':        self.cfg.get('def_quantity', 1),
            'trade_amount': self.cfg.get('def_trade_amount', 10_000),
            'order_mode':   self.cfg.get('def_order_mode', ''),
        }

    # ── Shared option-instrument builder ─
    def _build_opt_instrument(
        self,
        sym_data:    dict,
        base_symb:   str,
        expiry:      str,
        signal:      str,
        entry_val:   float,
        exch:        str,
        seg:         str,
        def_quant:   int,
        expiry_fmt:  str,
    ) -> Instrument:
        """
        Build an Instrument for any option segment (OPTIDX / OPTSTK / OPTFUT).

        sym_data  : per-symbol overrides from the TOML config (may be empty).
        base_symb : underlying name used for auto strike calculation.
        expiry    : section-level default expiry (overridden by sym_data).
        expiry_fmt: strftime format for the exchange ('%b%Y' NSE, '%d%b%Y' MCX).
        """
        expiry   = sym_data.get('expiry_date', expiry)
        opt_type = _signal_to_opt(signal)

        # Strike resolution priority:
        #   1. explicit 'call_strike' / 'put_strike' key in config
        #   2. generic 'strike' key
        #   3. auto-calculated from entry price
        auto_strike = get_price_strike(base_symb, entry_val, signal)
        strike = sym_data.get('strike', auto_strike)
        sig_key = 'call_strike' if signal == 'BUY' else 'put_strike'
        strike = sym_data.get(sig_key, strike)

        final_symb = build_option_symbol(
            sym_data.get('symbol', base_symb), expiry, strike, opt_type, expiry_fmt
        )
        return Instrument(
            symb=final_symb, exch=exch, seg=seg,
            expiry_date=expiry, signal='BUY',
            quant=sym_data.get('quantity', def_quant),
        )

    # ── Exchange resolvers ────────────────
    def _resolve_nse(self, symb: str, signal: str,
                     entry_val: float, quant: int) -> Optional[Instrument]:
        dfl = self._defaults()

        # ── NSE Indices (OPTIDX) ──────────
        nse_index = self.cfg.get('nse_index', {})
        if symb in nse_index.get('symbols', {}):
            idx_cfg = nse_index.get('config', {})
            if idx_cfg.get('order_mode', dfl['order_mode']) != 'OPT':
                return None
            sym_data = nse_index['symbols'][symb] or {}
            expiry   = idx_cfg.get('expiry_date', dfl['expiry'])
            return self._build_opt_instrument(
                sym_data, symb, expiry, signal, entry_val,
                exch='NSE', seg='OPTIDX',
                def_quant=dfl['quant'], expiry_fmt='%b%Y',
            )

        # ── NSE Stocks (EQUITY / OPTSTK) ─
        nse_stocks = self.cfg.get('nse_stocks', {})
        stk_cfg    = nse_stocks.get('config', {})
        ord_mode   = stk_cfg.get('order_mode', dfl['order_mode'])
        expiry     = stk_cfg.get('expiry_date', dfl['expiry'])

        if ord_mode == 'EQ':
            return Instrument(
                symb=symb, exch='NSE', seg='EQUITY',
                signal=signal, quant=quant,
                entry_val=entry_val,
                trade_amount=stk_cfg.get('trade_amount', dfl['trade_amount']),
            )

        if ord_mode == 'OPT':
            return self._build_opt_instrument(
                sym_data={}, base_symb=symb, expiry=expiry,
                signal=signal, entry_val=entry_val,
                exch='NSE', seg='OPTSTK',
                def_quant=dfl['quant'], expiry_fmt='%b%Y',
            )

        return None

    def _resolve_mcx(self, symb: str, signal: str,
                     entry_val: float) -> Optional[Instrument]:
        dfl = self._defaults()

        mcx_comm = self.cfg.get('mcx_comm', {})
        mcx_data = mcx_comm.get('symbols', {})
        if symb not in mcx_data:
            return None

        comm_cfg = mcx_comm.get('config', {})
        if comm_cfg.get('order_mode', dfl['order_mode']) != 'OPT':
            return None

        sym_data = mcx_data[symb] or {}
        expiry   = comm_cfg.get('expiry_date', dfl['expiry'])
        return self._build_opt_instrument(
            sym_data, symb, expiry, signal, entry_val,
            exch='MCX', seg='OPTFUT',
            def_quant=dfl['quant'], expiry_fmt='%d%b%Y',
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

    # ── Order placement ──────────────────
    def _post_order(self, url: str, payload: dict, label: str = ''):
        try:
            resp = self.session.post(url, json=payload, headers=API_HEADERS)
            if resp.status_code != 200:
                print(f'[✗] {label} Order failed ({resp.status_code})')
        except Exception as exc:
            print(f'[✗] {label} Network error: {exc}')

    def place_super_order(
        self, symb: str, exchange_seg: str, lot_size: int,
        sec_id: str, signal: str, quant: int,
        entry_val: float, trade_amount: float,
    ):
        if entry_val == 0:
            payload = {
                'dhanClientId':    CLIENT_ID,
                'correlationId':   f'auto_{CLIENT_ID}',
                'transactionType': signal,
                'exchangeSegment': exchange_seg,
                'productType':     'INTRADAY',
                'orderType':       'MARKET',
                'validity':        'DAY',
                'securityId':      sec_id,
                'quantity':        quant * lot_size,
                'price':           0,
            }
            print(f'##### MARKET | {symb} | {sec_id} | {signal} | '
                  f'qty={quant * lot_size}')
            self._post_order(ORDER_URL, payload, label='MARKET')
            return

        levels     = compute_price_levels(entry_val, signal)
        total_quant = compute_quantity(trade_amount, levels.entry, lot_size, quant)
        print(f'##### SUPER | {symb} | {sec_id} | {signal} | '
              f'qty={total_quant} | entry={levels.entry} | '
              f'amt={round(total_quant * levels.entry)}')

        payload = {
            'dhanClientId':    CLIENT_ID,
            'correlationId':   f'auto_{CLIENT_ID}',
            'transactionType': signal,
            'exchangeSegment': exchange_seg,
            'productType':     'INTRADAY',
            'orderType':       'LIMIT',
            'validity':        'DAY',
            'securityId':      sec_id,
            'quantity':        total_quant,
            'price':           levels.limit,
            'stopLossPrice':   levels.stop_loss,
            'trailingJump':    levels.trail,
        }
        self._post_order(SUPER_ORDER_URL, payload, label='SUPER')

    # ── Main entry point ─────────────────
    def fire_trade(self, symb: str, exch: str, signal: str,
                   quant: int = 1, entry_val: float = 0):
        """Resolve, look up, and place a single trade."""
        trade_key = f'{exch}:{symb}:{signal}'
        if trade_key in self.traded_this_scan:
            print(f'[skip] {trade_key} already traded this scan cycle.')
            return
        self.traded_this_scan.add(trade_key)

        inst = self.resolve_instrument(symb, exch, signal, quant, entry_val)
        if inst is None:
            return

        exchange_seg = f'{inst.exch}_{SEG_EXCHANGE_SUFFIX[inst.seg]}'

        # lookup_with_fallback: tries original strike, then rounder strike,
        # then gives up — all in one call, one final error log.
        sec_id, lot_size, resolved_symb = self.scrip.lookup_with_fallback(
            inst.symb, inst.exch, inst.seg, inst.expiry_date
        )
        if sec_id is None:
            return
        inst.symb = resolved_symb   # update to whichever symbol was found

        self.place_super_order(
            inst.symb, exchange_seg, lot_size, sec_id,
            inst.signal, inst.quant, inst.entry_val, inst.trade_amount,
        )


# ───────────────────────────────────────
# Test Execution
# ───────────────────────────────────────
if __name__ == '__main__':
    trader = DhanTrader()
    trader.begin_session()
    trader.fire_trade('RELIANCE', 'NSE', 'BUY', quant=15)