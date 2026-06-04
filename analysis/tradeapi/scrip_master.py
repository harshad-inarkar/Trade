"""
scrip_master.py — Isolated module for managing instrument data and search.
"""

# stdlib
import bisect
import io
import math
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Optional

# third-party
import pandas as pd
import requests

# local
from tradeapi.price_strike_calc import get_strike_interval
from utils.data.paths import OUT_DIR

# ───────────────────────────────────────
# Constants & Paths
# ───────────────────────────────────────
BASE_DIR          = Path(__file__).parent
LOCAL_CSV         = Path(OUT_DIR) / 'scrip_master.csv'
SEARCH_CONFIG_PATH = BASE_DIR / 'scrip_search.toml'

INSTRUMENT_SEGMENTS = ['NSE_EQ', 'NSE_FNO', 'MCX_COMM', 'IDX_I']
INSTRUMENT_URL      = 'https://api.dhan.co/v2/instrument/{segment}'

FILTER_EXCH = frozenset({'NSE', 'MCX'})

FILTER_SEG = frozenset({
    'EQUITY', 'INDEX',
    'OPTSTK', 'OPTIDX', 'OPTFUT',
    'FUTIDX', 'FUTCOM', 'FUTSTK',
})

FILTER_INST_TYPE = frozenset({'ES', 'EQ', 'OP', 'FUT', 'OPTFUT', 'FUTCOM', 'INDEX'})

SCRIP_COLS = [
    'EXCH_ID', 'INSTRUMENT', 'INSTRUMENT_TYPE',
    'UNDERLYING_SYMBOL', 'DISPLAY_NAME', 'SECURITY_ID', 'UNDERLYING_SECURITY_ID',
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

_FALLBACK_STEPS = (1, 2, 5, 10, 20, 25, 50, 100, 200, 500, 1_000, 5_000)


# ───────────────────────────────────────
# Data Classes & Utilities
# ───────────────────────────────────────
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


def _get_today_str() -> str:
    return datetime.now().strftime('%Y-%m-%d')


def _next_round_step(current_step: int) -> Optional[int]:
    for step in _FALLBACK_STEPS:
        if step > current_step:
            return step
    return None


def _get_fallback_strike(base: str, strike: float, opt_type: str) -> Optional[float]:
    fb_step = _next_round_step(get_strike_interval(base, strike))
    if fb_step is None:
        return None

    if opt_type == 'CE':
        new_strike = math.floor(strike / fb_step) * fb_step
    else:
        new_strike = math.ceil(strike / fb_step) * fb_step

    return float(new_strike) if new_strike != strike else None


# ───────────────────────────────────────
# Config Loading
# ───────────────────────────────────────
class SearchConfig:
    """Loads and encapsulates TOML search settings."""
    def __init__(self, path: Path):
        self.stop_words: set[str] = set()
        self.call_words: set[str] = set()
        self.put_words: set[str]  = set()
        self.fut_words: set[str]  = set()
        self.month_aliases: dict[str, str] = {}
        self.gen_tags: dict[int, str] = {}
        self._load(path)

    def _load(self, path: Path) -> None:
        if not path.exists():
            print(f"[!] Warning: {path} not found. Using empty defaults.")
            return

        with open(path, 'rb') as f:
            data = tomllib.load(f).get('search', {})

        self.stop_words = set(data.get('stop_words', {}).get('words', []))
        aliases = data.get('aliases', {})
        self.call_words = set(aliases.get('call', {}).get('words', []))
        self.put_words  = set(aliases.get('put', {}).get('words', []))
        self.fut_words  = set(aliases.get('fut', {}).get('words', []))
        self.month_aliases = data.get('month_aliases', {})

        raw_gen_tags = data.get('month_generation_tags', {})
        self.gen_tags = {
            int(k): str(v).upper() for k, v in raw_gen_tags.items()
        }


# ───────────────────────────────────────
# ScripMaster Engine
# ───────────────────────────────────────
class ScripMaster:
    def __init__(
        self, 
        session_obj: requests.Session, 
        refresh_master_scrip: bool = False
    ):
        self._eq_index     = None
        self._opt_index    = None
        self._expiry_index = None
        self._secid_info   = None
        self._search_data: list[dict]  = []
        self._display_to_data: dict[str, dict] = {}

        self.session = session_obj
        self.cfg = SearchConfig(SEARCH_CONFIG_PATH)
        
        self._ensure_loaded(refresh_master_scrip)

    def search_symbols(self, query: str, limit: int = 30) -> list[dict]:
        self._ensure_loaded()

        if not self._search_data:
            return []

        query = query.strip().upper()
        if len(query) < 2:
            return []

        parts = [p.strip() for p in query.replace('-', ' ').split() if p.strip()]

        symbol_tokens = set()
        month_token  = None
        strike_token = None
        opt_type     = None
        want_fut     = False

        for tok in parts:
            if tok in self.cfg.stop_words:
                continue
            if tok in self.cfg.month_aliases:
                month_token = self.cfg.month_aliases[tok]
                continue
            if tok in self.cfg.call_words:
                opt_type = 'CE'
                continue
            if tok in self.cfg.put_words:
                opt_type = 'PE'
                continue
            if tok in self.cfg.fut_words:
                want_fut = True
                continue
            if tok.isdigit():
                strike_token = tok
                continue

            tok = tok.upper()
            symbol_tokens.add(tok)
            if len(tok) >= 6:
                symbol_tokens.add(tok[:3])

        results = []

        for item in self._search_data:
            score = 0
            item_tokens = item['_search_tokens']

            # Symbol/Company Match
            matched = 0
            strong_match = False
            company_match = False
            sym_upper = item['symbol'].upper()

            for tok in symbol_tokens:
                if tok in item_tokens:
                    matched += 1
                    company_match = True
                if (sym_upper == tok or 
                    sym_upper.startswith(tok) or 
                    tok.startswith(sym_upper)):
                    strong_match = True

            if symbol_tokens and not (strong_match or company_match):
                continue

            score += matched * 100

            # Filter Matches
            if opt_type:
                if item['opt_type'] == opt_type:
                    score += 120
                else:
                    continue

            if want_fut:
                if item['inst_type'] == 'FUT':
                    score += 150
                else:
                    continue

            if strike_token:
                if not opt_type and item['inst_type'] != 'OPT':
                    continue
                item_strike = item['strike']
                if not item_strike:
                    continue
                strike_clean = (
                    str(int(item_strike))
                    if float(item_strike).is_integer()
                    else str(item_strike)
                )
                if strike_clean == strike_token:
                    score += 250
                else:
                    continue

            if month_token:
                if item['_month_tag'] == month_token:
                    score += 180
                else:
                    continue

            # Exact Match Boost
            for tok in symbol_tokens:
                if sym_upper == tok:
                    score += 500
                elif sym_upper.startswith(tok):
                    score += 250

            # Instrument Priority
            if item['inst_type'] == 'EQ':
                score += 10
            elif item['inst_type'] == 'FUT':
                score += 25
            else:
                score += 40

            # Expiry Priority (Nearer is Better)
            exp_sort = item['_expiry_sort']
            if exp_sort:
                try:
                    exp_num = int(exp_sort.replace('-', ''))
                    score += max(0, 100000000 - exp_num)
                except Exception:
                    pass

            results.append((score, item))

        results.sort(
            key=lambda x: (
                -x[0],
                x[1]['_expiry_sort'],
                x[1]['strike']
            )
        )

        final = []
        for _, item in results[:limit]:
            obj = dict(item)
            obj.pop('_search_tokens', None)
            obj.pop('_expiry_sort', None)
            obj.pop('_month_tag', None)
            obj.pop('_sort_key', None)

            try:
                obj['strike'] = float(obj.get('strike', 0))
            except Exception:
                obj['strike'] = 0.0

            final.append(obj)

        return final

    def get_symbol_name(self, sec_id: str, fallback: str = "") -> str:
        if not self._secid_info:
            return fallback
        key = self._secid_info.get(str(sec_id))
        if not key:
            return fallback
        return key[6] if key[6] else fallback

    def get_base_symbol(self, sec_id: str, fallback: str = "") -> str:
        if not self._secid_info:
            return fallback
        key = self._secid_info.get(str(sec_id))
        return key[2] if key else fallback

    def _ensure_loaded(self, refresh_master_scrip: bool = False) -> None:
        if self._eq_index is not None:
            return
        if not LOCAL_CSV.exists() or refresh_master_scrip:
            raw_df = self._download_segments()
            if raw_df is not None:
                self._save_and_index(raw_df)
        else:
            self._index_from_csv()

    def _download_segments(self) -> Optional[pd.DataFrame]:
        frames = []
        for segment in INSTRUMENT_SEGMENTS:
            try:
                resp = self.session.request(
                    'GET', INSTRUMENT_URL.format(segment=segment), timeout=5
                )
                resp.raise_for_status()

                df = pd.read_csv(
                    io.StringIO(resp.text),
                    usecols=lambda c: c in SCRIP_COLS,
                    low_memory=False,
                )

                mask = pd.Series(True, index=df.index)
                if 'EXCH_ID' in df.columns:
                    mask &= df['EXCH_ID'].isin(FILTER_EXCH)
                if 'INSTRUMENT' in df.columns:
                    mask &= df['INSTRUMENT'].isin(FILTER_SEG)
                if 'INSTRUMENT_TYPE' in df.columns:
                    mask &= df['INSTRUMENT_TYPE'].isin(FILTER_INST_TYPE)

                frames.append(df[mask])

            except Exception as exc:
                print(f'Failed to download {segment}: {exc}')

        return pd.concat(frames, ignore_index=True) if frames else None

    def _save_and_index(self, df: pd.DataFrame) -> None:
        today_str = _get_today_str()
        is_eq = df['INSTRUMENT'].isin(EQ_INDEX_INSTR)

        expiry_dates = df['SM_EXPIRY_DATE'].astype(str).str.split().str[0]

        df = df[is_eq | (~is_eq & (expiry_dates >= today_str))].copy()
        df.to_csv(LOCAL_CSV, index=False)

        eq_index, opt_index, expiry_index, secid_info = {}, {}, {}, {}
        self._fold_chunk(
            df, eq_index, opt_index, expiry_index, secid_info, today_str
        )
        self._commit_indexes(eq_index, opt_index, expiry_index, secid_info)

    def _index_from_csv(self) -> None:
        eq_index, opt_index, expiry_index, secid_info = {}, {}, {}, {}
        today_str = _get_today_str()
        try:
            with pd.read_csv(
                LOCAL_CSV, usecols=SCRIP_COLS, chunksize=50_000, low_memory=False
            ) as reader:
                for chunk in reader:
                    if 'INSTRUMENT_TYPE' in chunk.columns:
                        chunk = chunk[chunk['INSTRUMENT_TYPE'].isin(FILTER_INST_TYPE)]
                    self._fold_chunk(
                        chunk, eq_index, opt_index, expiry_index, secid_info, today_str
                    )
        except Exception as exc:
            print(f'Error loading scrip master: {exc}')
            return

        self._commit_indexes(eq_index, opt_index, expiry_index, secid_info)

    def _commit_indexes(
        self,
        eq_index: dict,
        opt_index: dict,
        expiry_index: dict,
        secid_info: dict,
    ) -> None:
        self._eq_index     = eq_index
        self._opt_index    = opt_index
        self._expiry_index = expiry_index
        self._secid_info   = secid_info

        base_to_name = {}
        for info in secid_info.values():
            if info[1] in EQ_INDEX_INSTR and info[6]:
                base_to_name[info[2]] = str(info[6]).strip().upper()

        search_entries = []
        for info in secid_info.values():
            exch_id, inst, underlying, exp, strike, opt_type, display_name = info
            if not display_name:
                continue
                
            display_str = str(display_name).strip()
            
            if inst in OPT_SEGMENTS:
                inst_type = 'OPT'
                inst_priority = 2
                if not display_str.upper().startswith(underlying.upper()):
                    display_str = f"{underlying} {display_str}"
            elif inst in FUT_SEGMENTS:
                inst_type = 'FUT'
                inst_priority = 1
                if not display_str.upper().startswith(underlying.upper()):
                    display_str = f"{underlying} FUT {display_str}"
            else:
                inst_type = 'EQ'
                inst_priority = 0
                if display_str.upper() == underlying.upper():
                    display_str = f"{underlying} - EQ"
                elif display_str.upper().startswith(underlying.upper()):
                    clean_name = display_str[len(underlying):].strip(' -()')
                    suffix = f" ({clean_name})" if clean_name else ""
                    display_str = f"{underlying} - EQ{suffix}"
                else:
                    display_str = f"{underlying} - EQ ({display_str})"

            exp_str = str(exp) if exp else ""
            upper_exp = exp_str.upper()
            
            # Map Month Tags dynamically using TOML config
            month_tags = ""
            for m_int in range(1, 13):
                m_str = f"-{m_int:02d}-"
                if m_str in upper_exp:
                    month_tags = self.cfg.gen_tags.get(m_int, "")
                    break
            
            if not month_tags:
                for k, v in self.cfg.month_aliases.items():
                    if f"-{k}" in upper_exp:
                        month_tags = v
                        break

            opt_safe_raw = str(opt_type).strip().upper() if opt_type else ""
            opt_tags = ""

            if opt_safe_raw.startswith("C"):
                opt_safe = "CE"
                opt_tags = "CE CALL CA C"
            elif opt_safe_raw.startswith("P"):
                opt_safe = "PE"
                opt_tags = "PE PUT PA P"
            else:
                opt_safe = ""

            strike_val = strike or 0.0
            strike_str = str(strike_val)
            strike_clean = (
                str(int(strike_val)) 
                if float(strike_val).is_integer() 
                else strike_str
            )

            comp_name = base_to_name.get(underlying, "")
            underlying_u = underlying.upper()
            display_u = display_str.upper()
            comp_name_u = comp_name.upper()

            all_text = (
                f"{underlying_u} {display_u} {comp_name_u} {exp_str} "
                f"{month_tags} {inst_type} {opt_safe} {opt_tags} "
                f"{strike_str} {strike_clean}"
            )

            raw_tokens = [
                t.strip().upper()
                for t in all_text.replace('-', ' ').split()
                if t.strip()
            ]

            tokens = set()
            for tok in raw_tokens:
                if tok in self.cfg.stop_words:
                    continue
                tokens.add(tok)
                if len(tok) >= 3:
                    tokens.add(tok[:3])

            joined = "".join(t for t in raw_tokens if t not in self.cfg.stop_words)
            tokens.add(joined)

            search_entries.append({
                'display':        display_str,
                'symbol':         underlying,
                'inst_type':      inst_type,
                'strike':         float(strike_val or 0),
                'opt_type':       opt_safe,
                'expiry':         exp_str,
                'exch':           exch_id,
                '_search_tokens': list(tokens),
                '_expiry_sort':   exp_str,
                '_month_tag':     month_tags.split()[0] if month_tags else "",
                '_sort_key':      (inst_priority, exp_str, strike_val, underlying)
            })

        search_entries.sort(
            key=lambda x: (
                x['_sort_key'][0],
                x['_expiry_sort'],
                x['strike'],
                x['symbol']
            )
        )
        self._search_data = search_entries
        
        self._display_to_data = {
            " ".join(x['display'].upper().split()): x 
            for x in search_entries
        }

    def _fold_chunk(
        self,
        chunk: pd.DataFrame,
        eq_index: dict,
        opt_index: dict,
        expiry_index: dict,
        secid_info: dict,
        today_str: str,
    ) -> None:
        if chunk.empty:
            return

        expiry_strs = chunk['SM_EXPIRY_DATE'].astype(str).str.split().str[0]
        eq_mask     = chunk['INSTRUMENT'].isin(EQ_INDEX_INSTR)

        for row in chunk[eq_mask].itertuples(index=False):
            str_sec_id = str(row.SECURITY_ID)
            eq_index[(row.EXCH_ID, row.UNDERLYING_SYMBOL)] = (str_sec_id, int(row.LOT_SIZE))

            display_raw = getattr(row, 'DISPLAY_NAME', None)
            disp_name = (
                row.UNDERLYING_SYMBOL 
                if pd.isna(display_raw) or not display_raw 
                else " ".join(str(display_raw).split())
            )

            secid_info[str_sec_id] = (
                row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL,
                None, None, None, disp_name
            )

        for row, exp in zip(chunk[~eq_mask].itertuples(index=False), expiry_strs[~eq_mask]):
            if exp < today_str:
                continue

            base_key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL)
            expiry_index.setdefault(base_key, set()).add(exp)

            strike, opt_type = None, None
            if row.INSTRUMENT in OPT_SEGMENTS:
                strike   = float(row.STRIKE_PRICE)
                opt_type = str(row.OPTION_TYPE).strip().upper()

            display_raw = getattr(row, 'DISPLAY_NAME', None)
            disp_name = (
                row.UNDERLYING_SYMBOL 
                if pd.isna(display_raw) or not display_raw 
                else " ".join(str(display_raw).split())
            )

            key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL, exp, strike, opt_type)
            if key not in opt_index:
                str_sec_id = str(row.SECURITY_ID)
                opt_index[key] = (str_sec_id, int(row.LOT_SIZE))
                secid_info[str_sec_id] = (
                    row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL,
                    exp, strike, opt_type, disp_name
                )

    def lookup(self, inst: Instrument) -> tuple[Optional[str], int]:
        self._ensure_loaded()

        if inst.seg in EQ_INDEX_INSTR:
            if self._eq_index:
                return self._eq_index.get((inst.exch, inst.symb), (None, 0))
            return None, 0

        valid_expiries = self._expiry_index.get((inst.exch, inst.seg, inst.symb))
        if not valid_expiries:
            return None, 0

        date_key = inst.expiry_date
        if not date_key or date_key not in valid_expiries:
            date_key = min(valid_expiries)
            inst.expiry_date = date_key

        key = (inst.exch, inst.seg, inst.symb, date_key, inst.strike, inst.opt_type)
        result = self._opt_index.get(key)
        return result if result else (None, 0)

    def lookup_with_fallback(self, inst: Instrument) -> tuple[Optional[str], int]:
        sec_id, lot_size = self.lookup(inst)
        if sec_id is not None:
            return sec_id, lot_size

        if inst.seg not in OPT_SEGMENTS:
            return None, 0

        fb_strike = _get_fallback_strike(inst.symb, inst.strike, inst.opt_type)
        if fb_strike:
            original_strike = inst.strike
            inst.strike     = fb_strike
            sec_id, lot_size = self.lookup(inst)

            if sec_id is not None:
                return sec_id, lot_size

            inst.strike = original_strike

        return None, 0