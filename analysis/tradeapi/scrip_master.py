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
    """Loads and encapsulates TOML search settings including scoring weights."""

    def __init__(self, path: Path):
        self.stop_words: set[str]       = set()
        self.call_words: set[str]       = set()
        self.put_words:  set[str]       = set()
        self.fut_words:  set[str]       = set()
        self.month_aliases: dict[str, str] = {}
        self.gen_tags:   dict[int, str] = {}

        # Scoring weights (all tuneable via TOML)
        self.exact_symbol_bonus:    float = 1000.0
        self.symbol_prefix_bonus:   float = 400.0
        self.company_idf_factor:    float = 100.0
        self.all_tokens_coherence:  float = 200.0
        self.expiry_proximity_scale: float = 0.0
        self.eq_base_bonus:         float = 50.0

        self._load(path)

    def _load(self, path: Path) -> None:
        if not path.exists():
            print(f"[!] Warning: {path} not found. Using defaults.")
            return

        with open(path, 'rb') as f:
            data = tomllib.load(f).get('search', {})

        self.stop_words = set(data.get('stop_words', {}).get('words', []))
        aliases = data.get('aliases', {})
        self.call_words = set(aliases.get('call', {}).get('words', []))
        self.put_words  = set(aliases.get('put',  {}).get('words', []))
        self.fut_words  = set(aliases.get('fut',  {}).get('words', []))
        self.month_aliases = {k.upper(): v.upper()
                              for k, v in data.get('month_aliases', {}).items()}

        raw_gen_tags = data.get('month_generation_tags', {})
        self.gen_tags = {int(k): str(v).upper() for k, v in raw_gen_tags.items()}

        sc = data.get('scoring', {})
        self.exact_symbol_bonus    = float(sc.get('exact_symbol_bonus',    self.exact_symbol_bonus))
        self.symbol_prefix_bonus   = float(sc.get('symbol_prefix_bonus',   self.symbol_prefix_bonus))
        self.company_idf_factor    = float(sc.get('company_idf_factor',    self.company_idf_factor))
        self.all_tokens_coherence  = float(sc.get('all_tokens_coherence',  self.all_tokens_coherence))
        self.expiry_proximity_scale = float(sc.get('expiry_proximity_scale', self.expiry_proximity_scale))
        self.eq_base_bonus         = float(sc.get('eq_base_bonus',         self.eq_base_bonus))


# ───────────────────────────────────────
# ScripMaster Engine
# ───────────────────────────────────────
class ScripMaster:
    """
    Manages instrument data, lookup indexes, and the search engine.

    Search engine design
    ────────────────────
    Index time  (_commit_indexes)
      • _name_inv_index : token → sorted list[int] of entry indices
          Built from {underlying} + {company_name} tokens (minus stop-words),
          plus 3- and 4-char prefix tokens so partial typing still hits.
      • _idf             : token → float IDF weight
          IDF = log(N / df) + 1.  Rare tokens (unique tickers) score much
          higher than common ones ("BANK", "LTD").

    Query time  (search_symbols)
      1. Classify tokens → name_tokens | month | strike | opt_type | want_fut
      2. AND-intersect posting lists for all name_tokens  → small candidate set.
         Falls back to OR-union if AND returns nothing (handles typos / partial).
      3. Hard-filter candidates on structured fields (month / strike / opt_type).
      4. IDF-weighted score per candidate; sort score DESC, expiry ASC.
    """

    def __init__(
        self,
        session_obj: requests.Session,
        refresh_master_scrip: bool = False,
    ):
        self._eq_index:     Optional[dict] = None
        self._opt_index:    Optional[dict] = None
        self._expiry_index: Optional[dict] = None
        self._secid_info:   Optional[dict] = None
        self._search_data:  list[dict]     = []
        self._display_to_data: dict[str, dict] = {}

        # ── Search engine indexes ─────────────────────────────────────
        # token → sorted list of entry-indices (built from name/company tokens)
        self._name_inv_index: dict[str, list[int]] = {}
        # token → IDF weight
        self._idf:            dict[str, float]     = {}

        self.session = session_obj
        self.cfg = SearchConfig(SEARCH_CONFIG_PATH)

        self._ensure_loaded(refresh_master_scrip)

    def search_symbols(self, query: str, limit: int = 30) -> list[dict]:
        """
        Return up to *limit* instrument suggestions for *query*.

        Token classification
        ────────────────────
        month      → hard filter on _month_tag    (e.g. "JUN", "JUNE", "06")
        strike     → hard filter on _strike_int   (e.g. "960", "940.0")
        opt_type   → hard filter on opt_type      (CE aliases → "CE")
        want_fut   → hard filter on inst_type     ("FUT")
        name_tokens→ everything else; scored via IDF-weighted inverted index
        """
        self._ensure_loaded()
        if not self._search_data:
            return []

        query = query.strip().upper()
        if len(query) < 2:
            return []

        parts = [p for p in query.replace('-', ' ').split() if p]

        # ── Token classification ──────────────────────────────────────────
        name_tokens: list[str] = []
        month_token: Optional[str] = None
        strike_int:  Optional[int] = None
        opt_type:    Optional[str] = None   # 'CE' or 'PE'
        want_fut:    bool = False

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
            # Numeric → strike price
            try:
                v = float(tok)
                if v > 0:
                    strike_int = int(v) if v.is_integer() else int(round(v))
                continue
            except ValueError:
                pass
            name_tokens.append(tok)

        # ── Candidate retrieval via AND-intersection of posting lists ─────
        # We always try AND first; fall back to OR when AND is empty so that
        # partial / slightly wrong queries still return something.
        def _posting(tok: str) -> set[int]:
            """Return entry-index set for tok, or empty set."""
            return set(self._name_inv_index.get(tok, []))

        def _intersect(tokens: list[str]) -> tuple[set[int], bool]:
            """
            Attempt AND intersection.  Returns (index_set, used_and).
            Falls back to OR union when AND is empty.
            """
            if not tokens:
                return set(range(len(self._search_data))), False

            # Sort by posting-list length (smallest first) → faster intersection
            sorted_toks = sorted(tokens, key=lambda t: len(self._name_inv_index.get(t, [])))
            result = _posting(sorted_toks[0])
            for t in sorted_toks[1:]:
                result &= _posting(t)
                if not result:
                    break

            if result:
                return result, True   # AND succeeded

            # AND was empty → OR fallback
            union: set[int] = set()
            for t in tokens:
                union |= _posting(t)
            return union, False       # OR fallback

        candidate_idx, used_and = _intersect(name_tokens)
        candidates = [self._search_data[i] for i in candidate_idx]

        # ── Hard structural filters ───────────────────────────────────────
        if opt_type:
            candidates = [c for c in candidates if c['opt_type'] == opt_type]
        elif want_fut:
            candidates = [c for c in candidates if c['inst_type'] == 'FUT']

        if month_token:
            candidates = [c for c in candidates if c['_month_tag'] == month_token]

        if strike_int is not None:
            candidates = [c for c in candidates if c['_strike_int'] == strike_int]

        if not candidates:
            return []

        # ── IDF-weighted scoring ──────────────────────────────────────────
        cfg = self.cfg

        def _score(entry: dict) -> float:
            sym  = entry['symbol']
            ntok = entry['_name_tokens']   # frozenset[str]
            s    = 0.0
            matched = 0

            for tok in name_tokens:
                if tok not in ntok:
                    continue
                matched += 1
                idf = self._idf.get(tok, 1.0)

                if sym == tok:
                    # Exact ticker match — completely dominates
                    s += cfg.exact_symbol_bonus + idf * cfg.company_idf_factor
                elif sym.startswith(tok) or tok.startswith(sym):
                    # Prefix overlap (e.g. "SBI" matches "SBIN")
                    s += cfg.symbol_prefix_bonus + idf * cfg.company_idf_factor
                else:
                    # Company-name match; weighted by how rare the word is
                    s += idf * cfg.company_idf_factor

            # Coherence bonus: reward when every query token actually matched
            if used_and and matched == len(name_tokens) and name_tokens:
                s += cfg.all_tokens_coherence

            # Expiry proximity (nearer expiry = slightly higher score)
            exp = entry['_expiry_sort']
            if exp:
                try:
                    s += cfg.expiry_proximity_scale * (99_999_999 - int(exp.replace('-', '')))
                except Exception:
                    pass
            elif not (month_token or strike_int or opt_type or want_fut):
                # Generic query with no structured filter → float equities up
                s += cfg.eq_base_bonus

            return s

        scored = [(  _score(e), e) for e in candidates]
        scored.sort(key=lambda x: (-x[0], x[1]['_expiry_sort'], x[1]['strike']))

        # ── Strip internal fields before returning ────────────────────────
        final = []
        for _, item in scored[:limit]:
            obj = {k: v for k, v in item.items() if not k.startswith('_')}
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

        # symbol → company display name (equities/indices only)
        base_to_name: dict[str, str] = {}
        for info in secid_info.values():
            if info[1] in EQ_INDEX_INSTR and info[6]:
                base_to_name[info[2]] = str(info[6]).strip().upper()

        stop = self.cfg.stop_words

        search_entries: list[dict] = []

        for info in secid_info.values():
            exch_id, inst, underlying, exp, strike, opt_type, display_name = info
            if not display_name:
                continue

            display_str = str(display_name).strip()

            if inst in OPT_SEGMENTS:
                inst_type     = 'OPT'
                inst_priority = 2
                if not display_str.upper().startswith(underlying.upper()):
                    display_str = f"{underlying} {display_str}"
            elif inst in FUT_SEGMENTS:
                inst_type     = 'FUT'
                inst_priority = 1
                if not display_str.upper().startswith(underlying.upper()):
                    display_str = f"{underlying} FUT {display_str}"
            else:
                inst_type     = 'EQ'
                inst_priority = 0
                if display_str.upper() == underlying.upper():
                    display_str = f"{underlying} - EQ"
                elif display_str.upper().startswith(underlying.upper()):
                    clean = display_str[len(underlying):].strip(' -()')
                    display_str = f"{underlying} - EQ ({clean})" if clean else f"{underlying} - EQ"
                else:
                    display_str = f"{underlying} - EQ ({display_str})"

            # ── Structured fields (used for hard filtering) ──────────────
            exp_str = str(exp) if exp else ""
            month_tag = ""
            for m_int in range(1, 13):
                if f"-{m_int:02d}-" in exp_str:
                    tags = self.cfg.gen_tags.get(m_int, "")
                    month_tag = tags.split()[0] if tags else ""
                    break

            opt_safe_raw = str(opt_type).strip().upper() if opt_type else ""
            if opt_safe_raw.startswith("C"):
                opt_safe = "CE"
            elif opt_safe_raw.startswith("P"):
                opt_safe = "PE"
            else:
                opt_safe = ""

            strike_val = float(strike or 0.0)
            strike_int = int(strike_val) if strike_val and float(strike_val).is_integer() else None

            # ── Name tokens (IDF index) ───────────────────────────────────
            # Built ONLY from underlying symbol + company name, minus stop-words.
            # Keeps structured noise (month/strike/CE/PE) OUT of the IDF space
            # so that "BANK" in a company name can't accidentally match "BAN"
            # from a strike like "24800".
            comp_name = base_to_name.get(underlying, "")
            name_src  = f"{underlying} {comp_name}"
            raw_name  = [
                t.upper() for t in name_src.replace('-', ' ').split()
                if t.strip() and t.upper() not in stop
            ]

            name_token_set: set[str] = set()
            for tok in raw_name:
                name_token_set.add(tok)
                # Prefix tokens so partial typing hits (e.g. "SBI" → SBIN,
                # "STA" → STATE, "STAT" → STATE)
                for plen in range(3, min(len(tok), 5)):   # 3-char and 4-char
                    name_token_set.add(tok[:plen])

            search_entries.append({
                'display':       display_str,
                'symbol':        underlying,
                'inst_type':     inst_type,
                'strike':        strike_val,
                'opt_type':      opt_safe,
                'expiry':        exp_str,
                'exch':          exch_id,
                # internal
                '_name_tokens':  frozenset(name_token_set),
                '_month_tag':    month_tag,
                '_strike_int':   strike_int,
                '_expiry_sort':  exp_str,
                '_sort_key':     (inst_priority, exp_str, strike_val, underlying),
            })

        # Default sort: EQ first, then nearest expiry, then strike
        search_entries.sort(key=lambda x: x['_sort_key'])
        self._search_data = search_entries

        self._display_to_data = {
            " ".join(x['display'].upper().split()): x
            for x in search_entries
        }

        # ── Build inverted index + IDF ────────────────────────────────────
        N = len(search_entries)

        # doc_freq[tok] = number of entries containing tok
        # posting[tok]  = sorted list of entry indices
        doc_freq: dict[str, int]       = defaultdict(int)
        posting:  dict[str, list[int]] = defaultdict(list)

        for i, entry in enumerate(search_entries):
            for tok in entry['_name_tokens']:
                posting[tok].append(i)   # already unique per entry (frozenset)
                doc_freq[tok] += 1

        # Freeze posting lists as plain lists (already built in order 0..N-1)
        self._name_inv_index = dict(posting)

        # IDF = log(N / df) + 1  (Lucene-style smoothed IDF)
        # Rare tokens (unique tickers) → high IDF.  "BANK" (common) → low IDF.
        self._idf = {
            tok: math.log(N / df) + 1.0
            for tok, df in doc_freq.items()
            if df > 0
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