"""Instrument master-data loading and lookup."""

from __future__ import annotations

import gc
import io
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

import pandas as pd
import tomllib
from requests.exceptions import RequestException

from tradeapi.scrip_search import SearchEngine
from utils.data.paths import OUT_DIR
from utils.utility import INDIA_TZ, LOGGER

if TYPE_CHECKING:
    import requests

BASE_DIR = Path(__file__).parent
LOCAL_CSV = Path(OUT_DIR) / "scrip_master.csv"
CONFIG_PATH = BASE_DIR / "scrip_master.toml"

REQUEST_TIMEOUT_SECONDS = 3

# Keys excluded from public search-result dicts.
# expiry_sort and sort_key no longer exist as slots, so they're removed here too.
EXCLUDED_KEYS = {"month_tag", "name_tokens", "strike_int"}

# Sort priority for instrument type: lower = closer to top of results.
_INST_PRIORITY: dict[str, int] = {"EQ": 0, "FUT": 1, "OPT": 2}


class ScripEntry:
    """Highly compressed, __slots__-based replacement for row dictionaries.

    Memory changes vs previous version
    ───────────────────────────────────
    • Removed ``expiry_sort`` slot — was a duplicate of ``expiry`` (same
      sys.intern'd string).  Saves 8 B/entry slot pointer * 300k ≈ 2.4 MB.
    • Removed ``sort_key`` slot — was a 72-B 4-tuple built just for sorting.
      Saves 72 B * 300k ≈ 21.6 MB.  Sorting is now done with an inline key
      function in ``_commit_indexes`` that reads existing slots directly.
    • ``name_tokens`` is now a ``tuple[str, ...]`` (set by SearchEngine).
      Previously a ``frozenset`` (728 B each).  Tuple costs ≈ 88 B per entry.
      Saves ~640 B * 300k ≈ 192 MB.  (O(n) tuple membership for n ≤ 10
      tokens is indistinguishable from O(1) frozenset lookup in practice.)
    """

    # These use zero instance memory but make Mypy happy!
    display: str
    exch: str
    expiry: str
    inst_type: str
    month_tag: str
    name_tokens: tuple[str, ...]
    opt_type: str
    strike: float
    strike_int: int | None
    symbol: str

    __slots__ = (
        "display",
        "exch",
        "expiry",
        "inst_type",
        "month_tag",
        "name_tokens",
        "opt_type",
        "strike",
        "strike_int",
        "symbol",
    )

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def items(self) -> Iterator[tuple[str, Any]]:
        return ((k, getattr(self, k)) for k in self.__slots__ if hasattr(self, k))


def _get_today_str() -> str:
    return datetime.now(tz=INDIA_TZ).strftime("%Y-%m-%d")


class ScripConfig:
    """Loads and encapsulates TOML Master Scrip & Search settings."""

    def __init__(self, path: Path):
        self.stop_words: set[str] = set()
        self.call_words: set[str] = set()
        self.put_words: set[str] = set()
        self.fut_words: set[str] = set()
        self.symbol_aliases: dict[str, str] = {}
        self.month_aliases: dict[str, str] = {}
        self.month_tags: dict[int, str] = {}

        self.exact_symbol_bonus: float = 1000.0
        self.symbol_prefix_bonus: float = 400.0
        self.company_idf_factor: float = 100.0
        self.all_tokens_coherence: float = 200.0
        self.expiry_proximity_scale: float = 0.0
        self.eq_base_bonus: float = 50.0

        self.instrument_segments: list[str] = []
        self.instrument_url: str = ""
        self.filter_exch: frozenset[str] = frozenset()
        self.filter_seg: frozenset[str] = frozenset()
        self.filter_inst_type: frozenset[str] = frozenset()
        self.eq_index_instr: frozenset[str] = frozenset()

        self.scrip_dtypes: dict[str, str] = {}
        self.scrip_cols: list[str] = []

        self._load(path)

    def _load(self, path: Path) -> None:
        if not path.exists():
            LOGGER.warning("Scrip config not found at %s. Using defaults.", path)
            return

        try:
            with path.open("rb") as config_file:
                data = tomllib.load(config_file)
        except (OSError, tomllib.TOMLDecodeError):
            LOGGER.exception("Failed parsing config at %s", path)
            return

        master_cfg = data.get("master", {})
        self.instrument_segments = master_cfg.get("instrument_segments", [])
        self.instrument_url = master_cfg.get("instrument_url", "")
        self.filter_exch = frozenset(master_cfg.get("filter_exch", []))
        self.filter_seg = frozenset(master_cfg.get("filter_seg", []))
        self.filter_inst_type = frozenset(master_cfg.get("filter_inst_type", []))

        self.scrip_dtypes = master_cfg.get("scrip_dtypes", {})
        self.scrip_cols = list(self.scrip_dtypes.keys())

        self.eq_index_instr = frozenset(master_cfg.get("eq_index_instr", []))

        search_cfg = data.get("search", {})
        stop_words_cfg = search_cfg.get("stop_words", {})
        self.stop_words = set(stop_words_cfg.get("words", []))

        aliases = search_cfg.get("aliases", {})
        self.call_words = set(aliases.get("call", {}).get("words", []))
        self.put_words = set(aliases.get("put", {}).get("words", []))
        self.fut_words = set(aliases.get("fut", {}).get("words", []))

        for k, v in aliases.get("symbol", {}).items():
            self.symbol_aliases[str(k).upper()] = str(v).upper()

        self.month_aliases, self.month_tags = self._load_month_aliases(
            aliases.get("month", {}),
        )

        scoring_cfg = search_cfg.get("scoring", {})
        self.exact_symbol_bonus = float(
            scoring_cfg.get("exact_symbol_bonus", self.exact_symbol_bonus),
        )
        self.symbol_prefix_bonus = float(
            scoring_cfg.get("symbol_prefix_bonus", self.symbol_prefix_bonus),
        )
        self.company_idf_factor = float(
            scoring_cfg.get("company_idf_factor", self.company_idf_factor),
        )
        self.all_tokens_coherence = float(
            scoring_cfg.get("all_tokens_coherence", self.all_tokens_coherence),
        )
        self.expiry_proximity_scale = float(
            scoring_cfg.get("expiry_proximity_scale", self.expiry_proximity_scale),
        )
        self.eq_base_bonus = float(scoring_cfg.get("eq_base_bonus", self.eq_base_bonus))

    def _load_month_aliases(
        self, raw_aliases: dict
    ) -> tuple[dict[str, str], dict[int, str]]:
        month_order = [
            "JAN",
            "FEB",
            "MAR",
            "APR",
            "MAY",
            "JUN",
            "JUL",
            "AUG",
            "SEP",
            "OCT",
            "NOV",
            "DEC",
        ]
        aliases: dict[str, str] = {}
        tags_by_month: dict[int, str] = {}

        for month_num, tag in enumerate(month_order, start=1):
            section = raw_aliases.get(tag.lower(), {})
            words = section.get("words", [])
            if words:
                tags_by_month[month_num] = sys.intern(tag)
                for word in words:
                    aliases[str(word).upper()] = sys.intern(tag)

        return aliases, tags_by_month


class ScripMaster:
    """Manages instrument data ingestion and delegates lookup to SearchEngine."""

    def __init__(
        self, session_obj: requests.Session, *, refresh_master_scrip: bool = False
    ):
        self._eq_index: dict | None = None
        self._opt_index: dict | None = None
        self._expiry_index: dict | None = None
        self._secid_info: dict | None = None

        self.session = session_obj
        self.cfg = ScripConfig(CONFIG_PATH)
        self.search_engine = SearchEngine(self.cfg)

        self._ensure_loaded(refresh_master_scrip=refresh_master_scrip)

    def search_symbols(self, query: str, limit: int = 30) -> list[dict]:
        self._ensure_loaded()
        return self.search_engine.search(query, limit)

    def get_data_by_display_name(self, display_name: str) -> dict | None:
        key = " ".join(display_name.strip().upper().split())
        for entry in self.search_engine.entries:
            if " ".join(entry.display.upper().split()) == key:
                return {k: v for k, v in entry.items() if k not in EXCLUDED_KEYS}
        return None

    def get_symbol_name(self, sec_id: str, fallback: str = "") -> str:
        sec_info = self._secid_info
        if not sec_info:
            return fallback
        info = sec_info.get(str(sec_id))
        return info[6] if info else fallback

    def get_base_symbol(self, sec_id: str, fallback: str = "") -> str:
        sec_info = self._secid_info
        if not sec_info:
            return fallback
        info = sec_info.get(str(sec_id))
        return info[2] if info else fallback

    def get_instrument_details(self, sec_id: str) -> dict:
        sec_info = self._secid_info
        if not sec_info:
            return {}
        info = sec_info.get(str(sec_id))
        if not info:
            return {}
        return {
            "exch": info[0],
            "inst_type": info[1],
            "symbol": info[2],
            "expiry": info[3],
            "strike": info[4],
            "opt_type": info[5],
            "disp_name": info[6],
        }

    def lookup(
        self,
        exch: str,
        seg: str,
        symb: str,
        expiry_date: str,
        strike: float | None,
        opt_type: str | None,
    ) -> tuple[str | None, int]:
        self._ensure_loaded()

        if seg in self.cfg.eq_index_instr:
            if self._eq_index:
                return self._eq_index.get((exch, symb), (None, 0))
            return None, 0

        if not self._expiry_index or not self._opt_index:
            return None, 0

        valid_expiries = self._expiry_index.get((exch, seg, symb))
        if not valid_expiries:
            return None, 0

        date_key = expiry_date
        if not date_key or date_key not in valid_expiries:
            date_key = min(valid_expiries)

        key = (exch, seg, symb, date_key, strike, opt_type)
        return self._opt_index.get(key) or (None, 0)

    def _ensure_loaded(self, *, refresh_master_scrip: bool = False) -> None:
        if self._eq_index is not None:
            return
        if not LOCAL_CSV.exists() or refresh_master_scrip:
            raw_df = self._download_segments()
            if raw_df is not None and not raw_df.empty:
                self._save_and_index(raw_df)
            elif LOCAL_CSV.exists():
                LOGGER.warning("Download failed. Falling back to cached CSV.")
                self._index_from_csv()
            else:
                LOGGER.error("Scrip master unavailable: no download/cache.")
        else:
            self._index_from_csv()

    def _download_one_segment(self, segment: str) -> pd.DataFrame | None:
        try:
            url = self.cfg.instrument_url.format(segment=segment)
            resp = self.session.request("GET", url, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()

            df = pd.read_csv(
                io.StringIO(resp.text),
                usecols=lambda c: c in self.cfg.scrip_cols,
                dtype=self.cfg.scrip_dtypes,
                low_memory=False,
            )
            mask = pd.Series(data=True, index=df.index)
            if "EXCH_ID" in df.columns:
                mask &= df["EXCH_ID"].isin(self.cfg.filter_exch)
            if "INSTRUMENT" in df.columns:
                mask &= df["INSTRUMENT"].isin(self.cfg.filter_seg)
            if "INSTRUMENT_TYPE" in df.columns:
                mask &= df["INSTRUMENT_TYPE"].isin(self.cfg.filter_inst_type)
            return df[mask]
        except (RequestException, ValueError, pd.errors.ParserError):
            LOGGER.exception("Failed to download scrip segment %s", segment)
            return None

    def _download_segments(self) -> pd.DataFrame | None:
        if not self.cfg.instrument_url or not self.cfg.instrument_segments:
            LOGGER.error("Instrument URL or segments missing in config.")
            return None

        frames = [
            f
            for seg in self.cfg.instrument_segments
            if (f := self._download_one_segment(seg)) is not None
        ]
        return pd.concat(frames, ignore_index=True) if frames else None

    def _save_and_index(self, df: pd.DataFrame) -> None:
        today_str = _get_today_str()
        is_eq = df["INSTRUMENT"].isin(self.cfg.eq_index_instr)

        expiry_dates = df["SM_EXPIRY_DATE"].astype(str).str.split().str[0]
        df = df[is_eq | (~is_eq & (expiry_dates >= today_str))].copy()

        LOCAL_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(LOCAL_CSV, index=False)

        eq_idx: dict[Any, Any] = {}
        opt_idx: dict[Any, Any] = {}
        expiry_idx: dict[Any, Any] = {}
        sec_info: dict[Any, Any] = {}

        self._fold_chunk(df, eq_idx, opt_idx, expiry_idx, sec_info, today_str)

        # Free the DataFrame before building search index — reduces peak RAM.
        del df
        gc.collect()

        self._commit_indexes(eq_idx, opt_idx, expiry_idx, sec_info)

    def _index_from_csv(self) -> None:
        eq_idx: dict[Any, Any] = {}
        opt_idx: dict[Any, Any] = {}
        expiry_idx: dict[Any, Any] = {}
        sec_info: dict[Any, Any] = {}

        today_str = _get_today_str()
        try:
            with pd.read_csv(
                LOCAL_CSV,
                usecols=lambda c: c in self.cfg.scrip_cols,
                dtype=self.cfg.scrip_dtypes,
                chunksize=50_000,
                low_memory=False,
            ) as reader:
                for raw_chunk in reader:
                    chunk = raw_chunk
                    if "INSTRUMENT_TYPE" in chunk.columns:
                        chunk = chunk[
                            chunk["INSTRUMENT_TYPE"].isin(self.cfg.filter_inst_type)
                        ]
                    self._fold_chunk(
                        chunk, eq_idx, opt_idx, expiry_idx, sec_info, today_str
                    )
                    # Release each chunk promptly so peak RAM stays bounded.
                    del raw_chunk, chunk
                    gc.collect()
        except (OSError, ValueError, pd.errors.ParserError):
            LOGGER.exception("Error loading scrip master cache")
            return

        self._commit_indexes(eq_idx, opt_idx, expiry_idx, sec_info)

    def _commit_indexes(
        self, eq_index: dict, opt_index: dict, expiry_index: dict, secid_info: dict
    ) -> None:
        self._eq_index = eq_index
        self._opt_index = opt_index
        self._expiry_index = expiry_index
        self._secid_info = secid_info

        base_to_name: dict[str, str] = {}
        for info in secid_info.values():
            if info[1] in self.cfg.eq_index_instr and info[6]:
                base_to_name[info[2]] = str(info[6]).strip().upper()

        search_entries = self._build_search_entries(secid_info)

        # ── MEMORY CHANGE: sort_key slot removed from ScripEntry.
        #    We compute an equivalent key inline from existing slots:
        #      inst_priority  — 0 EQ / 1 FUT / 2 OPT  (same ordering as before)
        #      expiry         — ISO date string, lexicographically sortable
        #      strike         — float
        #      symbol         — underlying ticker
        search_entries.sort(
            key=lambda e: (
                _INST_PRIORITY.get(e.inst_type, 2),
                e.expiry or "",
                e.strike or 0.0,
                e.symbol,
            )
        )

        self.search_engine.build_index(search_entries, base_to_name)

        gc.collect()

    def _make_display_str(
        self, inst: str, underlying: str, display_name: str
    ) -> tuple[str, str, int]:
        display_str = str(display_name).strip()
        if inst.startswith("OPT"):
            inst_type = "OPT"
            inst_priority = 2
            if not display_str.upper().startswith(underlying.upper()):
                display_str = f"{underlying} {display_str}"
        elif inst.startswith("FUT"):
            inst_type = "FUT"
            inst_priority = 1
            if not display_str.upper().startswith(underlying.upper()):
                display_str = f"{underlying} FUT {display_str}"
        else:
            inst_type = "EQ"
            inst_priority = 0
            if display_str.upper() == underlying.upper():
                display_str = f"{underlying} - EQ"
            elif display_str.upper().startswith(underlying.upper()):
                clean = display_str[len(underlying) :].strip(" -()")
                suffix = f" ({clean})" if clean else ""
                display_str = f"{underlying} - EQ{suffix}"
            else:
                display_str = f"{underlying} - EQ ({display_str})"
        return display_str, inst_type, inst_priority

    def _normalise_opt_type(self, opt_type: str | None) -> str:
        opt_safe_raw = str(opt_type).strip().upper() if opt_type else ""
        if opt_safe_raw.startswith("C"):
            return "CE"
        if opt_safe_raw.startswith("P"):
            return "PE"
        return ""

    def _build_search_entries(self, secid_info: dict) -> list[ScripEntry]:
        entries: list[ScripEntry] = []
        for info in secid_info.values():
            exch_id, inst, underlying, exp, strike, opt_type, display_name = info
            if not display_name:
                continue

            display_str, inst_type, _inst_priority = self._make_display_str(
                inst, underlying, display_name
            )

            exp_str = str(exp) if exp else ""
            month_tag = ""
            for m_int in range(1, 13):
                if f"-{m_int:02d}-" in exp_str:
                    month_tag = self.cfg.month_tags.get(m_int, "")
                    break

            opt_safe = self._normalise_opt_type(opt_type)
            strike_val = float(strike or 0.0)
            strike_int = (
                int(strike_val)
                if strike_val and float(strike_val).is_integer()
                else None
            )

            # ── MEMORY CHANGE: expiry_sort and sort_key slots removed.
            #    • expiry_sort was identical to expiry — use entry.expiry instead.
            #    • sort_key 4-tuple (72 B each) replaced by inline lambda in
            #      _commit_indexes that reads existing slots directly.
            entries.append(
                ScripEntry(
                    display=display_str,
                    symbol=sys.intern(underlying),
                    inst_type=sys.intern(inst_type),
                    strike=strike_val,
                    opt_type=sys.intern(opt_safe),
                    expiry=sys.intern(exp_str) if exp_str else "",
                    exch=sys.intern(exch_id),
                    month_tag=sys.intern(month_tag) if month_tag else "",
                    strike_int=strike_int,
                )
            )
        return entries

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

        expiry_strs = chunk["SM_EXPIRY_DATE"].astype(str).str.split().str[0]
        eq_mask = chunk["INSTRUMENT"].isin(self.cfg.eq_index_instr)

        for row in chunk[eq_mask].itertuples(index=False):
            str_sec_id = str(row.SECURITY_ID)

            if str_sec_id in secid_info:
                existing_inst = secid_info[str_sec_id][1]
                if str(row.INSTRUMENT).upper() == "INDEX" and existing_inst != "INDEX":
                    continue

            exch_interned = sys.intern(str(row.EXCH_ID))
            inst_interned = sys.intern(str(row.INSTRUMENT))
            sym_interned = sys.intern(str(row.UNDERLYING_SYMBOL))

            eq_index[(exch_interned, sym_interned)] = (str_sec_id, int(row.LOT_SIZE))
            display_raw = getattr(row, "DISPLAY_NAME", None)
            disp_name = (
                row.UNDERLYING_SYMBOL
                if pd.isna(display_raw) or not display_raw
                else " ".join(str(display_raw).split())
            )

            secid_info[str_sec_id] = (
                exch_interned,
                inst_interned,
                sym_interned,
                None,
                None,
                None,
                disp_name,
            )

        for row, exp in zip(
            chunk[~eq_mask].itertuples(index=False), expiry_strs[~eq_mask], strict=True
        ):
            if exp < today_str:
                continue

            exch_interned = sys.intern(str(row.EXCH_ID))
            inst_interned = sys.intern(str(row.INSTRUMENT))
            sym_interned = sys.intern(str(row.UNDERLYING_SYMBOL))
            exp_interned = sys.intern(str(exp))

            base_key = (exch_interned, inst_interned, sym_interned)
            expiry_index.setdefault(base_key, set()).add(exp_interned)

            strike, opt_type = None, None
            if inst_interned.startswith("OPT"):
                strike = float(row.STRIKE_PRICE)
                opt_type = sys.intern(str(row.OPTION_TYPE).strip().upper())

            display_raw = getattr(row, "DISPLAY_NAME", None)
            disp_name = (
                row.UNDERLYING_SYMBOL
                if pd.isna(display_raw) or not display_raw
                else " ".join(str(display_raw).split())
            )

            key = (
                exch_interned,
                inst_interned,
                sym_interned,
                exp_interned,
                strike,
                opt_type,
            )
            if key not in opt_index:
                str_sec_id = str(row.SECURITY_ID)
                opt_index[key] = (str_sec_id, int(row.LOT_SIZE))
                secid_info[str_sec_id] = (
                    exch_interned,
                    inst_interned,
                    sym_interned,
                    exp_interned,
                    strike,
                    opt_type,
                    disp_name,
                )
