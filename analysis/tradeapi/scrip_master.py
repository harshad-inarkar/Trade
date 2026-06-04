"""Instrument master-data loading, lookup, and search helpers."""

import contextlib
import io
import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import requests
import tomllib
from requests.exceptions import RequestException

from utils.data.paths import OUT_DIR

INDIA_TZ = pytz.timezone("Asia/Kolkata")
LOGGER = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
LOCAL_CSV = Path(OUT_DIR) / "scrip_master.csv"
CONFIG_PATH = BASE_DIR / "scrip_master.toml"

REQUEST_TIMEOUT_SECONDS = 10
_MIN_QUERY_LEN = 2


@dataclass
class SearchQueryContext:
    name_tokens: list[str]
    month_token: str | None
    strike_int: int | None
    opt_type: str | None
    want_fut: bool
    used_and: bool = False


def _get_today_str() -> str:
    return datetime.now(tz=INDIA_TZ).strftime("%Y-%m-%d")


class ScripConfig:
    """Loads and encapsulates TOML Master Scrip & Search settings."""

    def __init__(self, path: Path):
        self.stop_words: set[str] = set()
        self.call_words: set[str] = set()
        self.put_words: set[str] = set()
        self.fut_words: set[str] = set()
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
        self.scrip_cols: list[str] = []
        self.eq_index_instr: frozenset[str] = frozenset()

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
        self.scrip_cols = master_cfg.get("scrip_cols", [])
        self.eq_index_instr = frozenset(master_cfg.get("eq_index_instr", []))

        search_cfg = data.get("search", {})
        stop_words_cfg = search_cfg.get("stop_words", {})
        self.stop_words = set(stop_words_cfg.get("words", []))

        aliases = search_cfg.get("aliases", {})
        self.call_words = set(aliases.get("call", {}).get("words", []))
        self.put_words = set(aliases.get("put", {}).get("words", []))
        self.fut_words = set(aliases.get("fut", {}).get("words", []))

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

    @staticmethod
    def _load_month_aliases(raw_aliases: dict) -> tuple[dict[str, str], dict[int, str]]:
        month_order = (
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
        )
        aliases: dict[str, str] = {}
        tags_by_month: dict[int, str] = {}

        for month_num, tag in enumerate(month_order, start=1):
            section = raw_aliases.get(tag.lower(), raw_aliases.get(tag, {}))
            words = section.get("words", []) if isinstance(section, dict) else []
            if words:
                tags_by_month[month_num] = tag
                for word in words:
                    aliases[str(word).upper()] = tag

        return aliases, tags_by_month


class ScripMaster:
    """Manages instrument data, lookup indexes, and the search engine."""

    def __init__(
        self,
        session_obj: requests.Session,
        *,
        refresh_master_scrip: bool = False,
    ):
        self._eq_index: dict | None = None
        self._opt_index: dict | None = None
        self._expiry_index: dict | None = None
        self._secid_info: dict | None = None

        self._search_data: list[dict] = []
        self._display_to_data: dict[str, dict] = {}
        self._name_inv_index: dict[str, frozenset[int]] = {}
        self._all_search_indices: frozenset[int] = frozenset()
        self._idf: dict[str, float] = {}

        self.session = session_obj
        self.cfg = ScripConfig(CONFIG_PATH)

        self._ensure_loaded(refresh_master_scrip=refresh_master_scrip)

    # ── Search Engine Methods ───────────────────────────────────────────────
    def search_symbols(self, query: str, limit: int = 30) -> list[dict]:
        self._ensure_loaded()
        if not self._search_data or len(query) < _MIN_QUERY_LEN:
            return []

        ctx = self._parse_query(query)
        if not (
            ctx.name_tokens
            or ctx.month_token
            or ctx.strike_int
            or ctx.opt_type
            or ctx.want_fut
        ):
            return []

        candidates = self._get_candidates(ctx)
        if not candidates:
            return []

        scored = [(self._score_candidate(c, ctx), c) for c in candidates]
        scored.sort(key=lambda x: (-x[0], x[1]["_expiry_sort"], x[1]["strike"]))

        return self._format_results(scored, limit)

    def _parse_query(self, query: str) -> SearchQueryContext:
        parts = [p for p in query.strip().upper().replace("-", " ").split() if p]
        ctx = SearchQueryContext(
            name_tokens=[],
            month_token=None,
            strike_int=None,
            opt_type=None,
            want_fut=False,
        )

        for tok in parts:
            if tok in self.cfg.stop_words:
                continue
            if tok in self.cfg.month_aliases:
                ctx.month_token = self.cfg.month_aliases[tok]
                continue
            if tok in self.cfg.call_words:
                ctx.opt_type = "CE"
                continue
            if tok in self.cfg.put_words:
                ctx.opt_type = "PE"
                continue
            if tok in self.cfg.fut_words:
                ctx.want_fut = True
                continue
            try:
                v = float(tok)
                if v > 0:
                    ctx.strike_int = int(v) if v.is_integer() else round(v)
                continue
            except ValueError:
                pass
            ctx.name_tokens.append(tok)

        return ctx

    def _get_candidates(self, ctx: SearchQueryContext) -> list[dict]:
        if not ctx.name_tokens:
            candidate_idx = self._all_search_indices
            ctx.used_and = False
        else:
            sorted_toks = sorted(
                ctx.name_tokens,
                key=lambda t: len(self._name_inv_index.get(t, ())),
            )
            result = self._name_inv_index.get(sorted_toks[0], frozenset())
            for t in sorted_toks[1:]:
                result = result.intersection(self._name_inv_index.get(t, frozenset()))
                if not result:
                    break

            if result:
                candidate_idx = frozenset(result)
                ctx.used_and = True
            else:
                union: set[int] = set()
                for t in ctx.name_tokens:
                    union.update(self._name_inv_index.get(t, frozenset()))
                candidate_idx = frozenset(union)
                ctx.used_and = False

        candidates = [self._search_data[i] for i in candidate_idx]

        if ctx.opt_type:
            candidates = [c for c in candidates if c["opt_type"] == ctx.opt_type]
        elif ctx.want_fut:
            candidates = [c for c in candidates if c["inst_type"] == "FUT"]

        if ctx.month_token:
            candidates = [c for c in candidates if c["_month_tag"] == ctx.month_token]

        if ctx.strike_int is not None:
            candidates = [c for c in candidates if c["_strike_int"] == ctx.strike_int]

        return candidates

    def _score_candidate(self, entry: dict, ctx: SearchQueryContext) -> float:
        sym = entry["symbol"]
        ntok = entry["_name_tokens"]
        s = 0.0
        matched = 0

        for tok in ctx.name_tokens:
            if tok not in ntok:
                continue
            matched += 1
            idf = self._idf.get(tok, 1.0)
            if sym == tok:
                s += self.cfg.exact_symbol_bonus + (idf * self.cfg.company_idf_factor)
            elif sym.startswith(tok) or tok.startswith(sym):
                s += self.cfg.symbol_prefix_bonus + (idf * self.cfg.company_idf_factor)
            else:
                s += idf * self.cfg.company_idf_factor

        if ctx.used_and and matched == len(ctx.name_tokens) and ctx.name_tokens:
            s += self.cfg.all_tokens_coherence

        exp = entry["_expiry_sort"]
        has_context = bool(
            ctx.month_token or ctx.strike_int or ctx.opt_type or ctx.want_fut,
        )

        if exp:
            with contextlib.suppress(ValueError):
                s += self.cfg.expiry_proximity_scale * (
                    99_999_999 - int(exp.replace("-", ""))
                )
        elif not has_context:
            s += self.cfg.eq_base_bonus

        return s

    def _format_results(
        self,
        scored: list[tuple[float, dict]],
        limit: int,
    ) -> list[dict]:
        final = []
        for _, item in scored[:limit]:
            obj = {k: v for k, v in item.items() if not k.startswith("_")}
            try:
                obj["strike"] = float(obj.get("strike", 0.0))
            except (TypeError, ValueError):
                obj["strike"] = 0.0
            final.append(obj)
        return final

    def get_data_by_display_name(self, display_name: str) -> dict | None:
        key = " ".join(display_name.strip().upper().split())
        return self._display_to_data.get(key)

    def get_symbol_name(self, sec_id: str, fallback: str = "") -> str:
        if not self._secid_info:
            return fallback
        key = self._secid_info.get(str(sec_id))
        return key[6] if key else fallback

    def get_base_symbol(self, sec_id: str, fallback: str = "") -> str:
        if not self._secid_info:
            return fallback
        key = self._secid_info.get(str(sec_id))
        return key[2] if key else fallback

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

        valid_expiries = self._expiry_index.get((exch, seg, symb))
        if not valid_expiries:
            return None, 0

        date_key = expiry_date
        if not date_key or date_key not in valid_expiries:
            date_key = min(valid_expiries)

        key = (exch, seg, symb, date_key, strike, opt_type)
        result = self._opt_index.get(key)
        return result or (None, 0)

    def _ensure_loaded(self, *, refresh_master_scrip: bool = False) -> None:
        if self._eq_index is not None:
            return
        if not LOCAL_CSV.exists() or refresh_master_scrip:
            raw_df = self._download_segments()
            if raw_df is not None:
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
        df.to_csv(LOCAL_CSV, index=False)

        eq_idx, opt_idx, expiry_idx, sec_info = {}, {}, {}, {}
        self._fold_chunk(df, eq_idx, opt_idx, expiry_idx, sec_info, today_str)
        self._commit_indexes(eq_idx, opt_idx, expiry_idx, sec_info)

    def _index_from_csv(self) -> None:
        eq_idx, opt_idx, expiry_idx, sec_info = {}, {}, {}, {}
        today_str = _get_today_str()
        try:
            with pd.read_csv(
                LOCAL_CSV,
                usecols=self.cfg.scrip_cols,
                chunksize=50_000,
                low_memory=False,
            ) as reader:
                for raw_chunk in reader:
                    chunk = raw_chunk
                    if "INSTRUMENT_TYPE" in chunk.columns:
                        mask = chunk["INSTRUMENT_TYPE"].isin(self.cfg.filter_inst_type)
                        chunk = chunk[mask]
                    self._fold_chunk(
                        chunk,
                        eq_idx,
                        opt_idx,
                        expiry_idx,
                        sec_info,
                        today_str,
                    )
        except (OSError, ValueError, pd.errors.ParserError):
            LOGGER.exception("Error loading scrip master cache")
            return

        self._commit_indexes(eq_idx, opt_idx, expiry_idx, sec_info)

    def _commit_indexes(
        self,
        eq_index: dict,
        opt_index: dict,
        expiry_index: dict,
        secid_info: dict,
    ) -> None:
        self._eq_index = eq_index
        self._opt_index = opt_index
        self._expiry_index = expiry_index
        self._secid_info = secid_info

        base_to_name: dict[str, str] = {}
        for info in secid_info.values():
            if info[1] in self.cfg.eq_index_instr and info[6]:
                base_to_name[info[2]] = str(info[6]).strip().upper()

        search_entries = self._build_search_entries(secid_info, base_to_name)
        search_entries.sort(key=lambda x: x["_sort_key"])

        self._search_data = search_entries
        self._display_to_data = {
            " ".join(x["display"].upper().split()): x for x in search_entries
        }
        self._build_inverted_index(search_entries)

    def _make_display_str(
        self,
        inst: str,
        underlying: str,
        display_name: str,
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

    def _normalise_opt_type(self, opt_type: str | None) -> tuple[str, str]:
        opt_safe_raw = str(opt_type).strip().upper() if opt_type else ""
        if opt_safe_raw.startswith("C"):
            return "CE", "CE CALL CA C"
        if opt_safe_raw.startswith("P"):
            return "PE", "PE PUT PA P"
        return "", ""

    def _build_name_tokens(self, underlying: str, comp_name: str) -> frozenset[str]:
        name_src = f"{underlying} {comp_name}"
        raw_name = [
            t.upper()
            for t in name_src.replace("-", " ").split()
            if t.strip() and t.upper() not in self.cfg.stop_words
        ]
        name_token_set: set[str] = set()
        for tok in raw_name:
            name_token_set.add(tok)
            for plen in range(3, min(len(tok), 5)):
                name_token_set.add(tok[:plen])
        return frozenset(name_token_set)

    def _build_search_entries(self, secid_info: dict, base_to_name: dict) -> list[dict]:
        entries: list[dict] = []
        for info in secid_info.values():
            exch_id, inst, underlying, exp, strike, opt_type, display_name = info
            if not display_name:
                continue

            display_str, inst_type, inst_priority = self._make_display_str(
                inst,
                underlying,
                display_name,
            )

            exp_str = str(exp) if exp else ""
            month_tag = ""
            for m_int in range(1, 13):
                if f"-{m_int:02d}-" in exp_str:
                    month_tag = self.cfg.month_tags.get(m_int, "")
                    break

            opt_safe, _ = self._normalise_opt_type(opt_type)

            strike_val = float(strike or 0.0)
            strike_int = (
                int(strike_val)
                if strike_val and float(strike_val).is_integer()
                else None
            )

            name_token_set = self._build_name_tokens(
                underlying,
                base_to_name.get(underlying, ""),
            )

            entries.append(
                {
                    "display": display_str,
                    "symbol": underlying,
                    "inst_type": inst_type,
                    "strike": strike_val,
                    "opt_type": opt_safe,
                    "expiry": exp_str,
                    "exch": exch_id,
                    "_name_tokens": name_token_set,
                    "_month_tag": month_tag,
                    "_strike_int": strike_int,
                    "_expiry_sort": exp_str,
                    "_sort_key": (inst_priority, exp_str, strike_val, underlying),
                },
            )
        return entries

    def _build_inverted_index(self, search_entries: list[dict]) -> None:
        total_entries = len(search_entries)
        doc_freq: dict[str, int] = defaultdict(int)
        posting: dict[str, list[int]] = defaultdict(list)

        for i, entry in enumerate(search_entries):
            for tok in entry["_name_tokens"]:
                posting[tok].append(i)
                doc_freq[tok] += 1

        self._name_inv_index = {
            tok: frozenset(indices) for tok, indices in posting.items()
        }
        self._all_search_indices = frozenset(range(total_entries))
        self._idf = {
            tok: math.log(total_entries / df) + 1.0
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

        expiry_strs = chunk["SM_EXPIRY_DATE"].astype(str).str.split().str[0]
        eq_mask = chunk["INSTRUMENT"].isin(self.cfg.eq_index_instr)

        for row in chunk[eq_mask].itertuples(index=False):
            str_sec_id = str(row.SECURITY_ID)
            eq_index[(row.EXCH_ID, row.UNDERLYING_SYMBOL)] = (
                str_sec_id,
                int(row.LOT_SIZE),
            )
            display_raw = getattr(row, "DISPLAY_NAME", None)
            disp_name = (
                row.UNDERLYING_SYMBOL
                if pd.isna(display_raw) or not display_raw
                else " ".join(str(display_raw).split())
            )
            secid_info[str_sec_id] = (
                row.EXCH_ID,
                row.INSTRUMENT,
                row.UNDERLYING_SYMBOL,
                None,
                None,
                None,
                disp_name,
            )

        for row, exp in zip(
            chunk[~eq_mask].itertuples(index=False),
            expiry_strs[~eq_mask],
            strict=True,
        ):
            if exp < today_str:
                continue
            base_key = (row.EXCH_ID, row.INSTRUMENT, row.UNDERLYING_SYMBOL)
            expiry_index.setdefault(base_key, set()).add(exp)

            strike, opt_type = None, None
            if str(row.INSTRUMENT).startswith("OPT"):
                strike = float(row.STRIKE_PRICE)
                opt_type = str(row.OPTION_TYPE).strip().upper()

            display_raw = getattr(row, "DISPLAY_NAME", None)
            disp_name = (
                row.UNDERLYING_SYMBOL
                if pd.isna(display_raw) or not display_raw
                else " ".join(str(display_raw).split())
            )

            key = (
                row.EXCH_ID,
                row.INSTRUMENT,
                row.UNDERLYING_SYMBOL,
                exp,
                strike,
                opt_type,
            )
            if key not in opt_index:
                str_sec_id = str(row.SECURITY_ID)
                opt_index[key] = (str_sec_id, int(row.LOT_SIZE))
                secid_info[str_sec_id] = (
                    row.EXCH_ID,
                    row.INSTRUMENT,
                    row.UNDERLYING_SYMBOL,
                    exp,
                    strike,
                    opt_type,
                    disp_name,
                )
