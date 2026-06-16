"""Search engine module for financial instruments."""

import contextlib
import heapq
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tradeapi.scrip_master import ScripConfig

_MIN_QUERY_LEN = 2


@dataclass
class SearchQueryContext:
    name_tokens: list[str]
    month_token: str | None
    strike_int: int | None
    opt_type: str | None
    want_fut: bool
    used_and: bool = False


class SearchEngine:
    """Decoupled Search Engine for indexing and scoring financial instruments."""

    def __init__(self, cfg: "ScripConfig"):
        self.cfg = cfg
        self.entries: list[dict] = []
        self._name_inv_index: dict[str, frozenset[int]] = {}
        self._all_indices: frozenset[int] = frozenset()
        self._idf: dict[str, float] = {}
        self.vocab: list[str] = []

    @staticmethod
    def is_subsequence(query: str, target: str) -> bool:
        """Robustly matches abbreviations (e.g., 'NATGAS' in 'NATURALGASM')."""
        it = iter(target)
        return all(c in it for c in query)

    def build_index(self, entries: list[dict], base_to_name: dict[str, str]) -> None:
        self.entries = entries
        total_entries = len(entries)
        posting: dict[str, set[int]] = defaultdict(set)
        doc_freq: dict[str, int] = defaultdict(int)

        for i, entry in enumerate(entries):
            underlying = entry["symbol"]
            comp_name = base_to_name.get(underlying, "")
            display_name = entry["display"]

            # Tokenize strictly by full words to save RAM
            text = f"{underlying} {comp_name} {display_name}".upper()
            words = {
                w
                for w in text.replace("-", " ").split()
                if w and w not in self.cfg.stop_words
            }

            entry["_name_tokens"] = frozenset(words)

            for tok in words:
                posting[tok].add(i)
                doc_freq[tok] += 1

        self._name_inv_index = {tok: frozenset(idx) for tok, idx in posting.items()}
        self.vocab = sorted(self._name_inv_index.keys())
        self._all_indices = frozenset(range(total_entries))
        self._idf = {
            tok: math.log(total_entries / df) + 1.0
            for tok, df in doc_freq.items()
            if df > 0
        }

    def search(self, query: str, limit: int = 30) -> list[dict]:
        if not self.entries or len(query) < _MIN_QUERY_LEN:
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

        # 1. Calculate scores for all candidates
        scored = [(self._score_candidate(c, ctx), c) for c in candidates]

        # 2. Use a heap to efficiently extract ONLY the top `limit` results
        best_scored = heapq.nsmallest(
            limit,
            scored,
            key=lambda x: (-x[0], x[1]["_expiry_sort"], x[1]["strike"]),
        )

        return self._format_results(best_scored, limit)

    def _parse_query(self, query: str) -> SearchQueryContext:
        parts = [p for p in query.strip().upper().replace("-", " ").split() if p]
        ctx = SearchQueryContext(
            name_tokens=[],
            month_token=None,
            strike_int=None,
            opt_type=None,
            want_fut=False,
        )

        for raw_tok in parts:
            if raw_tok in self.cfg.stop_words:
                continue

            tok = self.cfg.symbol_aliases.get(raw_tok, raw_tok)

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
                if math.isfinite(v) and v > 0:
                    ctx.strike_int = int(v) if v.is_integer() else round(v)
                continue
            except ValueError:
                pass
            ctx.name_tokens.append(tok)

        return ctx

    def _get_candidates(self, ctx: SearchQueryContext) -> list[dict]:
        if not ctx.name_tokens:
            candidate_idx = self._all_indices
            ctx.used_and = False
        else:
            token_matches = []

            for q_tok in ctx.name_tokens:
                tok_idx = set()

                # 1. Exact & Prefix Match (using dynamic vocabulary scan)
                matched_words = [w for w in self.vocab if w.startswith(q_tok)]
                if matched_words:
                    for w in matched_words:
                        tok_idx.update(self._name_inv_index[w])
                else:
                    # 2. Subsequence Fallback
                    matched_words = [
                        w for w in self.vocab if self.is_subsequence(q_tok, w)
                    ]
                    for w in matched_words:
                        tok_idx.update(self._name_inv_index[w])

                token_matches.append(tok_idx)

            if token_matches:
                # AND logic: Intersect indices starting with smallest set
                sorted_matches = sorted(token_matches, key=len)
                result = sorted_matches[0].copy()
                for idx_set in sorted_matches[1:]:
                    result.intersection_update(idx_set)
                    if not result:
                        break

                if result:
                    candidate_idx = frozenset(result)
                    ctx.used_and = True
                else:
                    # Fallback to OR logic if intersection yields nothing
                    union = set()
                    for idx_set in token_matches:
                        union.update(idx_set)
                    candidate_idx = frozenset(union)
                    ctx.used_and = False
            else:
                candidate_idx = frozenset()
                ctx.used_and = False

        # Apply structural filters
        candidates = [self.entries[i] for i in candidate_idx]

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
            idf = self._idf.get(tok, 1.0)

            # Primary target: The underlying symbol
            if sym == tok:
                matched += 1
                s += self.cfg.exact_symbol_bonus + (idf * self.cfg.company_idf_factor)
                continue
            if sym.startswith(tok) or tok.startswith(sym):
                matched += 1
                s += self.cfg.symbol_prefix_bonus + (idf * self.cfg.company_idf_factor)
                continue
            if self.is_subsequence(tok, sym):
                matched += 1
                s += (self.cfg.symbol_prefix_bonus * 0.5) + (
                    idf * self.cfg.company_idf_factor * 0.5
                )
                continue

            # Secondary target: Company/Display names
            if tok in ntok:
                matched += 1
                s += idf * self.cfg.company_idf_factor
            elif any(w.startswith(tok) for w in ntok):
                matched += 1
                s += idf * self.cfg.company_idf_factor * 0.8
            elif any(self.is_subsequence(tok, w) for w in ntok):
                matched += 1
                s += idf * self.cfg.company_idf_factor * 0.4

        if ctx.used_and and matched >= len(ctx.name_tokens) and ctx.name_tokens:
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
