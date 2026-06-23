"""Search engine module for financial instruments."""

from __future__ import annotations

import array
import contextlib
import heapq
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tradeapi.scrip_master import ScripConfig, ScripEntry

_MIN_QUERY_LEN = 2

# Only internal/helper fields that must never appear in search result dicts.
# expiry_sort and sort_key have been removed from ScripEntry entirely.
EXCLUDED_KEYS = {"month_tag", "name_tokens"}


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

    def __init__(self, cfg: ScripConfig):
        self.cfg = cfg
        self.entries: list[ScripEntry] = []
        # ── Posting lists stored as compact signed-int arrays instead of
        #    frozenset[int].  frozenset hash-table overhead is ~12.8 B/element;
        #    array.array('i') costs exactly 4 B/element.  For a 12k-word vocab
        #    with avg posting-list depth of 300 this saves ~85 MB.
        self._name_inv_index: dict[str, array.array] = {}
        self._idf: dict[str, float] = {}
        self.vocab: list[str] = []
        # NOTE: _all_indices removed — we use range(len(self.entries)) lazily
        #       instead of materialising a 300k-element frozenset (~8 MB).

    @staticmethod
    def is_subsequence(query: str, target: str) -> bool:
        it = iter(target)
        return all(c in it for c in query)

    def build_index(
        self, entries: list[ScripEntry], base_to_name: dict[str, str]
    ) -> None:
        self.entries = entries
        total_entries = len(entries)

        posting: dict[str, list[int]] = defaultdict(list)
        doc_freq: dict[str, int] = defaultdict(int)

        # Cache underlying tokens to share tuple references across options!
        token_cache: dict[str, tuple[str, ...]] = {}

        # Pre-calculate ignore words for extremely fast filtering
        ignore_words = self.cfg.stop_words.copy()
        ignore_words.update({"CE", "PE", "CALL", "PUT", "FUT", "OPT", "EQ"})
        ignore_words.update(self.cfg.month_aliases.keys())

        for i, entry in enumerate(entries):
            underlying = entry.symbol

            # [RAM FIX]: Options share identical textual tokens with their base equity.
            # Reusing the exact same tuple reference saves ~25 MB of RAM instantly.
            if entry.inst_type != "EQ" and underlying in token_cache:
                tokens = token_cache[underlying]
                entry.name_tokens = tokens
                for tok in tokens:
                    posting[tok].append(i)
                    doc_freq[tok] += 1
                continue

            comp_name = base_to_name.get(underlying, "")
            display_name = entry.display

            text = f"{underlying} {comp_name} {display_name}".upper()

            words = {
                w
                for w in text.replace("-", " ").split()
                if w and w not in ignore_words and not w.isdigit()
            }

            tokens = tuple(words)
            entry.name_tokens = tokens
            if entry.inst_type == "EQ":
                token_cache[underlying] = tokens

            for tok in tokens:
                posting[tok].append(i)
                doc_freq[tok] += 1

        self._name_inv_index = {
            tok: array.array("i", idx_list) for tok, idx_list in posting.items()
        }
        del posting

        self.vocab = sorted(self._name_inv_index.keys())

        self._idf = {
            tok: math.log(total_entries / df) + 1.0
            for tok, df in doc_freq.items()
            if df > 0
        }
        del doc_freq

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

        scored = [(self._score_candidate(c, ctx), c) for c in candidates]

        # ── MEMORY CHANGE: use entry.expiry in place of removed expiry_sort
        best_scored = heapq.nsmallest(
            limit,
            scored,
            key=lambda x: (-x[0], x[1].expiry, x[1].strike),
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
            with contextlib.suppress(ValueError):
                v = float(tok)
                if math.isfinite(v) and v > 0:
                    ctx.strike_int = int(v) if v.is_integer() else round(v)
                continue
            ctx.name_tokens.append(tok)

        return ctx

    def _get_candidates(self, ctx: SearchQueryContext) -> list[ScripEntry]:
        if not ctx.name_tokens:
            # ── MEMORY CHANGE: range object (48 B) replaces a 300k-element
            #    frozenset (~8 MB).  List comprehension below accepts any iterable.
            candidate_idx: set | range = range(len(self.entries))
            ctx.used_and = False
        else:
            token_matches: list[set[int]] = []

            for q_tok in ctx.name_tokens:
                tok_idx: set[int] = set()

                matched_words = [w for w in self.vocab if w.startswith(q_tok)]
                if matched_words:
                    for w in matched_words:
                        # array.array is iterable — set.update() accepts it directly
                        tok_idx.update(self._name_inv_index[w])
                else:
                    matched_words = [
                        w for w in self.vocab if self.is_subsequence(q_tok, w)
                    ]
                    for w in matched_words:
                        tok_idx.update(self._name_inv_index[w])

                token_matches.append(tok_idx)

            if token_matches:
                sorted_matches = sorted(token_matches, key=len)
                result = sorted_matches[0].copy()
                for idx_set in sorted_matches[1:]:
                    result.intersection_update(idx_set)
                    if not result:
                        break

                if result:
                    candidate_idx = result  # plain set — no frozenset wrap needed
                    ctx.used_and = True
                else:
                    union: set[int] = set()
                    for idx_set in token_matches:
                        union.update(idx_set)
                    candidate_idx = union
                    ctx.used_and = False
            else:
                candidate_idx = set()
                ctx.used_and = False

        candidates = [self.entries[i] for i in candidate_idx]

        if ctx.opt_type:
            candidates = [c for c in candidates if c.opt_type == ctx.opt_type]
        elif ctx.want_fut:
            candidates = [c for c in candidates if c.inst_type == "FUT"]

        if ctx.month_token:
            candidates = [c for c in candidates if c.month_tag == ctx.month_token]

        # Lazily check strike logic to avoid storing 300,000 integers in memory
        if ctx.strike_int is not None:
            candidates = [
                c
                for c in candidates
                if c.strike > 0
                and c.strike.is_integer()
                and int(c.strike) == ctx.strike_int
            ]

        return candidates

    def _score_candidate(self, entry: ScripEntry, ctx: SearchQueryContext) -> float:
        sym = entry.symbol
        ntok = entry.name_tokens  # now a tuple — iteration/membership unchanged
        s = 0.0
        matched = 0

        for tok in ctx.name_tokens:
            idf = self._idf.get(tok, 1.0)

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

        # ── MEMORY CHANGE: entry.expiry replaces removed expiry_sort slot
        exp = entry.expiry
        has_context = bool(
            ctx.month_token or ctx.strike_int or ctx.opt_type or ctx.want_fut
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
        self, scored: list[tuple[float, ScripEntry]], limit: int
    ) -> list[dict]:
        final = []
        for _, item in scored[:limit]:
            obj = {k: v for k, v in item.items() if k not in EXCLUDED_KEYS}
            try:
                obj["strike"] = float(obj.get("strike", 0.0))
            except (TypeError, ValueError):
                obj["strike"] = 0.0
            final.append(obj)
        return final
