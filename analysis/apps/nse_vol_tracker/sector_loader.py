"""
sector_loader.py
----------------
Parses categories.csv to build sector -> [symbols] map.

Changes from original:
  - ARCH 2 fixed: replaced @lru_cache (which hid stale data forever) with an
    mtime-aware cache.  If categories.csv is updated on disk the next call
    automatically reloads it — no restart required.
  - PLW0603 fixed: module-level mutable state moved into a dataclass so no
    `global` statements are needed.
  - Added invalidate_sector_cache() for explicit invalidation (e.g. from an
    admin endpoint).
  - Kept the public API identical: load_sector_symbols() returns the same
    dict[str, list[str]] as before.
"""

import csv
import threading
from dataclasses import dataclass, field
from pathlib import Path

from utils.data.paths import NSE_INDX_DATA

CATEGORIES_CSV = Path(NSE_INDX_DATA) / "categories.csv"


# ---------------------------------------------------------------------------
# Cache state container  (avoids module-level `global` mutations — PLW0603)
# ---------------------------------------------------------------------------


@dataclass
class _SectorCache:
    lock: threading.Lock = field(default_factory=threading.Lock)
    data: dict[str, list[str]] | None = None
    mtime: float = 0.0
    path: Path | None = None


_cache = _SectorCache()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_sector_symbols(csv_path: str | Path = CATEGORIES_CSV) -> dict[str, list[str]]:
    """
    Returns an ordered dict: { sector_name: [symbol, ...] }

    Row-0 of the CSV contains sector names (headers).
    Each subsequent row has one symbol per sector column (may be empty).

    The result is cached in memory.  The cache is invalidated automatically
    when the file's mtime changes, so hot-reloading categories.csv does not
    require an application restart.
    """
    p = Path(csv_path)

    with _cache.lock:
        try:
            current_mtime = p.stat().st_mtime
        except OSError:
            # File missing — return whatever we have (or empty dict)
            return _cache.data or {}

        if (
            _cache.data is not None
            and _cache.path == p
            and current_mtime == _cache.mtime
        ):
            return _cache.data

        # Cache miss or file changed: (re)parse
        parsed = _parse_sector_csv(p)
        _cache.data = parsed
        _cache.mtime = current_mtime
        _cache.path = p
        return parsed


def invalidate_sector_cache() -> None:
    """Force the next load_sector_symbols() call to re-read from disk."""
    with _cache.lock:
        _cache.data = None
        _cache.mtime = 0.0


# ---------------------------------------------------------------------------
# Internal parser
# ---------------------------------------------------------------------------


def _parse_sector_csv(csv_path: Path) -> dict[str, list[str]]:
    sector_symbols: dict[str, list[str]] = {}

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows = list(reader)

    if not rows:
        return sector_symbols

    headers = [h.strip() for h in rows[0]]

    for h in headers:
        if h:
            sector_symbols[h] = []

    for row in rows[1:]:
        for i, cell in enumerate(row):
            sym = cell.strip()
            if sym and i < len(headers) and headers[i]:
                sector_symbols[headers[i]].append(sym)

    return sector_symbols
