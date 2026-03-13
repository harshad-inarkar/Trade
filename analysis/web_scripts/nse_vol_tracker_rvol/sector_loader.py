"""
sector_loader.py  –  Parses all_categories.csv to build sector -> [symbols] map.
Place this file alongside app.py.
"""

import csv
import os
from collections import defaultdict

from sys import path as _syspath
_syspath.append(os.path.abspath("../../")) # analysis dir
from web_scripts.data_scripts.sync_data import NSE_INDX_DATA

# Resolve path relative to this file so it works regardless of cwd
CATEGORIES_CSV = os.path.join(NSE_INDX_DATA, 'categories.csv')
UNIQ_CATEGORIES_CSV = os.path.join(NSE_INDX_DATA, 'uniq_categories.csv')


def load_sector_symbols(csv_path: str = CATEGORIES_CSV) -> dict[str, list[str]]:
    """
    Returns an ordered dict: { sector_name: [symbol, ...] }
    Row-0 of the CSV contains sector names (headers).
    Each subsequent row has one symbol per sector column (may be empty).
    """
    sector_symbols: dict[str, list[str]] = {}

    with open(csv_path, newline='', encoding='utf-8') as fh:
        reader = csv.reader(fh)
        rows = list(reader)

    if not rows:
        return sector_symbols

    headers = [h.strip() for h in rows[0]]

    # Pre-build ordered keys
    for h in headers:
        if h:
            sector_symbols[h] = []

    for row in rows[1:]:
        for i, cell in enumerate(row):
            sym = cell.strip()
            if sym and i < len(headers) and headers[i]:
                sector_symbols[headers[i]].append(sym)

    return sector_symbols