"""
Processes candidates.txt → categories.csv using NSE index files.

Priority is determined dynamically by index file size:
  - Smaller file (fewer stocks) = more specific sector = HIGHER priority
  - Larger file (more stocks)   = broader index        = LOWER priority

use_nse_indx=True  → NIFTY_INDICES only
use_nse_indx=False → SECTORAL_INDICES (includes the 5 NIFTY indices, deduped)

Final fallback → 'OTHERS'
"""

import argparse
from pathlib import Path

import pandas as pd

from utils.data.create_sectoral_index_files import (
    SECTORAL_INDICES,
    download_sectoral_indices,
    get_file_name_from_index,
)
from utils.data.paths import NSE_INDX_DATA
from utils.utility import out

CATEGORIES_CSV = Path(NSE_INDX_DATA) / "categories.csv"
UNIQ_CATEGORIES_CSV = Path(NSE_INDX_DATA) / "uniq_categories.csv"


def _download_missing_indices(index_list: list[str]) -> None:
    missing = [
        indx
        for indx in index_list
        if not (Path(NSE_INDX_DATA) / get_file_name_from_index(indx)).exists()
    ]
    if missing:
        missing_file_list = [get_file_name_from_index(indx) for indx in missing]
        out(f"📥 Downloading {len(missing)} missing index file(s)...")
        out(str(missing_file_list))
        download_sectoral_indices(missing)


def _load_indices(index_list: list[str]) -> tuple[dict, dict, set]:
    category_sets = {}
    category_sizes = {}
    all_symbols_set = set()
    for indx in index_list:
        path = Path(NSE_INDX_DATA) / get_file_name_from_index(indx)
        if path.exists():
            df = pd.read_csv(path)
            symbols = set(df["Symbol"].str.upper().str.strip().tolist())
            category_sets[indx] = symbols
            category_sizes[indx] = len(symbols)
            all_symbols_set.update(symbols)
        else:
            out(f"⚠️  Skipping {indx} — file not found: {path}")
    return category_sets, category_sizes, all_symbols_set


def _display_priority(priority_order: list[str], category_sizes: dict) -> None:
    out("\n📊 Index priority order (most specific → most general):")
    for rank, indx in enumerate(priority_order, 1):
        out(f"   {rank}. {indx:50s} → {category_sizes[indx]} stocks")


def process_with_index_files(
    *, all_flag: bool = False, unique_category: bool = False
) -> None:
    input_file_name = "nse_fno.csv"
    input_file = Path(NSE_INDX_DATA) / input_file_name
    output_file = CATEGORIES_CSV if not unique_category else UNIQ_CATEGORIES_CSV

    index_list = list(SECTORAL_INDICES)
    _download_missing_indices(index_list)

    category_sets, category_sizes, all_symbols_set = _load_indices(index_list)
    priority_order = sorted(category_sets.keys(), key=lambda x: category_sizes[x])

    if unique_category:
        _display_priority(priority_order, category_sizes)

    final_order = [*priority_order, "OTHERS"]
    lists_data = {cat: [] for cat in final_order}

    all_stocks = all_symbols_set
    candidates = set()

    if input_file.exists():
        df = pd.read_csv(input_file)
        candidates = set(df["Symbol"].str.upper().str.strip().tolist())

    if not all_flag:
        out(f"\nProcessing {input_file_name} Only")
        all_stocks = candidates
    else:
        out("\nProcessing Full Symbols Set")
        all_stocks.update(candidates)

    for stock in all_stocks:
        categorized = False
        for cat_name in priority_order:
            if stock in category_sets.get(cat_name, set()):
                lists_data[cat_name].append(stock)
                categorized = True
                if unique_category:
                    break
        if not categorized:
            lists_data["OTHERS"].append(stock)

    max_length = max((len(lst) for lst in lists_data.values()), default=0)
    for lst in lists_data.values():
        lst.extend([""] * (max_length - len(lst)))

    df_output = pd.DataFrame(lists_data, columns=final_order)

    out("\n📋 Stock counts per category:")
    tot = 0
    for col in df_output.columns:
        count = (df_output[col] != "").to_numpy().sum()
        tot += count
        if count > 0 or col == "OTHERS":
            out(f"   {col:50s}: {count}")

    df_output.to_csv(output_file, index=False)

    msg = (
        f"\n✅ {output_file} created with {len(all_stocks)} total stocks "
        f"across {len(final_order)} categories"
    )
    out(msg)


# MAIN EXECUTION
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Category")
    parser.add_argument(
        "-al", "--all", action="store_true", help="Use Full Symbols List"
    )
    parser.add_argument(
        "-uc", "--uniq-cat", action="store_true", help="Use Full Symbols List"
    )

    args, unknown = parser.parse_known_args()

    all_flag = bool(args.all)
    unique_category_flag = bool(args.uniq_cat)

    out(f"Uniq Category = {unique_category_flag} and All Flag = {all_flag}")
    process_with_index_files(all_flag=all_flag, unique_category=unique_category_flag)
