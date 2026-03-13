"""
Processes candidates.txt → categories.csv using NSE index files.

Priority is determined dynamically by index file size:
  - Smaller file (fewer stocks) = more specific sector = HIGHER priority
  - Larger file (more stocks)   = broader index        = LOWER priority

use_nse_indx=True  → NIFTY_INDICES only
use_nse_indx=False → SECTORAL_INDICES (includes the 5 NIFTY indices, deduped)

Final fallback → 'OTHERS'
"""

import pandas as pd
import os, sys, argparse
from create_sectoral_index_files import get_file_name_from_index, download_sectoral_indices, SECTORAL_INDICES, NSE_INDX_DATA

CATEGORIES_CSV = os.path.join(NSE_INDX_DATA, 'categories.csv')
UNIQ_CATEGORIES_CSV = os.path.join(NSE_INDX_DATA, 'uniq_categories.csv')


def process_with_index_files(all_flag=False,unique_category=False):

    input_file_name='nse_fno.csv'
    input_file  = f'{NSE_INDX_DATA}/{input_file_name}'

    
    output_file = CATEGORIES_CSV if not unique_category else UNIQ_CATEGORIES_CSV

    index_list = list(SECTORAL_INDICES)

    # Download any missing files
    missing = [indx for indx in index_list
               if not os.path.exists(f'{NSE_INDX_DATA}/{get_file_name_from_index(indx)}')]
    missing_file_list = [get_file_name_from_index(indx) for indx in missing]
               
    if missing:
        print(f"📥 Downloading {len(missing)} missing index file(s)...\n{missing_file_list}")
        download_sectoral_indices(missing)

    # Load indices
    category_sets  = {}
    category_sizes = {}
    all_symbols_set = set()
    for indx in index_list:
        path = f'{NSE_INDX_DATA}/{get_file_name_from_index(indx)}'
        if os.path.exists(path):
            df = pd.read_csv(path)
            symbols = set(df['Symbol'].str.upper().str.strip().tolist())
            category_sets[indx]  = symbols
            category_sizes[indx] = len(symbols)
            all_symbols_set.update(symbols)
        else:
            print(f"⚠️  Skipping {indx} — file not found: {path}")

    # Sort by size ascending: smallest (most specific) = highest priority
    priority_order = sorted(category_sets.keys(), key=lambda x: category_sizes[x])

    if unique_category:
        print("\n📊 Index priority order (most specific → most general):")
        for rank, indx in enumerate(priority_order, 1):
            print(f"   {rank}. {indx:50s} → {category_sizes[indx]} stocks")

    final_order = priority_order + ['OTHERS']
    lists_data  = {cat: [] for cat in final_order}

    # Read candidates
    all_stocks = all_symbols_set
    candidates=set()

    if os.path.exists(input_file):
        df = pd.read_csv(input_file)
        candidates = set(df['Symbol'].str.upper().str.strip().tolist())

    if not all_flag:
        print(f'\nProcessing {input_file_name} Only')
        all_stocks = candidates

    else:
        print('\nProcessing Full Symbols Set')
        all_stocks.update(candidates)


    # Categorize each stock
    for stock in all_stocks:
        categorized = False
        for cat_name in priority_order:
            if stock in category_sets.get(cat_name, set()):
                lists_data[cat_name].append(stock)
                categorized = True
                if unique_category:
                    break
        if not categorized:
            lists_data['OTHERS'].append(stock)

    # Pad & write
    max_length = max((len(lst) for lst in lists_data.values()), default=0)
    for lst in lists_data.values():
        lst.extend([''] * (max_length - len(lst)))

    df_output = pd.DataFrame(lists_data, columns=final_order)

    print("\n📋 Stock counts per category:")
    tot = 0
    for col in df_output.columns:
        count = (df_output[col] != '').values.sum()
        tot += count
        if count > 0 or col == 'OTHERS':
            print(f"   {col:50s}: {count}")

    df_output.to_csv(output_file, index=False)
    print(f"\n✅ {output_file} created with {len(all_stocks)} total stocks across {len(final_order)} categories")


# MAIN EXECUTION
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Category')
    parser.add_argument('-al', '--all', action='store_true', help='Use Full Symbols List')
    parser.add_argument('-uc', '--uniq-cat', action='store_true', help='Use Full Symbols List')

    args, unknown = parser.parse_known_args()

    all_flag = False
    if args.all:
        all_flag = True
   
    unique_category_flag = False
    if args.uniq_cat:
        unique_category_flag = True

    print(f'Uniq Category = {unique_category_flag} and All Flag = {all_flag}')
    process_with_index_files(all_flag=all_flag, unique_category=unique_category_flag)