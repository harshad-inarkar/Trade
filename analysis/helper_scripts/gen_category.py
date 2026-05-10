"""
Creates 5 NSE Index CSV Files + Processes candidates.txt → 5_lists.csv
Files from NSE: BankNifty, Finnifty, Nifty MidSelect, Nifty50, Nifty Smallcap 100
"""

import pandas as pd
import os, sys
from create_sectoral_index_files import get_file_name_from_index, download_sectoral_indices, NIFTY_INDICES, SECTORAL_INDICES

from nse_daily_data_downloader import NSE_INDX_DATA, OUT_DIR



def process_with_index_files(input_file='candidates.txt', output_file='categories.csv',use_nse_indx=True,unique_category=True):
    """Read 5 NSE index files → categorize candidates.txt → create categories.csv"""

    input_file = f'{OUT_DIR}/{input_file}'
    output_file = f'{OUT_DIR}/{output_file}'
    # Read all 5 index files

   # Priority categorization: BANKNIFTY > FINNIFTY > NIFTY_MIDSELECT > NIFTY50 > NIFTY_SMALLCAP100 > OTHERS
    
    priority_order = list(NIFTY_INDICES) if use_nse_indx else list(SECTORAL_INDICES)
    
    
    index_files = [f'{NSE_INDX_DATA}/{get_file_name_from_index(indx)}' for indx in priority_order]
    category_sets = {}
    
    i= 0
    nse_dl= False
    for file in index_files:

        if not os.path.exists(file) and not nse_dl:
            download_sectoral_indices(priority_order)
            nse_dl = True

        if os.path.exists(file):
            df = pd.read_csv(file)
            symbols = df['Symbol'].str.upper().str.strip().tolist()
            category_sets[priority_order[i]] = set(symbols)
            i+=1

    # download done

    priority_order.append('OTHERS')
    lists_data = [[] for i in range(len(priority_order))]

    # Read candidates.txt
    with open(input_file, 'r') as f:
        all_stocks = [line.strip().upper() for line in f if line.strip()]
    
        
    for stock in all_stocks:
        categorized = False
        for cat_name in priority_order:
            if stock in category_sets.get(cat_name, set()):
                lists_data[priority_order.index(cat_name)].append(stock)
                categorized = True
                if unique_category:
                    break
        
        if not categorized:
            lists_data[-1].append(stock)
    
    # Pad all lists to same length (length of candidates.txt)

    max_length = max([len(lst) for lst in lists_data])

    for lst in lists_data:
        lst.extend([''] * (max_length - len(lst)))
    
    # Create final output DataFrame
    
    df_output = pd.DataFrame(dict(zip(priority_order, lists_data)))

    tot = 0
    for col in df_output.columns:
        count = df_output[col][df_output[col] != ''].count()
        tot+=count
        print(f"{col}: {count}")

    df_output.to_csv(output_file, index=False)
    print(f"✅ {output_file} created with total {tot} stocks")

# MAIN EXECUTION
if __name__ == "__main__":
 

    if len(sys.argv) != 3 and len(sys.argv) != 1:
        print("Usage: python script.py <use_nse_indx_flag>")
        sys.exit(1)

    use_nse_indx_flag = True
    unique_category_flag = True

    if len(sys.argv) == 3:
        use_nse_indx_flag = True if sys.argv[1] == '1' else False
        unique_category_flag = True if sys.argv[2] == '1' else False


    print("\n🔄 Processing candidates.txt with NSE files...")
    print(f"use_nse_indx_flag {use_nse_indx_flag} and unique_category_flag {unique_category_flag}")
  
    process_with_index_files(use_nse_indx=use_nse_indx_flag, unique_category=unique_category_flag)