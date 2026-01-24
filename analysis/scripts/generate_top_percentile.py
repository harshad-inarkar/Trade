import csv
import sys
import math
import os
import re
from datetime import datetime


if len(sys.argv) != 7:
    print("Usage: python script.py <top> <last_ndays> <topk> <filter_ltp_range> <fo> <append>")
    sys.exit(1)

top= int(sys.argv[1])
last_ndays= int(sys.argv[2])
topk = int(sys.argv[3])
ltp_list= sys.argv[4].split('-')
start_ltp = float(ltp_list[0])
end_ltp = float(ltp_list[1])
fo = True if sys.argv[5] == 'f' else False
append_file = True if sys.argv[6] == '1' else False

value_col = 'Value (₹ Lakhs) - Futures' if  fo else 'Value (₹ Lakhs) - Options (Premium)'


data_dir = 'nse_data'
symb_ltp = {}
symbol_percentile_ewma_final = {}
symbol_vol_data = {}



def process_files():
    all_files = os.listdir(data_dir)
    csv_files = [f for f in all_files if f.endswith('.csv')]

    nse_files_with_dates = []
    date_pattern = r'nse_data_(\d{2})(\d{2})(\d{4})\.csv'
    
    for filename in csv_files:
        match = re.match(date_pattern, filename)
        if match:
            day, month, year = match.groups()
            date_str = f"{day}{month}{year}"
            
            # Parse date for proper sorting
            try:
                file_date = datetime.strptime(date_str, '%d%m%Y')
                nse_files_with_dates.append({
                    'filename': filename,
                    'date_str': date_str,
                    'date_obj': file_date
                })
            except ValueError:
                print(f"⚠️  Invalid date in {filename}, skipping")
                continue


    sorted_files = sorted(nse_files_with_dates, key=lambda x: x['date_obj'])
    files = [f'{data_dir}/{file_info['filename']}' for file_info in sorted_files]

 
    day_indx = 1
    for fname in files:
        with open(fname, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row['Symbol'].strip('"')
                symb_ltp[symbol] = float(row['Underlying'].strip('"'))
                
                if value_col in row:
                    value = float(row[value_col])
                    symbol_vol_data.setdefault(symbol,{})[day_indx] = value

        day_indx+=1

    return len(files)


def filter_ltp_range(symb_dict):
    sym_to_del = set()
    for sym in symb_dict.keys():
        ltp = symb_ltp[sym]
        if not(ltp >= start_ltp and ltp <= end_ltp):
            sym_to_del.add(sym)

    for sym in sym_to_del:
        del symb_dict[sym]


def get_percentile_dict(day_start,day_end):
    print(f"Processing  {day_start} to {day_end} files")
    symbol_percentile_ewma = {}
    
    if day_start > day_end:
        return symbol_percentile_ewma

    span = day_end - day_start+1    #10
    alpha = 2 / (span + 1)  # Smoothing factor: 2/(span+1)

    
    for day in range(day_start, day_end+1):
        # Get all volumes for this day
        day_volumes = []
        for symbol in symbol_vol_data:
            if day in symbol_vol_data[symbol]:
                day_volumes.append((symbol, symbol_vol_data[symbol][day]))
        
        if not day_volumes:
            continue
        
        # Sort by volume (highest first for percentile calculation)
        day_volumes.sort(key=lambda x: x[1], reverse=True)
        
        # Calculate percentile rank (100 = highest volume)
        n_vol = len(day_volumes)
        for rank, (symbol, volume) in enumerate(day_volumes):
            percentile = 100 * (1 - rank / n_vol)  # 100 for highest, 0 for lowest
            symbol_name = symbol
            # Calculate EWMA for this symbol
            if symbol_name not in symbol_percentile_ewma:
                symbol_percentile_ewma[symbol_name] = percentile  # First value
            else:
                # EWMA formula: EWMA_t = alpha * value_t + (1-alpha) * EWMA_{t-1}
                symbol_percentile_ewma[symbol_name] = alpha * percentile + (1 - alpha) * symbol_percentile_ewma[symbol_name]
    
    return symbol_percentile_ewma




#----------------


if __name__ == "__main__":
    n = process_files()
    filter_ltp_range(symbol_vol_data)
    symbol_percentile_ewma_final = get_percentile_dict(1,n)

    symbol_percentile_ewma_update = get_percentile_dict(n-last_ndays+1,n)
    update_list = sorted(symbol_percentile_ewma_update.items(), key=lambda x: x[1], reverse=True)[:topk]

    for symb, perc in update_list:
        symbol_percentile_ewma_final[symb] = 100 +perc

    sorted_perc_symbs = sorted(symbol_percentile_ewma_final.items(), key=lambda x: x[1], reverse=True)
    merge_mode = 'a' if append_file else 'w'
    with open('out/merged.txt', merge_mode) as out1, open('out/candidates.txt', 'w') as out2:
        count=0
        for symbol, comb_val in sorted_perc_symbs:
            out1.write(f"{symbol},{int(comb_val)}\n")
            if count < top:
                out2.write(f"{symbol}\n")
                count+=1
                
    instr = 'Futures' if fo else 'Options'

    append_string = 'appended' if append_file else ''
    print(f"Processed {n} {instr} files. Last {last_ndays} days and top {topk}. Output: merged.txt {len(sorted_perc_symbs)} {append_string} candidates.txt {top}")
