import csv
import sys
import math

if len(sys.argv) != 7:
    print("Usage: python script.py <day_range> <top> <max_wt> <avg_wt> <filter_ltp_range> <fo>")
    sys.exit(1)

days_list= sys.argv[1].split('-')
start_day = int(days_list[0])
end_day = int(days_list[1])

n = end_day - start_day +1

top= int(sys.argv[2])
max_wt=float(sys.argv[3])
avg_wt = float(sys.argv[4])
ltp_list= sys.argv[5].split('-')
start_ltp = float(ltp_list[0])
end_ltp = float(ltp_list[1])
fo = True if sys.argv[6] == 'f' else False

value_col = 'Value (₹ Lakhs) - Futures' if  fo else 'Value (₹ Lakhs) - Options (Premium)'


files = [f"data/wl{i}.csv" for i in range(start_day, end_day + 1)]


lambda_decay = 0.94

span = 10
alpha = 2 / (span + 1)  # Smoothing factor: 2/(span+1)

totals = {}
symb_ewma = {}
symb_max = {}
symb_comb = {}
symb_ltp = {}

symbol_percentile_ewma = {}
symbol_vol_data = {}


day = 1
for fname in files:
    with open(fname, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row['Symbol'].strip('"')
            symb_ltp[symbol] = float(row['Underlying'].strip('"'))
            
            if value_col in row:
                value = float(row[value_col])
                max_val = max(symb_max.get(symbol, 0.0), value)
                ewma = symb_ewma.get(symbol)
                if ewma is None:
                    symb_ewma[symbol] = value
                else:
                    symb_ewma[symbol] = lambda_decay * ewma + (1 - lambda_decay) * value


                totals[symbol] = totals.get(symbol, 0.0) + value
                symb_max[symbol] = max_val
                vol_dict = symbol_vol_data.setdefault(symbol,{})
                vol_dict[day] = value
    day+=1



# filter out ltp range
sym_to_del = set()
for sym in symbol_vol_data.keys():
    ltp = symb_ltp[sym]
    if not(ltp >= start_ltp and ltp <= end_ltp):
        sym_to_del.add(sym)

for sym in sym_to_del:
    del symbol_vol_data[sym]



for symbol, max_val in symb_max.items():
     #symb_comb[symbol] = (max_wt *max_val)+ (avg_wt * (totals[symbol]/n))
     avg_vol = totals[symbol]/n
     symb_comb[symbol] = (max_wt *max_val) + (avg_wt * avg_vol)


# calc percentile ewma

for day in range(1, n+1):
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




sorted_comb_symbols = sorted(symbol_percentile_ewma.items(), key=lambda x: x[1], reverse=True)
use_top_cand_list= False


with open('out/merged.txt', 'w') as out:
    for symbol, comb_val in sorted_comb_symbols:
        out.write(f"{symbol},{int(comb_val)}\n")


s1 = set()
if use_top_cand_list:
    with open('out/candidates_top.txt', "r") as f:
        s1 = {line.strip() for line in f if line.strip()}


with open('out/candidates.txt', 'w') as out:
    count=0
    for symbol, comb_val in sorted_comb_symbols:
        if (not use_top_cand_list) or symbol in s1:
            out.write(f"{symbol}\n") #,{int(comb_val)}\n")
            count+=1
            if count == top:
                break





instr = 'Futures' if fo else 'Options'
print(f"Processed {n} {instr} files . Output: merged.txt candidates.txt")
