import csv
import sys
import math

if len(sys.argv) != 7:
    print("Usage: python script.py <count> <top> <max_wt> <avg_wt> <filter_ltp_range> <fo>")
    sys.exit(1)

n = int(sys.argv[1])
top= int(sys.argv[2])
max_wt=float(sys.argv[3])
avg_wt = float(sys.argv[4])
ltp_list= sys.argv[5].split('-')
start_ltp = float(ltp_list[0])
end_ltp = float(ltp_list[1])
fo = True if sys.argv[6] == 'f' else False

files = [f"wl{i}.csv" for i in range(1, n + 1)]

totals = {}
symb_max = {}
symb_comb = {}

for fname in files:
    with open(fname, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row['Symbol'].strip('"')
            value_col = 'Value (₹ Lakhs) - Futures' if  fo else 'Value (₹ Lakhs) - Options (Premium)'
            ltp = float(row['Underlying'].strip('"'))
            if ltp >= start_ltp and ltp <= end_ltp:
                if value_col in row:
                    value = float(row[value_col])
                    max_val = max(symb_max.get(symbol, 0.0), value)
                    totals[symbol] = totals.get(symbol, 0.0) + value
                    symb_max[symbol] = max_val



for symbol, max_val in symb_max.items():
     #symb_comb[symbol] = (max_wt *max_val)+ (avg_wt * (totals[symbol]/n))
     avg_vol = totals[symbol]/n
     symb_comb[symbol] = (max_wt *max_val) + (avg_wt * avg_vol)



sorted_comb_symbols = sorted(symb_comb.items(), key=lambda x: x[1], reverse=True)


with open('merged.txt', 'w') as out:
    for symbol, comb_val in sorted_comb_symbols:
        out.write(f"{symbol},{int(comb_val)}\n")


with open('G.txt', "r") as f:
    first_line= f.readline()
    s1= {symb_str.split(':')[1] if ':' in symb_str else symb_str for symb_str in first_line.split(',')}
    #s1 = {line.strip() for line in f if line.strip()}


with open('candidates.txt', 'w') as out:
    count=0
    for symbol, comb_val in sorted_comb_symbols:
        if symbol in s1:
            out.write(f"{symbol}\n") #,{int(comb_val)}\n")
            count+=1
            if count == top:
                break



instr = 'Futures' if fo else 'Options'
print(f"Processed {n} {instr} files . Output: merged.txt candidates.txt")
