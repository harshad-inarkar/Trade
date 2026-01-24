import csv
import sys
import math
import heapq

if len(sys.argv) != 6:
    print("Usage: python script.py <days_range> <topk> <top> <filter_ltp_range> <fo>")
    sys.exit(1)

days_list= sys.argv[1].split('-')
start_day = int(days_list[0])
end_day = int(days_list[1])

n = end_day - start_day +1


topk= int(sys.argv[2])
top= int(sys.argv[3])
ltp_list= sys.argv[4].split('-')
start_ltp = float(ltp_list[0])
end_ltp = float(ltp_list[1])
fo = True if sys.argv[5] == 'f' else False

value_col = 'Value (₹ Lakhs) - Futures' if  fo else 'Value (₹ Lakhs) - Options (Premium)'


files = [f"data/wl{i}.csv" for i in range(start_day, end_day + 1)]


symb_top_count = {}
symb_ltp = {}

for fname in files:
    heap = []
    with open(fname, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row['Symbol'].strip('"')
            ltp = float(row['Underlying'].strip('"'))
            symb_ltp[symbol] = ltp

            if value_col in row:
                value = float(row[value_col])
                if len(heap) < topk:
                    heapq.heappush(heap, (value,symbol))
                else:
                    if value > heap[0][0]:
                        heapq.heapreplace(heap, (value,symbol))


    for v, s in heap:
        symb_top_count[s] = symb_top_count.get(s,0) + 1
    

# filter out ltp range
sym_to_del = set()
for sym in symb_top_count.keys():
    ltp = symb_ltp[sym]
    if not(ltp >= start_ltp and ltp <= end_ltp):
        sym_to_del.add(sym)

for sym in sym_to_del:
    del symb_top_count[sym]

sorted_symbols = sorted(symb_top_count.items(), key=lambda x: x[1], reverse=True)


with open('out/merged_top.txt', 'w') as out:
    for symbol, comb_val in sorted_symbols:
        out.write(f"{symbol},{int(comb_val)}\n")



with open('out/candidates_top.txt', 'w') as out:
    count=0
    for symbol, comb_val in sorted_symbols:
        out.write(f"{symbol}\n") #,{int(comb_val)}\n")
        count+=1
        if count == top:
            break



instr = 'Futures' if fo else 'Options'
print(f"Processed {n} {instr} files . Output: merged.txt candidates.txt")
