import sys

if len(sys.argv) != 4:
    print("Usage: python dedup.py file1.txt final_cand.txt top")
    sys.exit(1)

file1 = sys.argv[1]
file2 = sys.argv[2]
top= int(sys.argv[3])

sym_perc = {}

count1 = 0
with open(file1, "r") as f:
    for line in f:
        line = line.strip()
        if line:
            rec,val = line.split(',')
            sym_perc[rec] = max(int(val),sym_perc.get(rec,0))
            count1+=1


sorted_symb = sorted(sym_perc.items(), key=lambda x: x[1], reverse=True)

print(f"deduplicate {file1} original count {count1}  unique count {len(sorted_symb)}. Final candidates count {top}")

with open(file1, "w") as out1, open(file2,'w') as out2:
    count =0
    for symbol, comb_val in sorted_symb:
        out1.write(f"{symbol},{int(comb_val)}\n")

        if count < top:
            out2.write(f"{symbol}\n")
            count+=1


print(f"Dedup output {file1} {file2}")

