import sys

if len(sys.argv) != 2:
    print("Usage: python dedup.py file1.txt")
    sys.exit(1)

file1 = sys.argv[1]

sym_perc = {}

with open(file1, "r") as f:
    for line in f:
        line = line.strip()
        if line:
            rec,val = line.split(',')
            sym_perc[rec] = max(int(val),sym_perc.get(rec,0))


sorted_symb = sorted(sym_perc.items(), key=lambda x: x[1], reverse=True)

with open(f'{file1}1', "w") as out:
    for symbol, comb_val in sorted_symb:
        out.write(f"{symbol},{int(comb_val)}\n")
