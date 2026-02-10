import sys
import os

if len(sys.argv) != 2:
    print("Usage: python merge_wl.py <count>")
    sys.exit(1)

count = int(sys.argv[1])
all_symbols = set()

# Read all files: newwl1.txt, newwl2.txt, ..., newwl{count}.txt
for i in range(1, count + 1):
    filename = f'newwl{i}.txt'
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            symbols = {line.strip() for line in f if line.strip()}
            all_symbols.update(symbols)
    else:
        print(f"Warning: {filename} not found, skipping")

# Write unique sorted symbols to merged_wl.txt
unique_symbols = sorted(all_symbols)
with open('merged_wl.txt', 'w') as out:
    for symbol in unique_symbols:
        out.write(symbol + '\n')

print(f"Merged {len(unique_symbols)} unique symbols from {count} files")
