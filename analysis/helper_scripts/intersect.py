# intersect.py
import sys

if len(sys.argv) != 3:
    print("Usage: python intersect.py file1.txt file2.txt file3.txt")
    sys.exit(1)

file1, file2 = sys.argv[1], sys.argv[2]

with open(file1, "r") as f:
    s1 = {line.strip() for line in f if line.strip()}

with open(file2, "r") as f:
    s2 = {line.strip() for line in f if line.strip()}



intersection = sorted(s1 & s2)

with open("inter.txt", "w") as out:
    for sym in intersection:
        out.write(sym + "\n")
