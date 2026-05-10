import pandas as pd
import sys

# Get command line arguments
if len(sys.argv) != 4:
    print("Usage: python convert_wl.py <count> <LTP_threshold> <Value_threshold>")
    print("Example: python convert_wl.py wl1.csv 500 1000")
    sys.exit(1)

count = int(sys.argv[1])
ltp_threshold = float(sys.argv[2])
value_threshold = float(sys.argv[3])

# Read the CSV file

for i in range(1,count+1):
    df = pd.read_csv(f'wl{i}.csv')

    # Filter: LTP >= threshold and Value (in Cr.) >= threshold
    filtered_df = df[(df['Underlying'] >= ltp_threshold) & (df['Value (₹ Lakhs) - Options (Premium)'] >= value_threshold)]

    # Sort by Value (in Cr.) descending and select only Name column
    sorted_names = filtered_df.sort_values('Value (₹ Lakhs) - Options (Premium)', ascending=False)['Symbol'].reset_index(drop=True)

    # Overwrite the input file with just the sorted names (one per line)
    sorted_names.to_csv(f'newwl{i}.txt', index=False, header=False)

    print(f"File written with {len(sorted_names)} sorted names (LTP >= {ltp_threshold}, Value >= {value_threshold} Lakhs).")
