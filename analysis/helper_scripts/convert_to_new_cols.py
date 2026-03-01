


import os
import csv

# Define the directory to search for CSV files
PARENT_DIR='../'  # analysis dir
NSE_DATA = f'{PARENT_DIR}/nse_data'
NSE_INTRA_DAY = f'{NSE_DATA}/intraday'

# Define the column mapping
column_map = [
    ('Symbol', 'symbol'),
    ('Volume (Contracts) - Futures', 'vol_cum'),
    ('Value (₹ Lakhs) - Futures', 'vol_val_cum'),
    ('Underlying', 'price'),
]

output_columns = [out for _, out in column_map]

def process_csv_file(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)
        filtered_rows = []
        for row in reader:
            # Only process row if all mapped columns are present
            filtered_row = {}
            for src, out in column_map:
                if src in row:
                    val = row[src]
                    if out == 'vol_val_cum':
                        try:
                            val = f"{float(val):.2f}"
                        except Exception:
                            pass
                    filtered_row[out] = val
            if len(filtered_row) == len(column_map):
                filtered_rows.append(filtered_row)

    # Overwrite the original file
    with open(file_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_columns)
        writer.writeheader()
        writer.writerows(filtered_rows)

def process_all_csv_files(base_dir):
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith(".csv"):
                file_path = os.path.join(root, file)
                process_csv_file(file_path)

if __name__ == "__main__":
    process_all_csv_files(NSE_INTRA_DAY)