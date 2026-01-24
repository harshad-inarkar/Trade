import csv

# Lambda parameter for EWMA (0.94 is common for emphasizing recent data)
lambda_decay = 0.94

# Dictionary to store volumes for each symbol across days
# Structure: {symbol: [vol_day1, vol_day2, ..., vol_day30]}
symbol_volumes = {}

# Read all 30 CSV files
for i in range(1, 31):
    filename = f'wl{i}.csv'
    try:
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                symbol = row['symbol']
                volume = float(row['volume'])
                
                if symbol not in symbol_volumes:
                    symbol_volumes[symbol] = []
                symbol_volumes[symbol].append(volume)
    except FileNotFoundError:
        print(f"Warning: {filename} not found")

# Calculate EWMA for each symbol
ewma_results = {}

for symbol, volumes in symbol_volumes.items():
    if len(volumes) > 0:
        ewma = volumes[0]
        for vol in volumes[1:]:
            ewma = lambda_decay * ewma + (1 - lambda_decay) * vol
        ewma_results[symbol] = ewma

# Write results to CSV
if ewma_results:
    with open('volume_ewma_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['symbol', 'ewma_volume'])
        for symbol, ewma in sorted(ewma_results.items()):
            writer.writerow([symbol, ewma])
    
    print("EWMA calculation complete!")
    print(f"\nTotal symbols processed: {len(ewma_results)}")
    print(f"\nFirst 10 symbols:")
    for i, (symbol, ewma) in enumerate(sorted(ewma_results.items())[:10]):
        print(f"{symbol}: {ewma:.2f}")
    print(f"\nOutput saved to: volume_ewma_output.csv")
else:
    print("No data found in CSV files")
