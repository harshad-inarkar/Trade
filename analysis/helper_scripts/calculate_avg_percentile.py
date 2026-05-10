import csv
import os
from collections import defaultdict

# Dictionary to store all volume data: {symbol: {day: volume}}
symbol_data = defaultdict(dict)

# Read all 30 CSV files
for i in range(1, 31):
    filename = f'wl{i}.csv'
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row['symbol']
                volume = float(row['volume'])
                symbol_data[symbol][i] = volume
        print(f"Loaded {filename}")
    else:
        print(f"Warning: {filename} not found")

if not symbol_data:
    print("Error: No data loaded")
    exit()

print(f"Total unique symbols: {len(symbol_data)}")

# EWMA parameters (span=10 gives reasonable decay)
alpha = 2 / (10 + 1)  # Smoothing factor: 2/(span+1)

# Calculate percentile for each symbol for each day and compute EWMA
symbol_ewma = {}

for day in range(1, 31):
    # Get all volumes for this day
    day_volumes = []
    for symbol in symbol_data:
        if day in symbol_data[symbol]:
            day_volumes.append((symbol, symbol_data[symbol][day]))
    
    if not day_volumes:
        continue
    
    # Sort by volume (highest first for percentile calculation)
    day_volumes.sort(key=lambda x: x[1], reverse=True)
    
    # Calculate percentile rank (100 = highest volume)
    n = len(day_volumes)
    for rank, (symbol, volume) in enumerate(day_volumes):
        percentile = 100 * (1 - rank / n)  # 100 for highest, 0 for lowest
        symbol_name = symbol
        
        # Calculate EWMA for this symbol
        if symbol_name not in symbol_ewma:
            symbol_ewma[symbol_name] = percentile  # First value
        else:
            # EWMA formula: EWMA_t = alpha * value_t + (1-alpha) * EWMA_{t-1}
            symbol_ewma[symbol_name] = alpha * percentile + (1 - alpha) * symbol_ewma[symbol_name]

# Prepare results
results = []
for symbol, ewma_value in symbol_ewma.items():
    if symbol in symbol_data:
        volumes = list(symbol_data[symbol].values())
        avg_volume = sum(volumes) / len(volumes)
        results.append({
            'symbol': symbol,
            'ewma_percentile': ewma_value,
            'avg_volume': avg_volume
        })

# Sort by EWMA percentile (descending)
results.sort(key=lambda x: x['ewma_percentile'], reverse=True)

# Display top 10
print("\nTop 10 symbols by EWMA percentile:")
for i, item in enumerate(results[:10], 1):
    print(f"{i}. {item['symbol']}: {item['ewma_percentile']:.2f}%")

# Save to CSV
output_file = 'ewma_percentile_results.csv'
with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['symbol', 'ewma_percentile', 'avg_volume'])
    writer.writeheader()
    writer.writerows(results)

print(f"\nResults saved to {output_file}")
print(f"Total symbols processed: {len(results)}")
