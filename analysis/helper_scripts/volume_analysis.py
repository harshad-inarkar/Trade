import pandas as pd
import numpy as np
import os

# Configuration
NUM_DAYS = 21
CONSISTENCY_THRESHOLD = 0.7  # 70% of average volume
STABILITY_RANGE = 0.3  # Within ±30% of average

# Step 1: Read all volume data files
print("Reading volume data files...")
print("=" * 60)

all_data = []
for day in range(1, NUM_DAYS + 1):
    filename = f'wl{day}.csv'
    try:
        df = pd.read_csv(filename)
        # Extract symbol and volume columns
        df_subset = df[['Symbol', 'Volume']].copy()
        df_subset['Day'] = day
        all_data.append(df_subset)
        print(f"  {filename}: {len(df_subset)} symbols loaded")
    except FileNotFoundError:
        print(f"  WARNING: {filename} not found, skipping...")
    except KeyError as e:
        print(f"  ERROR: {filename} - Column not found: {e}")

if not all_data:
    print("\nERROR: No data files found!")
    exit(1)

# Step 2: Combine all data
print(f"\nCombining data from {len(all_data)} files...")
combined_df = pd.concat(all_data, ignore_index=True)
print(f"Total records: {len(combined_df)}")
print(f"Unique symbols: {combined_df['Symbol'].nunique()}")

# Step 3: Calculate metrics for each symbol
print("\nCalculating volume metrics...")
print("=" * 60)

results = []

# Group by symbol
for symbol in combined_df['Symbol'].unique():
    symbol_data = combined_df[combined_df['Symbol'] == symbol].copy()
    volumes = symbol_data['Volume'].values

    # Skip if insufficient data
    if len(volumes) < 5:
        continue

    # 1. Average Daily Volume
    avg_volume = np.mean(volumes)

    # 2. Standard Deviation
    std_volume = np.std(volumes, ddof=1)

    # 3. Coefficient of Variation (CV)
    if avg_volume > 0:
        cv = (std_volume / avg_volume) * 100
    else:
        cv = np.nan

    # 4. Consistency Ratio
    # Count days where volume >= threshold * average
    threshold_volume = CONSISTENCY_THRESHOLD * avg_volume
    days_above_threshold = np.sum(volumes >= threshold_volume)
    consistency_ratio = (days_above_threshold / len(volumes)) * 100

    # 5. Volume Stability Score
    # Count days where volume is within ±30% of average
    lower_bound = avg_volume * (1 - STABILITY_RANGE)
    upper_bound = avg_volume * (1 + STABILITY_RANGE)
    days_stable = np.sum((volumes >= lower_bound) & (volumes <= upper_bound))
    stability_score = (days_stable / len(volumes)) * 100

    # 6. Additional metrics
    min_volume = np.min(volumes)
    max_volume = np.max(volumes)
    median_volume = np.median(volumes)

    # Store results
    results.append({
        'Symbol': symbol,
        'Average_Volume': avg_volume,
        'Std_Deviation': std_volume,
        'Coefficient_of_Variation': cv,
        'Consistency_Ratio': consistency_ratio,
        'Volume_Stability_Score': stability_score,
        'Min_Volume': min_volume,
        'Max_Volume': max_volume,
        'Median_Volume': median_volume,
        'Days_with_Data': len(volumes)
    })

# Step 4: Create results dataframe
results_df = pd.DataFrame(results)

# Sort by Coefficient of Variation (ascending - lower is better)
results_df = results_df.sort_values('Coefficient_of_Variation', ascending=True)

print(f"\nAnalysis complete for {len(results_df)} symbols")

# Step 5: Display summary statistics
print("\n" + "=" * 60)
print("SUMMARY STATISTICS")
print("=" * 60)
print(f"Average CV across all symbols: {results_df['Coefficient_of_Variation'].mean():.2f}%")
print(f"Average Consistency Ratio: {results_df['Consistency_Ratio'].mean():.2f}%")
print(f"Average Stability Score: {results_df['Volume_Stability_Score'].mean():.2f}%")

# Step 6: Identify top performers
print("\n" + "=" * 60)
print("TOP 10 MOST CONSISTENT STOCKS (Lowest CV)")
print("=" * 60)
top_10 = results_df.head(10)[['Symbol', 'Coefficient_of_Variation', 
                                'Consistency_Ratio', 'Volume_Stability_Score', 
                                'Average_Volume']]
print(top_10.to_string(index=False))

# Step 7: Filter stocks with good metrics
print("\n" + "=" * 60)
print("FILTERING CRITERIA")
print("=" * 60)
print(f"  - Coefficient of Variation < 50%")
print(f"  - Consistency Ratio > 70%")
print(f"  - Stability Score > 60%")

filtered_df = results_df[
    (results_df['Coefficient_of_Variation'] < 50) &
    (results_df['Consistency_Ratio'] > 70) &
    (results_df['Volume_Stability_Score'] > 60)
]

print(f"\nStocks meeting criteria: {len(filtered_df)}")

if len(filtered_df) > 0:
    print("\nTop filtered stocks:")
    print(filtered_df.head(15)[['Symbol', 'Coefficient_of_Variation', 
                                 'Consistency_Ratio', 'Volume_Stability_Score', 
                                 'Average_Volume']].to_string(index=False))

# Step 8: Save results to CSV files
output_file = 'volume_analysis_results.csv'
filtered_file = 'volume_analysis_filtered.csv'

results_df.to_csv(output_file, index=False)
print(f"\n" + "=" * 60)
print(f"Full results saved to: {output_file}")

if len(filtered_df) > 0:
    filtered_df.to_csv(filtered_file, index=False)
    print(f"Filtered results saved to: {filtered_file}")

print("=" * 60)
print("\nANALYSIS COMPLETE!")
print("=" * 60)

# Display column descriptions
print("\nCOLUMN DESCRIPTIONS:")
print("-" * 60)
print("Symbol: Stock symbol")
print("Average_Volume: Mean volume over all days")
print("Std_Deviation: Standard deviation of volume")
print("Coefficient_of_Variation: (Std Dev / Mean) × 100")
print("                          Lower = More consistent")
print("Consistency_Ratio: % of days with volume ≥ 70% of average")
print("Volume_Stability_Score: % of days within ±30% of average")
print("Min_Volume: Minimum volume observed")
print("Max_Volume: Maximum volume observed")
print("Median_Volume: Median volume")
print("Days_with_Data: Number of days with data for this symbol")
