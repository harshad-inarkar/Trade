# Hardcoded 30 days of closing prices
prices = {
    '2026-01-06': 150.0, '2026-01-07': 152.5, '2026-01-08': 151.2, '2026-01-09': 154.0,
    '2026-01-10': 156.8, '2026-01-13': 155.3, '2026-01-14': 157.1, '2026-01-15': 159.2,
    '2026-01-16': 161.0, '2026-01-17': 160.5, '2026-01-20': 162.3, '2026-01-21': 164.1,
    '2026-01-22': 163.8, '2026-01-23': 166.2, '2026-01-24': 168.0, '2026-01-27': 167.5,
    '2026-01-28': 169.3, '2026-01-29': 171.0, '2026-01-30': 170.8, '2026-02-02': 172.5,
    '2026-02-03': 174.2, '2026-02-04': 173.9, '2026-02-05': 176.1, '2026-02-06': 178.0,
    '2026-02-09': 177.5, '2026-02-10': 179.2, '2026-02-11': 181.0, '2026-02-12': 180.8
}

price_list = list(prices.values())
dates = list(prices.keys())

def calculate_running_sma(prices, period=8):
    """Running SMA: average of available data up to current day (min 1 day)"""
    sma = []
    for i in range(len(prices)):
        # Use all data from day 0 to current day i, up to max period
        window_size = min(i + 1, period)
        window_start = max(0, i - period + 1)
        window = price_list[window_start:i+1]
        sma.append(sum(window) / len(window))
    return sma

def calculate_ema(prices, period=8):
    """Exponential Moving Average (unchanged)"""
    ema = []
    multiplier = 2 / (period + 1)
    
    # First EMA = first price
    ema.append(prices[0])
    
    # Rest use EMA formula
    for i in range(1, len(prices)):
        current_ema = (prices[i] * multiplier) + (ema[-1] * (1 - multiplier))
        ema.append(current_ema)
    return ema

# Calculate indicators
sma_8 = calculate_running_sma(price_list, 8)
ema_8 = calculate_ema(price_list, 8)

# Print first 10 + last 10 days comparison
print("Day\t\tClose\t8 SMA\t8 EMA")
print("-" * 35)
for i in range(10):
    print(f"{dates[i]}\t{price_list[i]:6.1f}\t{sma_8[i]:5.1f}\t{ema_8[i]:5.1f}")
print("...")
for i in range(-10, 0):
    print(f"{dates[i]}\t{price_list[i]:6.1f}\t{sma_8[i]:5.1f}\t{ema_8[i]:5.1f}")
