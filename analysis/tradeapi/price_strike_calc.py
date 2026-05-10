import math
import tomllib  # Requires Python 3.11+
from typing import Optional
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration Loader
# ---------------------------------------------------------------------------
def load_config(filename: str = "price_config.toml") -> dict:
    """Loads exchange interval settings from a TOML configuration file."""
    config_path = Path(__file__).parent / filename
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "rb") as f:
        return tomllib.load(f)

# Load global config once when the module is imported
CONFIG = load_config()

NSE_INDEX_INTERVALS = CONFIG.get("nse_indices", {})
MCX_INTERVALS = CONFIG.get("mcx_commodities", {})
NSE_EQUITY_BANDS = CONFIG.get("nse_equity_bands", [])
NSE_FALLBACK_INTERVAL = CONFIG.get("nse_equity_defaults", {}).get("fallback_interval", 100)

# ---------------------------------------------------------------------------
# Strike interval registry logic
# ---------------------------------------------------------------------------
def _nse_stock_interval(price: float) -> int:
    """
    NSE equity option strike intervals by price band.
    Iterates dynamically through bounds defined in TOML.
    """
    for band in NSE_EQUITY_BANDS:
        if price <= band["max_price"]:
            return band["interval"]
            
    # If price exceeds all defined bands, return fallback
    return NSE_FALLBACK_INTERVAL

def get_strike_interval(symbol: str, price: float) -> int:
    """
    Return the standard exchange strike interval for *symbol* at *price*.

    Lookup order:
        1. NSE index exact match
        2. MCX commodity exact match
        3. Fall back to NSE stock price-band rule
    """
    sym = symbol.strip().upper()
    
    if sym in NSE_INDEX_INTERVALS:
        return NSE_INDEX_INTERVALS[sym]
    if sym in MCX_INTERVALS:
        return MCX_INTERVALS[sym]
        
    # Default: treat as NSE equity option
    return _nse_stock_interval(price)

def get_strike_price_full_data(
    symbol: str,
    entry_price: float,
    signal: str,
    custom_interval: Optional[int] = None,
) -> dict:
    """
    Calculate the naked option strike price.
    """
    if entry_price <= 0:
        raise ValueError(f"entry_price must be > 0, got {entry_price}")

    sig = signal.strip().upper()
    if sig not in ("BUY", "SELL"):
        raise ValueError(f"signal must be 'BUY' or 'SELL', got {signal!r}")

    interval = custom_interval if custom_interval else get_strike_interval(symbol, entry_price)

    if sig == "BUY":
        # Floor -> nearest strike AT or BELOW entry_price
        strike = int(math.floor(entry_price / interval) * interval)
        option_type = "CALL"
    else:  # SELL
        # Ceil -> nearest strike AT or ABOVE entry_price
        strike = int(math.ceil(entry_price / interval) * interval)
        option_type = "PUT"

    return {
        "symbol":      symbol.upper(),
        "entry_price": entry_price,
        "signal":      sig,
        "interval":    interval,
        "strike":      strike,
        "option_type": option_type,
    }

# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------
def get_price_strike(symbol: str, price: float, signal: str) -> int:
    """Thin wrapper; returns just the strike integer."""
    return get_strike_price_full_data(symbol, price, signal)["strike"]

# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_cases = [
        # (symbol,         price,    signal,  expected_strike)
        ("SBIN",           1073,    "BUY",    1060),   # stock, step=20, floor
        ("SBIN",           1028,    "SELL",   1040),   # stock, step=20, ceil
        ("NIFTY",         22475,    "BUY",   22450),   # index, step=50, floor
        ("NIFTY",         22515,    "SELL",  22550),   # index, step=50, ceil
        ("BANKNIFTY",     48320,    "BUY",   48300),   # index, step=100, floor
        ("BANKNIFTY",     48350,    "SELL",  48400),   # index, step=100, ceil
        ("GOLD",          71540,    "BUY",   71500),   # MCX, step=100, floor
        ("GOLD",          71560,    "SELL",  71600),   # MCX, step=100, ceil
        ("SILVER",        85350,    "BUY",   85300),   # MCX, step=100, floor
        ("CRUDEOIL",       6475,    "BUY",    6450),   # MCX, step=50, floor
        ("CRUDEOIL",       6525,    "SELL",   6550),   # MCX, step=50, ceil
        ("NATURALGAS",      175,    "BUY",     170),   # MCX, step=10, floor
        ("NATURALGAS",      182,    "SELL",    190),   # MCX, step=10, ceil
        ("RELIANCE",       2870,    "BUY",    2850),   # stock >2500, step=50
        ("RELIANCE",       2910,    "SELL",   2950),   # stock >2500, step=50
        ("INFY",           1640,    "BUY",    1640),   # stock >1000, step=20, exact
        ("TCS",            3750,    "BUY",    3750),   # stock >2500, step=50, exact
        ("HDFCBANK",       1725,    "SELL",   1740),   # stock >1000, step=20
    ]

    print(f"\n{'Symbol':<14}{'Price':>8}{'Signal':>7}{'Step':>6}{'Strike':>8}{'Expected':>10}{'Pass':>6}")
    print("─" * 62)
    all_pass = True
    for sym, price, sig, expected in test_cases:
        result = get_strike_price_full_data(sym, price, sig)
        strike   = result["strike"]
        interval = result["interval"]
        passed   = strike == expected
        all_pass = all_pass and passed
        status   = "✓" if passed else f"✗ (got {strike})"
        print(f"{sym:<14}{price:>8}{sig:>7}{interval:>6}{strike:>8}{expected:>10}   {status}")

    print("─" * 62)
    print("All tests passed ✓" if all_pass else "Some tests FAILED ✗")