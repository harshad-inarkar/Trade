#!/usr/bin/env python3
"""
clear_superorders.py — Auto-cancels orphaned Dhan Super Orders.
Object-Oriented, Proxy-Aware, and continuous execution ready.
"""

import argparse
import sys

from tradeapi.dhan_trade import DhanTrader
from utils.utility import wait_next_wall_clock


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Execution Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    parser = argparse.ArgumentParser(description="Dhan Orphaned Super Order Cleaner")
    parser.add_argument(
        "-ri",
        "--reload-interval",
        type=int,
        default=15,
        help="Run continuously with this interval in minutes.",
    )
    args = parser.parse_args()

    trader = DhanTrader()
    trader.begin_session()

    # Run cleanup independently
    trader.clean_orphaned_orders()

    if args.reload_interval > 0:
        print(f"Order Cleaner active. Monitoring every {args.reload_interval} minutes.")
        buffer_seconds = 5

        while True:
            wait_next_wall_clock(args.reload_interval, buffer_seconds)
            trader.clean_orphaned_orders()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting Order Cleaner.")
        sys.exit(0)
