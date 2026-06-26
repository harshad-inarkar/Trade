import csv
import sys
import time

from curl_cffi import requests

from utils.data.paths import HOLIDAYS_LIST_PATH
from utils.logging.log_utils import out, set_out_log_level


def gen_holidays_list() -> None:
    base_url = "https://www.nseindia.com"
    api_url = "https://www.nseindia.com/api/holiday-master?type=trading"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Use impersonate="chrome" to mimic a real browser's TLS fingerprint
    with requests.Session(impersonate="chrome") as session:
        out("Fetching cookies from homepage...")
        session.get(base_url, headers=headers, timeout=10)

        # Adding a small 2-second delay. Bots move instantly; humans don't.
        # This helps bypass rate-limiting firewalls.
        time.sleep(2)

        out("Fetching holiday data...")
        resp = session.get(api_url, headers=headers, timeout=10)

        try:
            holiday_data = resp.json()
        except (ValueError, OSError):
            out("Failed to decode JSON. NSE blocked the request. Here is the response:")
            out(
                resp.text[:500]
            )  # Prints the first 500 characters of the block page for debugging
            sys.exit()

    # Extract the Capital Market (Equities) segment
    holidays = holiday_data.get("CM", [])

    _column_map: list[tuple[str, str]] = [
        ("Sr_no", "Sr No."),
        ("tradingDate", "Date"),
        ("description", "Holiday"),
    ]

    _hol_list_cols: list[str] = [v_out for _, v_out in _column_map]

    if holidays:
        filtered_rows = []
        for row in holidays:
            filtered_row = {}
            for src, v_out in _column_map:
                if src in row:
                    filtered_row[v_out] = row[src]

            if len(filtered_row) == len(_column_map):
                filtered_rows.append(filtered_row)

        with HOLIDAYS_LIST_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_hol_list_cols)
            writer.writeheader()
            writer.writerows(filtered_rows)

        out(f"Success! Wrote {len(filtered_rows)} holidays to {HOLIDAYS_LIST_PATH}")
    else:
        out("No holidays data found for the CM segment.")


if __name__ == "__main__":
    set_out_log_level("critical")
    gen_holidays_list()
