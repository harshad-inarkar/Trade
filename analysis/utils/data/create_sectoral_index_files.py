import argparse
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

from utils.data.paths import NSE_INDX_DATA


def out(msg: str = "", end: str = "\n") -> None:
    sys.stdout.write(f"{msg}{end}")


Path(NSE_INDX_DATA).mkdir(parents=True, exist_ok=True)

NIFTY_INDICES = [
    "NIFTY BANK",
    "NIFTY FINANCIAL SERVICES",
    "NIFTY MID SELECT",
    "NIFTY SMALLCAP 100",
    "NIFTY 50",
]


SECTORAL_INDICES = [
    "Nifty Auto",
    "Nifty Bank",
    "Nifty Cement",
    "Nifty Chemicals",
    "Nifty Financial Services",
    "Nifty Financial Services 25/50",
    "Nifty Financial Services Ex Bank",
    "Nifty FMCG",
    "Nifty Healthcare",
    "Nifty IT",
    "Nifty Media",
    "Nifty Metal",
    "Nifty Pharma",
    "Nifty Private Bank",
    "Nifty PSU Bank",
    "Nifty Realty",
    "Nifty Consumer Durables",
    "Nifty Oil and Gas",
    "Nifty500 Healthcare",
    "Nifty MidSmall Financial Services",
    "Nifty MidSmall Healthcare",
    "Nifty MidSmall IT & Telecom",
    "Nifty Capital Markets",
    "Nifty Commodities",
    "Nifty Conglomerate 50",
    "Nifty Core Housing",
    "Nifty CPSE",
    "Nifty Energy",
    "Nifty EV & New Age Automotive",
    "Nifty Housing",
    "Nifty100 ESG",
    "Nifty100 Enhanced ESG",
    "Nifty100 ESG Sector Leaders",
    "Nifty India Consumption",
    "Nifty India Defence",
    "Nifty India Digital",
    "Nifty India Infrastructure & Logistics",
    "Nifty India Internet",
    "Nifty India Manufacturing",
    "Nifty India New Age Consumption",
    "Nifty India Railways PSU",
    "Nifty India Tourism",
    "Nifty India Select 5 Corporate Groups (MAATR)",
    "Nifty Infrastructure",
    "Nifty IPO",
    "Nifty Midcap Liquid 15",
    "Nifty MidSmall India Consumption",
    "Nifty MNC",
    "Nifty Mobility",
    "Nifty PSE",
    "Nifty REITs & InvITs",
    "Nifty Rural",
    "Nifty Non-Cyclical Consumer",
    "Nifty Services Sector",
    "Nifty Shariah 25",
    "Nifty Transportation & Logistics",
    "Nifty100 Liquid 15",
    "Nifty50 Shariah",
    "Nifty500 Shariah",
    "Nifty500 Multicap India Manufacturing 50:30:20",
    "Nifty500 Multicap Infrastructure 50:30:20",
    "Nifty SME Emerge",
    "Nifty Waves",
]


def get_file_name_from_index(index_name: str) -> str:
    """
    Convert an index name to a clean filename format.

    Args:
        name: Original name string

    Returns:
        Cleaned filename with .csv extension
    """
    filename = index_name.lower()
    filename = filename.replace("&", "")
    filename = re.sub(r"[/\-\s]+", "_", filename)
    filename = re.sub(r"[^a-z0-9_]", "", filename)
    filename = re.sub(r"_+", "_", filename)
    filename = filename.strip("_")
    return filename + ".csv"


def download_sectoral_indices(index_list: list[str] | None = None) -> None:
    """Download all 21 sectoral indices to CSV files"""
    if index_list is None:
        index_list = SECTORAL_INDICES

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
    }

    out(f"Downloading {len(index_list)}  Files...")
    out("=" * 60)

    successful_downloads = 0
    failed_list = []
    for index_name in index_list:
        filename = f"{NSE_INDX_DATA}/{get_file_name_from_index(index_name)}"

        try:
            session = requests.Session()
            session.headers.update(headers)
            session.get(
                "https://www.nseindia.com/market-data/live-equity-market",
                headers=headers,
            )

            encoded_index = (
                index_name.upper()
                .replace(" ", "%20")
                .replace("/", "%2F")
                .replace("&", "%26")
                .replace("-", "%20")
            )
            url = f"https://www.nseindia.com/api/equity-stockIndices?index={encoded_index}"

            out(f"📊 Downloading {index_name}...")

            response = session.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()

            symbols = []
            if "data" in data and len(data["data"]) > 0:
                symbols = [
                    item.get("symbol", "").upper().strip()
                    for item in data["data"][1:]
                    if item.get("symbol")
                ]

            if symbols:
                df = pd.DataFrame({"Symbol": symbols})
                df.to_csv(filename, index=False)
                out(f"   ✓ {filename} ({len(symbols)} stocks)")
                successful_downloads += 1
            else:
                out(f"   ⚠️  No symbols found for {index_name}")
                failed_list.append(index_name)

        except requests.RequestException as e:
            err_msg = str(e)[:50]
            out(f"   ❌ Error: {index_name} - {err_msg}")
            failed_list.append(index_name)

        time.sleep(0.001)

    out("\n" + "=" * 60)
    out(f"✅ SUCCESS: {successful_downloads}/{len(index_list)} indices downloaded!")
    fail_str = "\n".join(failed_list)
    out(f"Failed List:\n{fail_str}")
    create_summary_report(index_list)


def create_summary_report(index_list: list[str] | None = None) -> None:
    """Create summary report of all downloaded files"""
    if index_list is None:
        index_list = SECTORAL_INDICES

    out("\n📋 SUMMARY REPORT:")
    total_stocks = 0

    for indx_name in index_list:
        filename = f"{NSE_INDX_DATA}/{get_file_name_from_index(indx_name)}"
        if Path(filename).exists():
            df = pd.read_csv(filename)
            count = len(df)
            total_stocks += count
            out(f"   {filename}: {count} stocks")

    out(f"\n🎯 GRAND TOTAL: {total_stocks} stocks across sectors")


if __name__ == "__main__":
    out("🏦 NSE INDICES DOWNLOADER")
    out("=" * 60)

    parser = argparse.ArgumentParser(description="Indices Downloader")
    parser.add_argument(
        "-ix",
        "--index",
        action="store_true",
        help="Nifty Broad Indices",
    )

    args, unknown = parser.parse_known_args()

    index_flag = False
    if args.index:
        index_flag = True

    if index_flag:
        out("\nDownload Nifty Broad Indices only")
        download_sectoral_indices(NIFTY_INDICES)
    else:
        out("\nDownload Sectoral Indices")
        download_sectoral_indices()
