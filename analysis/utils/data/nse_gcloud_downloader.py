#!/usr/bin/env python3
"""
NSE Intraday Data Downloader - Cloud Function Gen 2
Triggered by Cloud Scheduler every 3 mins, 09:15-15:30 IST, Mon-Fri
Uploads directly to GCS bucket (no rclone, no GDrive)
"""

import contextlib
import csv
import time
from datetime import datetime
from io import StringIO
from typing import Any

import functions_framework
import requests
from google.cloud import storage

from utils.utility import INDIA_TZ, out

# ── GCS config ────────────────────────────────────────────────────────────────
GCS_BUCKET = "nse-data-bucket"
NSE_DATA_DIR = "nse_data"
NSE_INTRA_DAY = "intraday"
GCS_PREFIX = f"{NSE_DATA_DIR}/{NSE_INTRA_DAY}"

# ── Session config ─────────────────────────────────────────────────────────────
start_session = "0915"
end_session = "1530"

SESSION_URL = "https://www.nseindia.com"
REFER_URL = "https://www.nseindia.com/market-data/most-active-underlying"
API_URL = "https://www.nseindia.com/api/live-analysis-most-active-underlying?csv=true"

# ── Column mapping ─────────────────────────────────────────────────────────────
column_map = [
    ("Symbol", "symbol"),
    ("Volume (Contracts) - Futures", "vol_cum"),
    ("Value (₹ Lakhs) - Futures", "vol_val_cum"),
    ("Underlying", "price"),
]
output_columns = [v_out for _, v_out in column_map]

# ── Module-level singletons (survive warm invocations) ────────────────────────
_cache: dict[str, Any] = {"http_session": None, "gcs_client": None}


def get_http_session() -> requests.Session:
    if _cache["http_session"] is None:
        out("Cold: initialising HTTP session")
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                ),
                "Referer": REFER_URL,
            },
        )
        s.get(SESSION_URL, timeout=10)
        _cache["http_session"] = s
    return _cache["http_session"]


def get_gcs_client() -> storage.Client:
    if _cache["gcs_client"] is None:
        out("Cold: initialising GCS client")
        _cache["gcs_client"] = storage.Client()
    return _cache["gcs_client"]


# ── Session window helpers ─────────────────────────────────────────────────────
def calculate_intervals(
    tf: int = 1, start_time_str: str = start_session, end_time_str: str = end_session
) -> float:
    start = datetime.strptime(start_time_str, "%H%M").replace(tzinfo=INDIA_TZ)
    end = datetime.strptime(end_time_str, "%H%M").replace(tzinfo=INDIA_TZ)
    if start >= end:
        return 0.0
    return (end - start).total_seconds() / 60 // tf


def check_valid_session(curr_time: str) -> tuple[bool, str]:
    interval = calculate_intervals(end_time_str=curr_time)
    valid_flag = interval > 0
    new_ts = curr_time
    if valid_flag and interval > calculate_intervals():
        out(f"Timestamp {curr_time} past end session {end_session}, clamping")
        new_ts = end_session
    return valid_flag, new_ts


# ── Core download + upload ─────────────────────────────────────────────────────
def download_nse_data() -> None:
    t0 = time.time()
    nowdt = datetime.now(INDIA_TZ)

    date_str = nowdt.strftime("%d%m%Y")
    timestamp = nowdt.strftime("%H%M")

    valid_flag, timestamp = check_valid_session(timestamp)
    if not valid_flag:
        out(f"Outside session window ({timestamp}), skipping")
        return

    # ── 1. Download ────────────────────────────────────────────────────────────
    t1 = time.time()
    session = get_http_session()
    response = session.get(API_URL, timeout=30)
    response.raise_for_status()

    # ── 2. Parse + filter CSV ──────────────────────────────────────────────────
    content_str = response.content.decode("utf-8-sig")
    reader = csv.DictReader(StringIO(content_str))
    filtered_rows = []

    for row in reader:
        filtered_row = {}
        for src, v_out in column_map:
            if src in row:
                val = row[src]
                if v_out == "vol_val_cum":
                    with contextlib.suppress(ValueError, TypeError):
                        val = f"{float(val):.2f}"
                filtered_row[v_out] = val
        if len(filtered_row) == len(column_map):
            filtered_rows.append(filtered_row)

    # ── 3. Serialise to in-memory CSV (no disk write) ──────────────────────────
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=output_columns)
    writer.writeheader()
    writer.writerows(filtered_rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    out(f"NSE download: {time.time() - t1:.2f}s  ({len(response.content)} bytes)")

    # ── 4. Upload to GCS ───────────────────────────────────────────────────────
    filename = f"nse_data_{timestamp}.csv"
    blob_path = f"{GCS_PREFIX}/{date_str}/{filename}"

    t2 = time.time()
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_path)

    if blob.exists():
        out(f"Already exists. Replacing : {blob_path}")

    blob.upload_from_string(csv_bytes, content_type="text/csv")
    out(f"GCS upload : {time.time() - t2:.2f}s  → gs://{GCS_BUCKET}/{blob_path}")
    out(f"Total      : {time.time() - t0:.2f}s")


# ── Cloud Function entrypoint ──────────────────────────────────────────────────
@functions_framework.http
def run_job(request: Any) -> tuple[str, int]:  # noqa: ARG001
    """HTTP-triggered Cloud Function. Called by Cloud Scheduler."""
    try:
        download_nse_data()
    except Exception as e:  # noqa: BLE001
        out(f"ERROR: {e}")
        return str(e), 500
    else:
        return "OK", 200


# ── Local dev entrypoint ───────────────────────────────────────────────────────
if __name__ == "__main__":
    download_nse_data()
