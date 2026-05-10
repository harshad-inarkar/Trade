#!/usr/bin/env python3
"""
NSE Intraday Data Downloader — Cloud Function Gen 2
Triggered by Cloud Scheduler every 3 mins, 09:15–15:30 IST, Mon–Fri
Uploads directly to GCS bucket (no rclone, no GDrive)
"""

import csv
import os
import time
from datetime import datetime
from io import StringIO

import functions_framework
import pytz
import requests
from google.cloud import storage

# ── Timezone ──────────────────────────────────────────────────────────────────
india_tz = pytz.timezone('Asia/Kolkata')

# ── GCS config ────────────────────────────────────────────────────────────────
GCS_BUCKET  = 'nse-data-bucket'
NSE_DATA_DIR='nse_data'
NSE_INTRA_DAY = 'intraday'
GCS_PREFIX  = f'{NSE_DATA_DIR}/{NSE_INTRA_DAY}'

# ── Session config ─────────────────────────────────────────────────────────────
start_session = '0915'
end_session   = '1530'

SESSION_URL = 'https://www.nseindia.com'
REFER_URL   = 'https://www.nseindia.com/market-data/most-active-underlying'
API_URL     = 'https://www.nseindia.com/api/live-analysis-most-active-underlying?csv=true'

# ── Column mapping ─────────────────────────────────────────────────────────────
column_map = [
    ('Symbol',                    'symbol'),
    ('Volume (Contracts) - Futures', 'vol_cum'),
    ('Value (₹ Lakhs) - Futures', 'vol_val_cum'),
    ('Underlying',                'price'),
]
output_columns = [out for _, out in column_map]

# ── Module-level singletons (survive warm invocations) ────────────────────────
# HTTP session: reused across warm calls — skips NSE cookie handshake each time
_http_session: requests.Session | None = None

# GCS client: reuses auth token across warm calls
_gcs_client: storage.Client | None = None


def get_http_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        print('Cold: initialising HTTP session')
        s = requests.Session()
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer':    REFER_URL,
        })
        s.get(SESSION_URL, timeout=10)   # establish NSE session cookie
        _http_session = s
    return _http_session


def get_gcs_client() -> storage.Client:
    global _gcs_client
    if _gcs_client is None:
        print('Cold: initialising GCS client')
        _gcs_client = storage.Client()   # uses Cloud Function service account automatically
    return _gcs_client


# ── Session window helpers ─────────────────────────────────────────────────────
def calculate_intervals(tf=1, start_time_str=start_session, end_time_str=end_session):
    start = datetime.strptime(start_time_str, '%H%M')
    end   = datetime.strptime(end_time_str,   '%H%M')
    if start >= end:
        return 0
    return (end - start).total_seconds() / 60 // tf


def check_valid_session(curr_time: str) -> tuple[bool, str]:
    interval   = calculate_intervals(end_time_str=curr_time)
    valid_flag = interval > 0
    new_ts     = curr_time
    if valid_flag and interval > calculate_intervals():
        print(f'Timestamp {curr_time} past end session {end_session}, clamping')
        new_ts = end_session
    return valid_flag, new_ts


# ── Core download + upload ─────────────────────────────────────────────────────
def download_nse_data():
    t0    = time.time()
    nowdt = datetime.now(india_tz)

    date_str  = nowdt.strftime('%d%m%Y')
    timestamp = nowdt.strftime('%H%M')

    valid_flag, timestamp = check_valid_session(timestamp)
    if not valid_flag:
        print(f'Outside session window ({timestamp}), skipping')
        return

    # ── 1. Download ────────────────────────────────────────────────────────────
    t1       = time.time()
    session  = get_http_session()
    response = session.get(API_URL, timeout=30)
    response.raise_for_status()
    
    # ── 2. Parse + filter CSV ──────────────────────────────────────────────────
    content_str  = response.content.decode('utf-8-sig')
    reader       = csv.DictReader(StringIO(content_str))
    filtered_rows = []

    for row in reader:
        filtered_row = {}
        for src, out in column_map:
            if src in row:
                val = row[src]
                if out == 'vol_val_cum':
                    try:
                        val = f'{float(val):.2f}'
                    except Exception:
                        pass
                filtered_row[out] = val
        if len(filtered_row) == len(column_map):
            filtered_rows.append(filtered_row)

    # ── 3. Serialise to in-memory CSV (no disk write) ──────────────────────────
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=output_columns)
    writer.writeheader()
    writer.writerows(filtered_rows)
    csv_bytes = buf.getvalue().encode('utf-8')

    print(f'NSE download: {time.time() - t1:.2f}s  ({len(response.content)} bytes)')

    # ── 4. Upload to GCS ───────────────────────────────────────────────────────
    filename  = f'nse_data_{timestamp}.csv'
    blob_path = f'{GCS_PREFIX}/{date_str}/{filename}'

    t2     = time.time()
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET)
    blob   = bucket.blob(blob_path)

    # skip if already uploaded (same as rclone --ignore-existing)
    if blob.exists():
        print(f'Already exists. Replacing : {blob_path}')

    blob.upload_from_string(csv_bytes, content_type='text/csv')
    print(f'GCS upload : {time.time() - t2:.2f}s  → gs://{GCS_BUCKET}/{blob_path}')
    print(f'Total      : {time.time() - t0:.2f}s')


# ── Cloud Function entrypoint ──────────────────────────────────────────────────
@functions_framework.http
def run_job(request):
    """HTTP-triggered Cloud Function. Called by Cloud Scheduler."""
    try:
        download_nse_data()
        return 'OK', 200
    except Exception as e:
        print(f'ERROR: {e}')
        return str(e), 500


# ── Local dev entrypoint ───────────────────────────────────────────────────────
if __name__ == '__main__':
    download_nse_data()
