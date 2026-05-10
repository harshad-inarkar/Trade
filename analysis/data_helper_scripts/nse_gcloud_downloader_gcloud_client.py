#!/usr/bin/env python3
"""
NSE intraday downloader — Google Cloud Run Jobs, personal Google Drive.

Auth strategy:
  - OAuth2 token is generated once locally and stored in Google Secret Manager.
  - Cloud Run reads the token at startup — no interactive browser needed.
  - If token is expired, it auto-refreshes and writes back to Secret Manager.

One-time local setup:
  1. Create OAuth2 Desktop credentials in GCP Console → download credentials.json
  2. Run: python nse_gcloud_downloader.py --setup credentials.json

Required env vars:
  GDRIVE_ROOT_FOLDER_ID     Folder ID in your personal Google Drive
  SECRET_NAME               Secret Manager secret name (default: nse-drive-token)
  GOOGLE_CLOUD_PROJECT      GCP project ID
"""

import argparse
import csv
import json
import os
import time
from datetime import datetime
from io import StringIO

import pytz
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from google.cloud import secretmanager

# ── Config ─────────────────────────────────────────────────────────────────────
india_tz = pytz.timezone('Asia/Kolkata')

GDRIVE_ROOT_FOLDER_ID =  os.environ['GDRIVE_ROOT_FOLDER_ID']
GCP_PROJECT           =  os.environ['GOOGLE_CLOUD_PROJECT']
SECRET_NAME           =  os.environ.get('SECRET_NAME', 'gdrive-cloud-data-token')


start_session = '0915'
end_session   = '1530'

column_map = [
    ('Symbol',                       'symbol'),
    ('Volume (Contracts) - Futures', 'vol_cum'),
    ('Value (₹ Lakhs) - Futures',   'vol_val_cum'),
    ('Underlying',                   'price'),
]
output_columns = [out for _, out in column_map]

SESSION_URL = 'https://www.nseindia.com'
REFER_URL   = 'https://www.nseindia.com/market-data/most-active-underlying'
API_URL     = 'https://www.nseindia.com/api/live-analysis-most-active-underlying?csv=true'

SCOPES = ['https://www.googleapis.com/auth/drive']

# ── Secret Manager ─────────────────────────────────────────────────────────────

def secret_path():
    return f'projects/{GCP_PROJECT}/secrets/{SECRET_NAME}/versions/latest'


def load_token_from_secret() -> dict:
    client  = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(name=secret_path()).payload.data.decode('utf-8')
    return json.loads(payload)


def save_token_to_secret(token_data: dict):
    client  = secretmanager.SecretManagerServiceClient()
    parent  = f'projects/{GCP_PROJECT}/secrets/{SECRET_NAME}'
    client.add_secret_version(
        parent=parent,
        payload={'data': json.dumps(token_data).encode('utf-8')}
    )


# ── Auth ───────────────────────────────────────────────────────────────────────

def get_credentials() -> Credentials:
    creds = Credentials.from_authorized_user_info(load_token_from_secret(), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_token_to_secret(json.loads(creds.to_json()))
        else:
            raise RuntimeError('Token invalid — re-run locally with --setup to regenerate.')
    return creds


_drive_service = None

def get_service():
    global _drive_service
    if _drive_service is None:
        _drive_service = build('drive', 'v3', credentials=get_credentials(), cache_discovery=False)
    return _drive_service


# ── One-time local setup ───────────────────────────────────────────────────────

def setup(credentials_file: str = 'credentials.json'):
    flow  = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
    creds = flow.run_local_server(port=0)

    sm_client = secretmanager.SecretManagerServiceClient()
    parent    = f'projects/{GCP_PROJECT}/secrets/{SECRET_NAME}'

    try:
        sm_client.get_secret(name=parent)
    except Exception:
        sm_client.create_secret(
            parent=f'projects/{GCP_PROJECT}',
            secret_id=SECRET_NAME,
            secret={'replication': {'automatic': {}}}
        )

    sm_client.add_secret_version(
        parent=parent,
        payload={'data': creds.to_json().encode('utf-8')}
    )
    print(f'✅ Token stored in Secret Manager as "{SECRET_NAME}". Ready to deploy.')


# ── Session helpers ────────────────────────────────────────────────────────────

def check_valid_session(curr_time: str) -> tuple[bool, str]:
    def to_mins(t):
        dt = datetime.strptime(t, '%H%M')
        return dt.hour * 60 + dt.minute

    start_m = to_mins(start_session)
    end_m   = to_mins(end_session)
    curr_m  = to_mins(curr_time)

    if curr_m < start_m:
        return False, curr_time
    if curr_m > end_m:
        return True, end_session
    return True, curr_time


# ── Drive helpers ──────────────────────────────────────────────────────────────

def get_or_create_folder(service, name: str, parent_id: str) -> str:
    q = (
        f"name='{name}' and '{parent_id}' in parents "
        f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    res = service.files().list(q=q, fields='files(id)', pageSize=1).execute()
    if res['files']:
        return res['files'][0]['id']
    folder = service.files().create(
        body={'name': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]},
        fields='id'
    ).execute()
    return folder['id']


def file_exists(service, name: str, parent_id: str) -> bool:
    q = f"name='{name}' and '{parent_id}' in parents and trashed=false"
    res = service.files().list(q=q, fields='files(id)', pageSize=1).execute()
    return bool(res['files'])


def upload_csv_bytes(service, csv_bytes: bytes, filename: str, parent_id: str):
    media = MediaInMemoryUpload(csv_bytes, mimetype='text/csv', resumable=False)
    service.files().create(
        body={'name': filename, 'parents': [parent_id]},
        media_body=media,
        fields='id'
    ).execute()


# ── Main job ───────────────────────────────────────────────────────────────────

def run_job():
    t0    = time.time()
    nowdt = datetime.now(india_tz)

    timestamp      = nowdt.strftime('%H%M')
    date_timestamp = nowdt.strftime('%d%m%Y')

    valid, timestamp = check_valid_session(timestamp)
    if not valid:
        print(f'Outside market hours ({timestamp}). Nothing to do.')
        return

    filename = f'nse_data_{timestamp}.csv'

    # ── 1. Resolve Drive folders ───────────────────────────────────────────────
    svc = get_service()
    t1  = time.time()

    nse_data_id = get_or_create_folder(svc, 'nse_data',     GDRIVE_ROOT_FOLDER_ID)
    intraday_id = get_or_create_folder(svc, 'intraday',     nse_data_id)
    date_dir_id = get_or_create_folder(svc, date_timestamp, intraday_id)

    if file_exists(svc, filename, date_dir_id):
        print(f'⏭  {filename} already exists. Done.')
        return

    print(f'Drive folders resolved in {time.time() - t1:.2f}s')

    # ── 2. Download from NSE ───────────────────────────────────────────────────
    t2 = time.time()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer':    REFER_URL,
    }
    http_session = requests.Session()
    http_session.get(SESSION_URL, headers=headers, timeout=10)
    resp = http_session.get(API_URL, headers=headers, timeout=20)
    resp.raise_for_status()
    print(f'NSE fetch in {time.time() - t2:.2f}s')

    # ── 3. Parse & filter in memory ───────────────────────────────────────────
    reader        = csv.DictReader(StringIO(resp.content.decode('utf-8-sig')))
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

    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=output_columns)
    writer.writeheader()
    writer.writerows(filtered_rows)
    csv_bytes = buf.getvalue().encode('utf-8')

    # ── 4. Upload from memory ─────────────────────────────────────────────────
    t3 = time.time()
    upload_csv_bytes(svc, csv_bytes, filename, date_dir_id)
    print(f'Upload in {time.time() - t3:.2f}s')

    print(f'✅ {filename} done in {time.time() - t0:.2f}s total')


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--setup',
        metavar='CREDENTIALS_JSON',
        nargs='?',
        const='credentials.json',
        help='Run OAuth2 setup flow and push token to Secret Manager'
    )
    args = parser.parse_args()

    if args.setup:
        setup(args.setup)
    else:
        run_job()