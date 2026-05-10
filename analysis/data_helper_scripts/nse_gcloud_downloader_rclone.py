#!/usr/bin/env python3
import requests
from datetime import datetime
import time
import csv
from io import StringIO
from datetime import datetime
import pytz
from sync_data import sync_data_args


india_tz = pytz.timezone('Asia/Kolkata')


PARENT_DIR ='gdrive:/cloud_data'
NSE_DATA_DIR=f'{PARENT_DIR}/nse_data'
NSE_INTRA_DAY = f'{NSE_DATA_DIR}/intraday'
TEMP_DIR='/tmp'

start_session = '0915'
end_session = '1530'


column_map = [
    ('Symbol', 'symbol'),
    ('Volume (Contracts) - Futures', 'vol_cum'),
    ('Value (₹ Lakhs) - Futures', 'vol_val_cum'),
    ('Underlying', 'price'),
]

output_columns = [out for _, out in column_map]

def calculate_intervals(tf=1, start_time_str=start_session, end_time_str=end_session):


    start = datetime.strptime(start_time_str, '%H%M')
    end = datetime.strptime(end_time_str, '%H%M')

    if start >= end:
        return 0

    total_duration = (end - start).total_seconds() / 60
    res = total_duration // tf
    return res


def check_valid_session(curr_time):
    interval = calculate_intervals(end_time_str=curr_time)
    valid_flag = interval > 0

    new_ts = curr_time

    if valid_flag and interval > calculate_intervals():
        print(f'Current timestamp {curr_time} passed end session time {end_session}')
        new_ts = end_session

    return valid_flag, new_ts
    


SESSION_URL='https://www.nseindia.com'
REFER_URL='https://www.nseindia.com/market-data/most-active-underlying'
API_URL="https://www.nseindia.com/api/live-analysis-most-active-underlying?csv=true"



def download_nse_data():

    nowdt = datetime.now(india_tz)
    start = time.time()


     # ----- Download data

    date_timestamp = nowdt.strftime("%d%m%Y")
    data_dir = f'{NSE_INTRA_DAY}/{date_timestamp}'
    timestamp = nowdt.strftime("%H%M")

    valid_flag, timestamp = check_valid_session(timestamp)

    if not valid_flag: 
        print(f'Not Valid timestamp {timestamp}. Ignore download')
        return


    url = API_URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': REFER_URL,
    }
    
    session = requests.Session()
    session.get(SESSION_URL, headers=headers)
    
    filename = f"nse_data_{timestamp}.csv"
    response = session.get(url, headers=headers, timeout=30)

 
    # Read CSV from response, filter and rename columns in map order
    content_str = response.content.decode(encoding='utf-8-sig')
    reader = csv.DictReader(StringIO(content_str))
    filtered_rows = []
    for row in reader:
        filtered_row = {}
        for src, out in column_map:
            if src in row:
                val = row[src]
                if out == 'vol_val_cum':
                    try:
                        val = f"{float(val):.2f}"
                    except Exception:
                        pass
                filtered_row[out] = val

        if len(filtered_row) == len(column_map):
            filtered_rows.append(filtered_row)
        
        
    file_path = f"{TEMP_DIR}/{filename}"

    # Write to CSV with output columns in required order
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_columns)
        writer.writeheader()
        writer.writerows(filtered_rows)
    
    print(f"Downloaded: {file_path} in {time.time() - start:.2f}s")


    # ---- upload to Google Drive ----   
    sync_data_args(file_path,data_dir)


def run_job():
    download_nse_data()

if __name__ == "__main__":
    run_job()