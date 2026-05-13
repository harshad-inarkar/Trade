#!/usr/bin/env python3
import requests
from datetime import datetime
import os, argparse, time
import csv
from io import StringIO
from utils.data.paths import NSE_INTRADAY_DIR_PATH,REMOTE_INTRADAY_DIR_PATH 
from utils.data.sync_data import sync_data_args


reset_remote_sched = False
gcp_state_file = os.path.join(os.path.dirname(__file__), ".gcp_state")

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

    time_exceeded = False
    if valid_flag and interval > calculate_intervals():
        print(f'Current timestamp {curr_time} passed end session time {end_session}')
        new_ts = end_session
        time_exceeded = True

    return valid_flag, new_ts, time_exceeded
    


SESSION_URL='https://www.nseindia.com'
REFER_URL='https://www.nseindia.com/market-data/most-active-underlying'
API_URL="https://www.nseindia.com/api/live-analysis-most-active-underlying?csv=true"



def read_gcp_state():
    gcp_state = ''
    try:
        with open(gcp_state_file, "r") as f:
            gcp_state = f.read().strip()
    except Exception as ex:
        print(f"Warning: failed to read .gcp_state: {ex}")
    
    return gcp_state

def save_gcp_state(gcp_state):
    try:
        with open(gcp_state_file, "w") as f:
            f.write(gcp_state)
    except Exception as ex:
        print(f"Warning: failed to write .gcp_state: {ex}")


def reset_gcp_sched(to_state):
    try:
        import subprocess
        result = subprocess.run(
            [
                "gcloud", "scheduler", "jobs", to_state,
                "nse-downloader-function-job",
                "--location=asia-south1"
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✅ {to_state}d GCP scheduler job:\n{result.stdout.strip()}")
        # Write new state to .gcp_state
        save_gcp_state(to_state)

    except Exception as ex:
        print(f"Error in {to_state} GCP scheduler job: {ex}")




def download_nse_data():
    t1= time.time()
    date_timestamp = datetime.now().strftime("%d%m%Y")
    data_dir = f'{NSE_INTRADAY_DIR_PATH}/{date_timestamp}'
    now = datetime.now()
    minute = now.minute + (1 if now.second >= 30 else 0)
    timestamp = now.strftime("%H") + f"{minute:02d}"

    valid_flag, timestamp, time_exceeded_flag = check_valid_session(timestamp)

    if not valid_flag: 
        print(f'Not Valid timestamp {timestamp}. Ignore download')
        return

    os.makedirs(data_dir, exist_ok=True)

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
        
        
    file_path = f"{data_dir}/{filename}"

    # Write to CSV with output columns in required order
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_columns)
        writer.writeheader()
        writer.writerows(filtered_rows)
    
    print(f"Downloaded: {file_path} in {time.time() - t1:.2f}s  ")
    

    gcp_state = read_gcp_state()
 
    if reset_remote_sched and not time_exceeded_flag:
        # Pause the Cloud Scheduler job if it is active
        should_pause = not gcp_state or gcp_state == 'resume'  # Default is to pause
        if should_pause:
            print('Sync from remote to local')
            sync_data_args(REMOTE_INTRADAY_DIR_PATH,NSE_INTRADAY_DIR_PATH)
            reset_gcp_sched('pause')
        else:
            print("Scheduler pause skipped: gcp_state is not resumed")
    elif reset_remote_sched and time_exceeded_flag:
        # resume it if it is paused
        should_resume = not gcp_state or gcp_state == 'pause'
        if should_resume:
            reset_gcp_sched('resume')
            print('Sync from local to remote')
            sync_data_args(NSE_INTRADAY_DIR_PATH,REMOTE_INTRADAY_DIR_PATH)
        else:
            print("Scheduler resume skipped: gcp_state is not 'pause'")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='NSE Data Downloader')
    parser.add_argument('-rr', '--reset-remote-sched', action='store_true', help='Stop Remote Sched')


    args, unknown = parser.parse_known_args()
     
    if args.reset_remote_sched:
        print("Reset Remote Sched")
        reset_remote_sched = True

    download_nse_data()

