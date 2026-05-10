#!/usr/bin/env python3
import requests
from datetime import datetime
import os, sys
import glob
import hashlib
import pandas as pd


OUT_DIR='out'
NSE_DATA = 'nse_data'
NSE_DAILY_DATA = f'{NSE_DATA}/daily/tgtl'
NSE_INDX_DATA = f'{NSE_DATA}/index'
NSE_INTRA_DAY = f'{NSE_DATA}/intraday/tgtl'


SESSION_URL='https://www.nseindia.com'
REFER_URL='https://www.nseindia.com/market-data/top-gainers-losers'
API_URL="https://www.nseindia.com/api/live-analysis-variations?index=gainers&type=FOSec&csv=true"



def compare_csv_files_by_hash(file1, file2):
    """
    Compare two CSV files using MD5 hash.

    Args:
        file1 (str): Path to first CSV file
        file2 (str): Path to second CSV file

    Returns:
        bool: True if files are identical, False otherwise
    """
    with open(file1, 'rb') as f1:
        hash1 = hashlib.md5(f1.read()).hexdigest()

    with open(file2, 'rb') as f2:
        hash2 = hashlib.md5(f2.read()).hexdigest()

    return hash1 == hash2


def delete_duplicate_csv(data_dir):
 
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))

    if len(csv_files) < 2:
        print(f"Found only {len(csv_files)} CSV file(s). Need at least 2 files to compare.")
        return

    # Sort by modification time (most recent first)
    csv_files.sort(key=lambda t: os.stat(t).st_mtime, reverse=True)

    file1, file2 = csv_files[:2]

   
    print(f"Comparing {file1} and {file2}")
    are_identical = compare_csv_files_by_hash(file1, file2)
    if are_identical:
        os.remove(file1)
        print(f"Duplicate file {file1} deleted successfully!")
 

def download_nse_data(intraday=False):

    data_dir = NSE_DAILY_DATA
    date_timestamp = datetime.now().strftime("%d%m%Y")
    timestamp= date_timestamp

    if intraday:
        data_dir = f'{NSE_INTRA_DAY}/{date_timestamp}'
        timestamp = datetime.now().strftime("%H%M")

    
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

    
    file_path = f"{data_dir}/{filename}"

    with open(file_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Downloaded: {file_path}")

    delete_duplicate_csv(data_dir)






if __name__ == "__main__":


    if len(sys.argv) != 2 and len(sys.argv) != 1:
        print("Usage: python script.py <intraday_flag>")
        sys.exit(1)

    intraday_flag = False

    if len(sys.argv) == 2:
        intraday_flag = True if sys.argv[1] == '1' else False


    download_nse_data(intraday_flag)
