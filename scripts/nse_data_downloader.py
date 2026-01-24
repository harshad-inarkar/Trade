#!/usr/bin/env python3
import requests
from datetime import datetime
import os
import glob
import hashlib
import pandas as pd

data_dir = 'nse_data'
os.makedirs(data_dir, exist_ok=True)



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


def delete_duplicate_csv():
 
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
 

def download_nse_data():
    url = "https://www.nseindia.com/api/live-analysis-most-active-underlying?csv=true"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.nseindia.com/market-data/most-active-underlying',
    }
    
    session = requests.Session()
    session.get('https://www.nseindia.com', headers=headers)
    
    timestamp = datetime.now().strftime("%d%m%Y")
    filename = f"nse_data_{timestamp}.csv"
    
    response = session.get(url, headers=headers, timeout=30)

    
    with open(f"{data_dir}/{filename}", 'wb') as f:
        f.write(response.content)
    
    print(f"Downloaded: {filename}")

    delete_duplicate_csv()






if __name__ == "__main__":
    download_nse_data()
