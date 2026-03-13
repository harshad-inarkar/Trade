import pandas as pd
import requests
import time
import os, re, sys, argparse
from io import StringIO

from sync_data import NSE_INDX_DATA

os.makedirs(NSE_INDX_DATA, exist_ok=True)

NIFTY_INDICES = [
    'NIFTY BANK',
    'NIFTY FINANCIAL SERVICES',
    'NIFTY MID SELECT',
    'NIFTY SMALLCAP 100',
    'NIFTY 50'
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
    "Nifty Waves" 
    ]


def get_file_name_from_index(index_name):
    """
    Convert an index name to a clean filename format.

    Args:
        name: Original name string
        
    Returns:
        Cleaned filename with .csv extension
    """
    # Convert to lowercase
    filename = index_name.lower()

    # Replace '&' with 'and' (optional, or you can remove it)
    filename = filename.replace('&', '')

    # Replace forward slashes and other special chars with underscores
    filename = re.sub(r'[/\-\s]+', '_', filename)

    # Remove any remaining special characters except underscores
    filename = re.sub(r'[^a-z0-9_]', '', filename)

    # Replace multiple consecutive underscores with single underscore
    filename = re.sub(r'_+', '_', filename)

    # Remove leading/trailing underscores
    filename = filename.strip('_')

    # Add .csv extension
    filename = filename + '.csv'

    return filename



def download_sectoral_indices(index_list=SECTORAL_INDICES):
    """Download all 21 sectoral indices to CSV files"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/'
    }
    
    print(f"Downloading {len(index_list)}  Files...")
    print("=" * 60)
    
    successful_downloads = 0
    failed_list = []
    for index_name in index_list:
        filename = f'{NSE_INDX_DATA}/{get_file_name_from_index(index_name)}'

        try:
            # Create session for NSE cookies
            session = requests.Session()
            session.headers.update(headers)
            
            # Establish session with NSE
            session.get('https://www.nseindia.com/market-data/live-equity-market', headers=headers)
            
            # URL encode index name
            encoded_index = index_name.upper().replace(' ', '%20').replace('/', '%2F').replace('&', '%26').replace('-', '%20')
            url = f'https://www.nseindia.com/api/equity-stockIndices?index={encoded_index}'
            
            print(f"📊 Downloading {index_name}...")
            
            # Download data
            response = session.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract symbols (skip first record if present)
            symbols = []
            if 'data' in data and len(data['data']) > 0:
                symbols = [item.get('symbol', '').upper().strip() for item in data['data'][1:] if item.get('symbol')]
            
            if symbols:
                df = pd.DataFrame({'Symbol': symbols})
                df.to_csv(filename, index=False)
                print(f"   ✓ {filename} ({len(symbols)} stocks)")
                successful_downloads += 1
            else:
                print(f"   ⚠️  No symbols found for {index_name}")
                failed_list.append(index_name)
                
        except Exception as e:
            print(f"   ❌ Error: {index_name} - {str(e)[:50]}")
            failed_list.append(index_name)


        
        # Rate limiting - NSE restriction
        time.sleep(0.001)
    
    print("\n" + "=" * 60)
    print(f"✅ SUCCESS: {successful_downloads}/{len(index_list)} indices downloaded!")
    print(f"Failed List:\n{'\n'.join(failed_list)}")
    create_summary_report(index_list)

    

def create_summary_report(index_list=SECTORAL_INDICES):
    """Create summary report of all downloaded files"""
    print("\n📋 SUMMARY REPORT:")
    total_stocks = 0
    
    for indx_name in index_list:
        filename = f'{NSE_INDX_DATA}/{get_file_name_from_index(indx_name)}'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            count = len(df)
            total_stocks += count
            print(f"   {filename}: {count} stocks")
    
    print(f"\n🎯 GRAND TOTAL: {total_stocks} stocks across sectors")

if __name__ == "__main__":
    print("🏦 NSE INDICES DOWNLOADER")
    print("=" * 60)

    parser = argparse.ArgumentParser(description='Indices Downloader')
    parser.add_argument('-ix', '--index', action='store_true', help='Nifty Broad Indices')

    args, unknown = parser.parse_known_args()

    index_flag = False
    if args.index:
        index_flag = True
   
    
    if index_flag:
        print('\nDownload Nifty Broad Indices only')
        download_sectoral_indices(NIFTY_INDICES)
    else:
        print('\nDownload Sectoral Indices')
        download_sectoral_indices()
