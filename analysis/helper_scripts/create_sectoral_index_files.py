import pandas as pd
import requests
import time
import os, re, sys
from io import StringIO
from nse_daily_data_downloader import NSE_INDX_DATA



os.makedirs(NSE_INDX_DATA, exist_ok=True)


NIFTY_INDICES = [
    'NIFTY BANK',
    'NIFTY FINANCIAL SERVICES',
    'NIFTY MID SELECT',
    'NIFTY SMALLCAP 100',
    'NIFTY 50'
]

SECTORAL_INDICES = [
 'NIFTY AUTO',
 'NIFTY BANK', 
 'NIFTY CHEMICALS',
 'NIFTY FINANCIAL SERVICES',
 'NIFTY FINANCIAL SERVICES 25/50',
 'NIFTY FMCG',
 'NIFTY HEALTHCARE',
 'NIFTY IT',
 'NIFTY MEDIA',
 'NIFTY METAL',
 'NIFTY PHARMA',
 'NIFTY PRIVATE BANK',
 'NIFTY PSU BANK',
 'NIFTY REALTY',
 'NIFTY CONSUMER DURABLES',
 'NIFTY OIL AND GAS',
 'NIFTY500 HEALTHCARE',
 'NIFTY MIDSMALL FINANCIAL SERVICES',
 'NIFTY MIDSMALL HEALTHCARE',
 'NIFTY MIDSMALL IT & TELECOM',
 "Nifty Capital Markets",
"Nifty Commodities",
"Nifty Core Housing",
"Nifty CPSE",
"Nifty EV & New Age Automotive",
"Nifty Energy",
"Nifty Housing",
"Nifty India Consumption",
"Nifty India Defence",
"Nifty India Digital",
"Nifty India Infrastructure & Logistics",
"Nifty India Internet",
"Nifty India Manufacturing",
"Nifty India Tourism",
"Nifty Infrastructure",
"Nifty MNC",
"Nifty Mobility",
"Nifty PSE",
"Nifty Rural",
"Nifty Services Sector",
"Nifty Transportation & Logistics",
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
                
        except Exception as e:
            print(f"   ❌ Error: {index_name} - {str(e)[:50]}")
        
        # Rate limiting - NSE restriction
        time.sleep(0.001)
    
    print("\n" + "=" * 60)
    print(f"✅ SUCCESS: {successful_downloads}/{len(index_list)} indices downloaded!")
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
    print("🏦 NSE SECTORAL INDICES DOWNLOADER")
    print("=" * 60)

    if len(sys.argv) != 3 and len(sys.argv) != 1:
        print("Usage: python script.py <sector_flag> <nse_index_flag>")
        sys.exit(1)

    sector_flag=True
    nse_index_flag = False

    if len(sys.argv) == 3:
        sector_flag = True if sys.argv[1] == '1' else False
        nse_index_flag = True if sys.argv[2] == '1' else False


    print(f"sector_flag {sector_flag}  and nse_index_flag {nse_index_flag}")

    if sector_flag:
        download_sectoral_indices()
    
    if nse_index_flag:
        download_sectoral_indices(NIFTY_INDICES)
    
    
