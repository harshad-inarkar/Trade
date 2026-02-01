
import os
import pandas as pd
import glob
from flask import Flask, render_template, jsonify
from datetime import datetime
import json
import csv
import sys
import math
import os
import re


if not(len(sys.argv) == 3 or len(sys.argv) == 1):
    print(f"wrong {len(sys.argv)} Usage: python app.py <filter_ltp_range> <fo>")
    sys.exit(1)



start_ltp = 0
end_ltp = float('inf')
fo = True 

if len(sys.argv) == 3:
    ltp_list= sys.argv[1].split('-')
    start_ltp = float(ltp_list[0])
    end_ltp = float(ltp_list[1])
    fo = True if sys.argv[2] == 'f' else False

value_col = 'Value (₹ Lakhs) - Futures' if  fo else 'Value (₹ Lakhs) - Options (Premium)'
data_dir = '../nse_data'



def process_files():
    symb_ltp = {}
    symbol_vol_data={}


    all_files = os.listdir(data_dir)
    csv_files = [f for f in all_files if f.endswith('.csv')]

    nse_files_with_dates = []
    date_pattern = r'nse_data_(\d{2})(\d{2})(\d{4})\.csv'
    
    for filename in csv_files:
        match = re.match(date_pattern, filename)
        if match:
            day, month, year = match.groups()
            date_str = f"{day}{month}{year}"
            
            # Parse date for proper sorting
            try:
                file_date = datetime.strptime(date_str, '%d%m%Y')
                nse_files_with_dates.append({
                    'filename': filename,
                    'date_str': file_date.strftime('%d'),
                    'date_obj': file_date
                })
            except ValueError:
                print(f"⚠️  Invalid date in {filename}, skipping")
                continue


    sorted_files = sorted(nse_files_with_dates, key=lambda x: x['date_obj'])
 
    day_indx = 0
    for finfo in sorted_files:
        fpath = f'{data_dir}/{finfo['filename']}'
        day_indx+=1
        with open(fpath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row['Symbol'].strip('"')
                ltp = float(row['Underlying'].strip('"'))
                symb_ltp[symbol] = ltp
                vol = float(row[value_col])
                symbol_vol_data.setdefault(symbol,{})[day_indx] = [vol,ltp,finfo['date_str'],finfo['filename'],0]


        

    # filter ltp
    sym_to_del = set()
    for sym in symbol_vol_data.keys():
        ltp = symb_ltp[sym]
        if not(ltp >= start_ltp and ltp <= end_ltp):
            sym_to_del.add(sym)

    for sym in sym_to_del:
        del symbol_vol_data[sym]


    return (symbol_vol_data, day_indx)



def get_percentile_dict():
    symbol_vol_data, n_days = process_files()
    symbol_percentile_ewma = {}
    symbol_vol_ewma ={}
    
    span = n_days    #10
    alpha = 2 / (span + 1)  # Smoothing factor: 2/(span+1)

    
    for day in range(1,n_days+1):
        # Get all volumes for this day
        day_volumes = []
        for symbol in symbol_vol_data:
            if day in symbol_vol_data[symbol]:
                day_volumes.append((symbol, symbol_vol_data[symbol][day][0]))
        
        if not day_volumes:
            continue
        
        # Sort by volume (highest first for percentile calculation)
        day_volumes.sort(key=lambda x: x[1], reverse=True)
        
        # Calculate percentile rank (100 = highest volume)
        n_vol = len(day_volumes)
        for rank, (symbol, volume) in enumerate(day_volumes):
            percentile = 100 * (1 - rank / n_vol)  # 100 for highest, 0 for lowest
            symbol_name = symbol
            symbol_vol_data[symbol][day][4] = percentile
            # Calculate EWMA for this symbol
            if symbol_name not in symbol_percentile_ewma:
                symbol_percentile_ewma[symbol_name] = percentile  # First value
                symbol_vol_ewma[symbol_name] = volume  # First value
            else:
                # EWMA formula: EWMA_t = alpha * value_t + (1-alpha) * EWMA_{t-1}
                symbol_percentile_ewma[symbol_name] = alpha * percentile + (1 - alpha) * symbol_percentile_ewma[symbol_name]
                symbol_vol_ewma[symbol_name] = alpha * volume + (1 - alpha) * symbol_vol_ewma[symbol_name]

    
    return (symbol_percentile_ewma, n_days,symbol_vol_data,symbol_vol_ewma)



#----

app = Flask(__name__)


fo = True
data_dir = '../nse_data'
symb_col = 'Symbol'
vol_col = 'Value (₹ Lakhs) - Futures' if  fo else 'Value (₹ Lakhs) - Options (Premium)'



def get_symbols_with_data():
    """Get unique symbols across all files with their volume data"""
 
    symbols_perc_ewma, n_days,symbol_vol_data, dummy = get_percentile_dict()
    symbols_data = {}

    for day_num in range(1,n_days+1):
        for symb, vol_data in symbol_vol_data.items():
            if day_num in vol_data:
                volume, ltp, dt_str, fname, perc = vol_data[day_num]
                symbols_data.setdefault(symb,[]).append({
                    'file': fname,
                    'timestamp': dt_str,
                    'volume': volume,
                    'ltp': ltp,
                    'perc': perc
                })
    
    return symbols_data


def get_symbols_with_avg_volume():
    """Get symbols with their average volume, sorted by average volume"""
    symbols_perc_ewma, n_days,symbol_vol_data, symbol_vol_ewma = get_percentile_dict()

    symbols_list = []
    for symbol, ewma in symbols_perc_ewma.items():
        last_ltp_day = len(symbol_vol_data[symbol])
        symbols_list.append({
            'symbol': symbol,
            'vol_perc_ma': ewma,
            'ltp': symbol_vol_data[symbol][last_ltp_day][1],
            'vol_ma': symbol_vol_ewma[symbol]
        })

    # Sort by average volume (highest first)
    symbols_list.sort(key=lambda x: x['vol_perc_ma'], reverse=True)

    return symbols_list



@app.route('/')
def index():
    """Main page with list of all symbols sorted by average volume"""
    symbols_list = get_symbols_with_avg_volume()

    return render_template('index.html', symbols=symbols_list, count=len(symbols_list))


@app.route('/symbol/<symbol_name>')
def symbol_detail(symbol_name):
    """Detail page for a specific symbol with volume graph"""
    symbols_data = get_symbols_with_data()

    if symbol_name not in symbols_data:
        return "Symbol not found", 404

    data = symbols_data[symbol_name]
    return render_template('symbol.html', symbol=symbol_name, data=data)


@app.route('/api/symbol/<symbol_name>')
def api_symbol(symbol_name):
    """API endpoint for symbol data"""

    print(f"inside api symbol for {symbol_name}")
    symbols_data = get_symbols_with_data()

    if symbol_name not in symbols_data:
        return jsonify({'error': 'Symbol not found'}), 404

    return jsonify({
        symb_col: symbol_name,
        'data': symbols_data[symbol_name]
    })

if __name__ == '__main__':
    print("Starting web portal on http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    app.run(host='0.0.0.0', port=5000, debug=True)
