import bisect
from operator import length_hint
import os
import pandas as pd
import glob
from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import csv
import sys
import re
import threading
import time

# ================= CONFIG =================

PARENT_DIR = '../'
NSE_DATA_DIR = f'{PARENT_DIR}/nse_data'
NSE_INTRADAY = f'{NSE_DATA_DIR}/intraday'


RELOAD_INTERVAL_MINUTES = None



timeframe = 0
one_day_intervals = 0
start_session = '0915'
end_session = '1530'

start_ltp = 0
end_ltp = float('inf')


symb_col = 'Symbol'
value_col = 'Value (₹ Lakhs) - Futures'
ltp_col = 'Underlying'

fields = ['volume', 'ltp', 'ts_str', 'vol_adj', 'volume_slow', 'volume_fast']
fields_dict = dict(zip(fields, range(len(fields))))

# ================= CACHE =================

TF_KEYS = ("3", "15", "D")
CACHE = {tf: {"symbols_data": None, "symbols_avg": None} for tf in TF_KEYS}
CACHE["refresh_time"] = None
CACHE_READY = False
CACHE_LOCK = threading.Lock()

# ================= UTILS =================

def calculate_intervals(tf, start_time_str=start_session, end_time_str=end_session, fraction=False):
    start = datetime.strptime(start_time_str, '%H%M')
    end = datetime.strptime(end_time_str, '%H%M')
    if start >= end:
        return 0
    total_duration = (end - start).total_seconds() / 60
    res = total_duration / tf
    return int(res) if not fraction else res


def check_valid_session(curr_time):
    interval = calculate_intervals(tf=1, end_time_str=curr_time)
    return 0 < interval <= calculate_intervals(tf=1)

# ================= CORE PROCESSING (UNCHANGED) =================

def process_files(data_dir, date_pattern, dt_frmt, dt_str_frmt):
    csv_files = glob.glob(os.path.join(data_dir, "**/*.csv"), recursive=True)
    nse_files_with_dates = []
    uniq_dates = set()

    for filename in csv_files:
        match = re.match(date_pattern, filename)
        if match:
            mgrplist = match.groups()
            date_str = ''.join(mgrplist)
            if not check_valid_session(date_str[-4:]):
                continue
            
            # Parse date for proper sorting
            try:
                file_date = datetime.strptime(date_str, dt_frmt)
                nse_files_with_dates.append({
                    'filename': filename,
                    'date_str': file_date.strftime(dt_str_frmt),
                    'date_obj': file_date
                })
                
                uniq_dates.add(file_date.date())


            except ValueError:
                print(f"⚠️  Invalid date in {filename}, skipping")
                continue

    sorted_files = sorted(nse_files_with_dates, key=lambda x: x['date_obj'])
    sorted_dates = [d.strftime('%d%m%Y') for d in sorted(uniq_dates)]
    sym_vol_data, total_files = process_csv_files(sorted_files, sorted_dates)
    return sym_vol_data, total_files, sorted_files[-1]['date_obj']
 


def get_ts_str_from_fileindex(indx, sorted_dates):
    ninterval = (indx - 1) % one_day_intervals
    dayindx = (indx - 1) // one_day_intervals
    start_date = datetime.strptime(sorted_dates[dayindx] + start_session, '%d%m%Y%H%M')
    newdate = start_date + timedelta(minutes=(ninterval + 1) * timeframe)
    return newdate.strftime('%d_%H%M')



def fill_empty_interval_gaps(symbol_vol_data, total_files, sorted_dates):
    """Fill missing intervals by interpolating from prev/next. O(symbols * missing)."""
    vol_idx = fields_dict['volume']
    vol_adj_idx = fields_dict['vol_adj']
    ltp_idx = fields_dict['ltp']
    ts_str_idx = fields_dict['ts_str']
    full_range = set(range(1, total_files + 1))

    for sym in symbol_vol_data:
        sym_data = symbol_vol_data[sym]
        if len(sym_data) == total_files:
            continue

        sorted_keys = sorted(sym_data.keys())
        missing = sorted(full_range - sym_data.keys())

        for i in missing:
            if (((i - 1) % one_day_intervals) + 1) == 1:
                def_data = [0] * len(fields)
            else:
                def_data = list(sym_data[i - 1])

            idx = bisect.bisect_right(sorted_keys, i)
            j = sorted_keys[idx] if idx < len(sorted_keys) else None
            ithday_lastinterval = (((i - 1) // one_day_intervals) + 1) * one_day_intervals

            if j is not None and j <= ithday_lastinterval:
                next_avail_data = sym_data[j]
                gap = j - i + 1
                def_data[vol_idx] += (next_avail_data[vol_idx] - def_data[vol_idx]) / gap
                def_data[vol_adj_idx] = def_data[vol_idx]
                if (i % one_day_intervals) == 1:
                    def_data[ltp_idx] = next_avail_data[ltp_idx]
                else:
                    def_data[ltp_idx] += (next_avail_data[ltp_idx] - def_data[ltp_idx]) / gap

            def_data[ts_str_idx] = get_ts_str_from_fileindex(i, sorted_dates)
            sym_data[i] = def_data




def process_csv_files(sorted_files, sorted_dates):
    symb_ltp = {}
    symbol_vol_data = {}
    n_file = 0
    curr_date = ''
    nday = -1
    index_file_map = {}
    sort_f_indx = 0
    len_sort_files = len(sorted_files)
    recent_nfiles = 5
    recent_files_symset = set()

    for finfo in sorted_files:
        fpath = finfo['filename']
        sort_f_indx += 1

        file_day, file_time =finfo['date_str'].split('_')
        if file_day != curr_date:
            nday+=1
            curr_date=file_day

        curr_float_interval = calculate_intervals(end_time_str=file_time,fraction=True,tf=timeframe)
        round_interval = round(curr_float_interval)
        n_file= nday * one_day_intervals + round_interval

        # get closest Time stamp file for n_file index
        if not curr_float_interval.is_integer():
            if n_file in index_file_map:
                prev_file_time = index_file_map[n_file]
                prev_float_interval=calculate_intervals(end_time_str=prev_file_time,fraction=True,tf=timeframe)

                if prev_float_interval.is_integer() or (abs(prev_float_interval - round_interval) <= abs(curr_float_interval - round_interval)):
                    continue

        index_file_map[n_file]=file_time
                

        recent_file_flag = (len_sort_files - sort_f_indx - recent_nfiles) <= 0

        with open(fpath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row[symb_col].strip('"')
                ltp = float(row[ltp_col].strip('"'))
                symb_ltp[symbol] = ltp
                vol = float(row[value_col])
                symb_data_list = symbol_vol_data.setdefault(symbol,{}).setdefault(n_file , [None] * len(fields))

                symb_data_list[fields_dict['volume']] = vol
                symb_data_list[fields_dict['vol_adj']] = vol
                symb_data_list[fields_dict['ltp']] = ltp
                symb_data_list[fields_dict['ts_str']] = get_ts_str_from_fileindex(n_file, sorted_dates)


                if recent_file_flag:
                    recent_files_symset.add(symbol)

        
    # filter ltp
    sym_to_del = set()
    for sym in symbol_vol_data.keys():
        ltp = symb_ltp[sym]
        if not(ltp >= start_ltp and ltp <= end_ltp) or sym not in recent_files_symset:
            sym_to_del.add(sym)

    for sym in sym_to_del:
        del symbol_vol_data[sym]


    total_files = max(index_file_map.keys())
    fill_empty_interval_gaps(symbol_vol_data, total_files, sorted_dates)

    # adjust volume
    def get_day_ts(symb,i):
        return symbol_vol_data[symb][i][fields_dict['ts_str']].split('_')[0]
    
    for symb in symbol_vol_data:
        for file_num in range(2,total_files+1):
            if get_day_ts(symb,file_num-1) == get_day_ts(symb,file_num):
                prev_day_vol = symbol_vol_data[symb][file_num-1][fields_dict['volume']]
                curr_vol = symbol_vol_data[symb][file_num][fields_dict['volume']] 
                if curr_vol > prev_day_vol:
                    symbol_vol_data[symb][file_num][fields_dict['vol_adj']] = curr_vol - prev_day_vol
        
                                
    # calculate ewma
    calculate_rma('vol_adj', 'volume_fast',symbol_vol_data,total_files,8) # blue line fast

       # calculate sma
    calculate_rma('vol_adj', 'volume_slow',symbol_vol_data,total_files,21)  # red line slow



    return symbol_vol_data, total_files



def calculate_sma(data_field, target_data_field,symbol_vol_data,total_days,sma_len):
    # calculate sma
    for symb in symbol_vol_data:
        for day_num in range(1,total_days+1):
            window_size = min(day_num, sma_len)
            window_start = max(1,  day_num - sma_len)
            window = []
            for i in range(window_start,day_num+1):
                window.append(symbol_vol_data[symb][i][fields_dict[data_field]] )

            
            symbol_vol_data[symb][day_num][fields_dict[target_data_field]] = (sum(window) / len(window))


def calculate_ewma(data_field, target_data_field,symbol_vol_data,total_days,ewma_len):
   # calculate ewma
    alpha = 2 / (ewma_len + 1)
    for symb in symbol_vol_data:
        symbol_vol_data[symb][1][fields_dict[target_data_field]] = symbol_vol_data[symb][1][fields_dict[data_field]]
        for day_num in range(2,total_days+1):
            prev_ewma = symbol_vol_data[symb][day_num-1][fields_dict[target_data_field]]
            curr_vol = symbol_vol_data[symb][day_num][fields_dict[data_field]]

            symbol_vol_data[symb][day_num][fields_dict[target_data_field]] = alpha * curr_vol + (1 - alpha) * prev_ewma


def calculate_rma(data_field, target_data_field,symbol_vol_data,total_days,ewma_len):
   # calculate ewma
    alpha = 1 /ewma_len
    for symb in symbol_vol_data:
        symbol_vol_data[symb][1][fields_dict[target_data_field]] = symbol_vol_data[symb][1][fields_dict[data_field]]
        for day_num in range(2,total_days+1):
            prev_ewma = symbol_vol_data[symb][day_num-1][fields_dict[target_data_field]]
            curr_vol = symbol_vol_data[symb][day_num][fields_dict[data_field]]
            symbol_vol_data[symb][day_num][fields_dict[target_data_field]] = alpha * curr_vol + (1 - alpha) * prev_ewma


def calculate_wma(data_field, target_data_field,symbol_vol_data,total_days,sma_len):
    # calculate sma
    for symb in symbol_vol_data:
        for day_num in range(1,total_days+1):
            window_size = min(day_num, sma_len)
            window_start = max(1,  day_num - sma_len)
            window = []
            for i in range(window_start,day_num+1):
                window.append(symbol_vol_data[symb][i][fields_dict[data_field]])

            weights = list(range(1, window_size+1))
            weighted_sum = sum(w * p for w, p in zip(weights, window))

            symbol_vol_data[symb][day_num][fields_dict[target_data_field]] = (weighted_sum / sum(weights))

# ================= CACHE LOADER =================

def process_files_for_timeframe(tf_arg):
    """Run process_files for a specific timeframe ('3', '15', 'D'). Returns (symbols_data, symbols_avg)."""
    global timeframe, one_day_intervals
    old_tf, old_oi = timeframe, one_day_intervals
    try:
        data_dir = NSE_INTRADAY
        date_pattern = r'.*(\d{2})(\d{2})(\d{4})/nse_data_(\d{2})(\d{2}).csv'
        dt_frmt, dt_str_frmt = '%d%m%Y%H%M', '%d_%H%M' 
        
        if tf_arg == "D":
            timeframe = calculate_intervals(tf=1)
        else:
            timeframe = int(tf_arg)


        one_day_intervals = calculate_intervals(tf=timeframe)
        symbol_vol_data, total_files, dt_obj = process_files(data_dir, date_pattern, dt_frmt, dt_str_frmt)
        return _build_cache_entries(symbol_vol_data, total_files,dt_obj)
    finally:
        timeframe, one_day_intervals = old_tf, old_oi


def _build_cache_entries(symbol_vol_data, total_files,dt_obj):
    """Build symbols_data and symbols_avg from processed symbol_vol_data."""
    symbols_data = {}
    for day_num in range(1, total_files + 1):
        for symb, vol_data in symbol_vol_data.items():
            if day_num in vol_data:
                row = vol_data[day_num]
                ts_str = row[fields_dict['ts_str']]
                ts = ts_str.split('_')[0] if one_day_intervals == 1 else  ts_str.split('_')[-1] 
                symbols_data.setdefault(symb, []).append({
                    'timestamp': ts,
                    'volume': row[fields_dict['vol_adj']],
                    'volume_slow': row[fields_dict['volume_slow']],
                    'volume_fast': row[fields_dict['volume_fast']],
                    'ltp': row[fields_dict['ltp']],
                })

    symbols_avg = []
    for symb in symbol_vol_data:
        last_idx = max(symbol_vol_data[symb].keys())
        slow = symbol_vol_data[symb][last_idx][fields_dict['volume_slow']]
        fast = symbol_vol_data[symb][last_idx][fields_dict['volume_fast']]
        symbols_avg.append({
            'symbol': symb,
            'volume_slow': slow,
            'volume_fast': fast,
            'vol_surge': fast - slow,
            'ltp': symbol_vol_data[symb][last_idx][fields_dict['ltp']],
        })
    symbols_avg.sort(key=lambda x: x['vol_surge'], reverse=True)
    return symbols_data, symbols_avg, dt_obj


def _do_load_into_cache():
    """Load data from disk for all 3 timeframes and update CACHE. Caller must hold CACHE_LOCK."""
    global CACHE_READY

    print(f"⚙️ {datetime.now().strftime('%M:%S')} : Processing CSV files for {TF_KEYS} timeframes...")
    for tf in TF_KEYS:
        print(f"   Loading tf={tf}...")
        symbols_data, symbols_avg , dt_obj = process_files_for_timeframe(tf)
        CACHE[tf]["symbols_data"] = symbols_data
        CACHE[tf]["symbols_avg"] = symbols_avg


    CACHE["refresh_time"] = dt_obj.strftime('%H:%M')
    CACHE_READY = True
    print(f"✅ {datetime.now().strftime('%M:%S')} : Data cached for all timeframes")


def load_data_once():
    global CACHE_READY

    if CACHE_READY:
        return

    with CACHE_LOCK:
        if CACHE_READY:
            return
        _do_load_into_cache()


def force_reload():
    """Reload data from disk for all 3 timeframes without blocking requests."""
    print(f"🔄 {datetime.now().strftime('%M:%S')} : Reloading data for {TF_KEYS} timeframes...")
    new_cache = {tf: {} for tf in TF_KEYS}
    for tf in TF_KEYS:
        print(f"   Loading tf={tf}...")
        symbols_data, symbols_avg,dt_obj = process_files_for_timeframe(tf)
        new_cache[tf]["symbols_data"] = symbols_data
        new_cache[tf]["symbols_avg"] = symbols_avg


    print(f"{datetime.now().strftime('%M:%S')} : Lock for Reload")
    with CACHE_LOCK:
        for tf in TF_KEYS:
            CACHE[tf]["symbols_data"] = new_cache[tf]["symbols_data"]
            CACHE[tf]["symbols_avg"] = new_cache[tf]["symbols_avg"]
        CACHE["refresh_time"] = dt_obj.strftime('%H:%M')
    print(f"✅ {datetime.now().strftime('%M:%S')} : Reload complete")

# ================= FLASK =================

app = Flask(__name__)


def periodic_reload():
    interval_sec = RELOAD_INTERVAL_MINUTES * 60
    print(f"🔁 Auto reload enabled: every {RELOAD_INTERVAL_MINUTES} minutes")

    while True:
        time.sleep(interval_sec)
        force_reload()

def _tf_safe(tf):
    return tf if tf in TF_KEYS else "3"

@app.route('/')
def index():
    load_data_once()
    tf = _tf_safe(request.args.get('tf', '3'))
    symbols_list = CACHE[tf]["symbols_avg"]
    return render_template('index.html', symbols=symbols_list, count=len(symbols_list), refresh_time=CACHE.get("refresh_time", "-"), timeframe=tf)

@app.route('/symbol/<symbol_name>')
def symbol_detail(symbol_name):
    load_data_once()
    tf = _tf_safe(request.args.get('tf', '3'))
    symbols_data = CACHE[tf]["symbols_data"]
    if symbol_name not in symbols_data:
        return "Symbol not found", 404

    return render_template('symbol.html', symbol=symbol_name, data=symbols_data[symbol_name], timeframe=tf)

@app.route('/api/symbol/<symbol_name>')
def api_symbol(symbol_name):
    load_data_once()
    tf = _tf_safe(request.args.get('tf', '3'))
    symbols_data = CACHE[tf]["symbols_data"]
    if symbol_name not in symbols_data:
        return jsonify({'error': 'Symbol not found'}), 404
    return jsonify({
        symb_col: symbol_name,
        'data': symbols_data[symbol_name]
    })

# ================= MAIN =================

if __name__ == '__main__':
    print("🚀 Starting web portal on http://localhost:5000")

    if not (len(sys.argv) == 2 or len(sys.argv) == 1):
        print("Usage: python app.py <reload_time>")
        sys.exit(1)

    if len(sys.argv) == 2:
        RELOAD_INTERVAL_MINUTES = int(sys.argv[1]) if sys.argv[1] != '0' else None


    if RELOAD_INTERVAL_MINUTES:
        print(f'Reloads every {RELOAD_INTERVAL_MINUTES} mins')
        t = threading.Thread(
            target=periodic_reload,
            daemon=True
        )
        t.start()

    load_data_once()


    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.jinja_env.auto_reload = True
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
