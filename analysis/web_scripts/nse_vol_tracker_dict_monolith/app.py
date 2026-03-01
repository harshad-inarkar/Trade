import bisect
from itertools import filterfalse
from operator import length_hint
import os
from typing import Any
import glob
from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import csv
import sys
import re, math
import threading
import time

from sys import path as _syspath
_syspath.append(os.path.abspath("../../")) # analysis dir
from web_scripts.data_scripts.sync_data import OUT_DIR, NSE_INTRADAY_DIR_PATH,TEMPLATES_PARENT_DIR

# ================= CONFIG =================



RELOAD_INTERVAL_MINUTES = None


timeframe = 0
one_day_intervals = 0
timeframe_str=''
data_dir = NSE_INTRADAY_DIR_PATH
date_pattern = r'.*(\d{2})(\d{2})(\d{4})/nse_data_(\d{2})(\d{2}).csv'
dt_frmt, dt_str_frmt, refresh_dt_pat = '%d%m%Y%H%M', '%d_%H%M' , 'Date: %d  Time: %H:%M'


start_session = '0915'
end_session = '1530'


symb_col = 'symbol'
value_col = 'vol_val_cum'
ltp_col = 'price'

fields = ['volume_cumulative', 'ltp','volume', 'volume_slow', 'volume_fast']
vol_cumul_indx, ltp_indx = fields.index('volume_cumulative'), fields.index('ltp')
vol_indx = fields.index('volume')
vol_slow_indx, vol_fast_indx = fields.index('volume_slow'), fields.index('volume_fast')

cache_symbols_fields = ['timestamp_full', 'timestamp', 'volume_cumulative', 'volume', 'volume_slow', 'volume_fast', 'ltp']
ch_ts_indx, ch_tsf_indx = cache_symbols_fields.index('timestamp'), cache_symbols_fields.index('timestamp_full')
ch_vol_cumul_indx, ch_vol_indx = cache_symbols_fields.index('volume_cumulative'), cache_symbols_fields.index('volume')
ch_vols_indx, ch_volf_indx = cache_symbols_fields.index('volume_slow'), cache_symbols_fields.index('volume_fast')
ch_ltp_indx = cache_symbols_fields.index('ltp')


cache_index_fields = ['symbol','volume_slow', 'volume_fast', 'vol_surge','ltp', 'volume']




# ================= CACHE =================
MIN_TF = '3'

TF_KEYS = (MIN_TF, "15", "D")
INDX_SORT_KEYS = cache_index_fields
CACHE = {tf: {"symbols_data": None, "symbols_avg": None} for tf in TF_KEYS}
CACHE["refresh_time"] = None
CACHE_READY = False
CACHE_LOCK = threading.Lock()

CACHE[MIN_TF]['processed'] = {}
CACHE[MIN_TF]['processed']['start_time'] = None
CACHE[MIN_TF]['processed']['refresh_time'] = None
CACHE[MIN_TF]['processed']['sorted_dates'] = None
CACHE[MIN_TF]['processed']['vol_data'] = None



# ================= UTILS =================

# Caches for utils functions (sorted_dates is constant, so excluded from cache keys)
_dt_obj_cache = {}  # key: indx -> datetime_obj
_index_from_dtobj_cache = {}  # key: (dt_obj_str, cur_tf_str) -> index
_one_day_intervals_cache = {}  # key: cur_tf_str -> (cur_tf, cur_one_day_intervals)

def clear_utils_caches():
    """Clear all utils function caches. Call when timeframe changes or data is reloaded."""
    global _dt_obj_cache, _index_from_dtobj_cache, _one_day_intervals_cache
    _dt_obj_cache.clear()
    _index_from_dtobj_cache.clear()
    _one_day_intervals_cache.clear()

def calculate_intervals(tf, start_time_str=start_session, end_time_str=end_session):
    start = datetime.strptime(start_time_str, '%H%M')
    end = datetime.strptime(end_time_str, '%H%M')
    if start >= end:
        return 0
    total_duration = (end - start).total_seconds() / 60

    return math.ceil(total_duration / tf)


def check_valid_session(curr_time):
    interval = calculate_intervals(tf=1, end_time_str=curr_time)
    return 0 < interval <= calculate_intervals(tf=1)


def get_one_day_intervals(cur_tf_str):
    if cur_tf_str in _one_day_intervals_cache:
        return _one_day_intervals_cache[cur_tf_str]
    
    cur_tf, cur_oneday_interval = None, None
    if cur_tf_str == 'D':
        cur_tf = calculate_intervals(tf=1)
    else:
        cur_tf = int(cur_tf_str)
    
    cur_oneday_interval = calculate_intervals(tf=cur_tf)

    result = (cur_tf, cur_oneday_interval)
    _one_day_intervals_cache[cur_tf_str] = result
    return result


def get_dt_obj_from_fileindex(indx, sorted_dates):
    # Cache by indx only (sorted_dates is constant)
    if indx in _dt_obj_cache:
        return _dt_obj_cache[indx]
    
    cur_tf, cur_one_day_intervals = timeframe, one_day_intervals
    
    ninterval = (indx - 1) % cur_one_day_intervals
    dayindx = (indx - 1) // cur_one_day_intervals
    start_date = datetime.strptime(sorted_dates[dayindx] + start_session, '%d%m%Y%H%M')
    newdate = start_date + timedelta(minutes=(ninterval + 1) * cur_tf)
    
    _dt_obj_cache[indx] = newdate
    return newdate


def get_index_from_dtobj(dt_obj, sorted_dates, cur_tf_str=''):
    # Cache by dt_obj string representation and cur_tf_str (sorted_dates is constant)
    
    date_obj_str = dt_obj.strftime('%d%m%Y')
    file_time = dt_obj.strftime('%H%M')
    cache_key = (date_obj_str, file_time, cur_tf_str)
    
    if cache_key in _index_from_dtobj_cache:
        return _index_from_dtobj_cache[cache_key]

    cur_tf, cur_one_day_intervals = timeframe, one_day_intervals
    
    if cur_tf_str and cur_tf_str != timeframe_str:
        cur_tf, cur_one_day_intervals = get_one_day_intervals(cur_tf_str)

    nday = sorted_dates.index(date_obj_str)

    ceil_interval = calculate_intervals(end_time_str=file_time, tf=cur_tf)

    n_file = nday * cur_one_day_intervals + ceil_interval
    
    _index_from_dtobj_cache[cache_key] = n_file
    return n_file



# ================= CORE PROCESSING (UNCHANGED) =================

def post_process_files(curr_symb_vol_data,total_indices,from_index=1):
         # adjust volume
    for symb in curr_symb_vol_data:
        for file_num in range(from_index,total_indices+1):
            if (file_num-1) % one_day_intervals != 0:
                prev_day_vol = curr_symb_vol_data[symb][file_num-1][vol_cumul_indx]
                curr_vol = curr_symb_vol_data[symb][file_num][vol_cumul_indx] 
                if curr_vol > prev_day_vol:
                    curr_symb_vol_data[symb][file_num][vol_indx] = curr_vol - prev_day_vol



    calculate_rma('volume', 'volume_fast',curr_symb_vol_data,total_indices,8,from_index=from_index) # blue line fast
    # calculate sma
    calculate_rma('volume', 'volume_slow',curr_symb_vol_data,total_indices,21,from_index=from_index)  # red line slow

    tf_data = {'vol_data': curr_symb_vol_data}

    return tf_data


def get_slice_cache_data(symb_data,get_indx=-1):
    new_symb_data = {}

    last_indx = -1
    for _, data in symb_data.items():
        last_indx = len(data)-1
        break

    if get_indx == -1:
        get_indx = last_indx

    if get_indx >=1:
        for sym, data in symb_data.items():
            new_data = new_symb_data.setdefault(sym,{}).setdefault(get_indx , [None] * len(fields))
            if get_indx < len(data):
                new_data[vol_cumul_indx] = data[get_indx][ch_vol_cumul_indx]
                new_data[vol_indx] = data[get_indx][ch_vol_indx]
                new_data[vol_slow_indx] = data[get_indx][ch_vols_indx]
                new_data[vol_fast_indx] = data[get_indx][ch_volf_indx]
                new_data[ltp_indx] = data[get_indx][ch_ltp_indx]

    return get_indx, new_symb_data



def process_files():
    global CACHE, CACHE_READY, timeframe_str,timeframe, one_day_intervals

    min_tf_refresh = CACHE[MIN_TF]['processed']['refresh_time']
    cache_refresh_time = CACHE.get('refresh_time')

    incremental = CACHE_READY

    if timeframe != int(MIN_TF):
        if cache_refresh_time and min_tf_refresh and cache_refresh_time == min_tf_refresh:
            print(f'No updates')
            return None

        mintf_sorted_dates = CACHE[MIN_TF]['processed']['sorted_dates']
        mintf_total_indices = get_index_from_dtobj(min_tf_refresh,mintf_sorted_dates,MIN_TF)
        
        curr_symb_vol_data = {}
        from_indx = 1

        if incremental:
            from_indx = get_index_from_dtobj(CACHE[MIN_TF]['processed']['start_time'],mintf_sorted_dates)
            last_cache_indx = get_index_from_dtobj(cache_refresh_time,mintf_sorted_dates)

            if from_indx != last_cache_indx:
                from_indx = last_cache_indx +1


            _,curr_symb_vol_data = get_slice_cache_data(CACHE[timeframe_str]['symbols_data'],from_indx-1)
            


        min_tf_int = int(MIN_TF)
        tfratio = timeframe//min_tf_int
        total_indices = math.ceil(mintf_total_indices/tfratio)

        mintf_vol_data = CACHE[MIN_TF]['processed']['vol_data']

        for symb, mintf_data in mintf_vol_data.items():
            curr_symb_data = curr_symb_vol_data.setdefault(symb,{})
            
            for nfileindx in range(from_indx,total_indices+1):
                cur_data = curr_symb_data.setdefault(nfileindx,[None] * len(fields))
                min_tf_indx = min(nfileindx * tfratio, mintf_total_indices)

                if min_tf_indx in mintf_data:
                    cur_data[vol_cumul_indx] = mintf_data[min_tf_indx][vol_cumul_indx]
                    cur_data[vol_indx] = mintf_data[min_tf_indx][vol_cumul_indx]
                    cur_data[ltp_indx] = mintf_data[min_tf_indx][ltp_indx]
        
        tf_data = post_process_files(curr_symb_vol_data,total_indices,from_indx)

        first_dt_obj = get_dt_obj_from_fileindex(from_indx,mintf_sorted_dates)

        tf_data.update({'sorted_dates':mintf_sorted_dates ,'last_file_dt_obj':min_tf_refresh,'first_file_dt_obj':first_dt_obj})

        return tf_data


     # Process MIN_TF data files
               
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
            
            file_date = datetime.strptime(date_str, dt_frmt)
            nse_files_with_dates.append({
                'filename': filename,
                'date_str': file_date.strftime(dt_str_frmt),
                'date_obj': file_date
            })
            
            uniq_dates.add(file_date.date())


    sorted_files = sorted(nse_files_with_dates, key=lambda x: x['date_obj'])
    sorted_dates = [d.strftime('%d%m%Y') for d in sorted(uniq_dates)]

    
    incremental_sorted_files=[]
    cache_sym_data = None
    last_sorted_file_dt_obj = sorted_files[-1]['date_obj']
    first_sorted_file_dt_obj = sorted_files[0]['date_obj']

    if incremental:
        if cache_refresh_time and last_sorted_file_dt_obj > cache_refresh_time:
            for f in sorted_files:
                if f['date_obj'] > cache_refresh_time:
                    incremental_sorted_files.append(f)
        
        sorted_files = incremental_sorted_files


    tf_data = None
    start_indx= 1
    if sorted_files:
        if incremental:
            start_indx = get_index_from_dtobj(first_sorted_file_dt_obj,sorted_dates)
            last_cache_indx = get_index_from_dtobj(cache_refresh_time,sorted_dates)

            if start_indx != last_cache_indx:
                start_indx = last_cache_indx+1


            _,cache_sym_data = get_slice_cache_data(CACHE[timeframe_str]['symbols_data'],start_indx-1)
            
        print(f'Updating data for tf {timeframe}: New files {len(sorted_files)}')

        tf_data = process_csv_files(sorted_files, sorted_dates,incremental,cache_sym_data,start_indx)

    return tf_data
 

def fill_empty_interval_gaps(symbol_vol_data, total_files,from_index=1):
    """Fill missing intervals by interpolating from prev/next. O(symbols * missing)."""

    full_range = set(range(from_index, total_files + 1))

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
                def_data[vol_cumul_indx] += (next_avail_data[vol_cumul_indx] - def_data[vol_cumul_indx]) / gap
                def_data[vol_indx] = def_data[vol_cumul_indx]
                if (i % one_day_intervals) == 1:
                    def_data[ltp_indx] = next_avail_data[ltp_indx]
                else:
                    def_data[ltp_indx] += (next_avail_data[ltp_indx] - def_data[ltp_indx]) / gap

            sym_data[i] = def_data


def process_csv_files(sorted_files, sorted_dates, incremental_flag=False,cache_sym_data=None, from_index=1):

    symbol_vol_data = {}
    
    if incremental_flag:
        symbol_vol_data = cache_sym_data

    for finfo in sorted_files:
        fpath = finfo['filename']

        n_file = get_index_from_dtobj(finfo['date_obj'],sorted_dates)

        with open(fpath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row[symb_col].strip('"')
                ltp = float(row[ltp_col].strip('"'))
                vol = float(row[value_col])
                symb_data_list = symbol_vol_data.setdefault(symbol,{}).setdefault(n_file , [None] * len(fields))

                symb_data_list[vol_cumul_indx] = vol
                symb_data_list[vol_indx] = vol
                symb_data_list[ltp_indx] = ltp


    last_sorted_file_dt_obj = sorted_files[-1]['date_obj']
    
    total_files = get_index_from_dtobj(last_sorted_file_dt_obj,sorted_dates)
    
    fill_empty_interval_gaps(symbol_vol_data, total_files,from_index=from_index)

    first_index_dt_obj= get_dt_obj_from_fileindex(from_index,sorted_dates)

    # adjust volume
    tf_data = post_process_files(symbol_vol_data,total_files,from_index=from_index)
    tf_data.update({'sorted_dates':sorted_dates ,'last_file_dt_obj':last_sorted_file_dt_obj,'first_file_dt_obj':first_index_dt_obj})

    return tf_data


'''
def calculate_sma(data_field, target_data_field,symbol_vol_data,total_days,sma_len):
    # calculate sma
    data_indx = fields.index(data_field)
    tar_indx = fields.index(target_data_field)
    for symb in symbol_vol_data:
        for day_num in range(1,total_days+1):
            window_size = min(day_num, sma_len)
            window_start = max(1,  day_num - sma_len)
            window = []
            for i in range(window_start,day_num+1):
                window.append(symbol_vol_data[symb][i][data_indx] )

            
            symbol_vol_data[symb][day_num][tar_indx] = (sum(window) / len(window))


def calculate_ewma(data_field, target_data_field,symbol_vol_data,total_days,ewma_len):

    data_indx = fields.index(data_field)
    tar_indx = fields.index(target_data_field)
   # calculate ewma
    alpha = 2 / (ewma_len + 1)
    for symb in symbol_vol_data:
        symbol_vol_data[symb][1][tar_indx] = symbol_vol_data[symb][1][data_indx]
        for day_num in range(2,total_days+1):
            prev_ewma = symbol_vol_data[symb][day_num-1][tar_indx]
            curr_vol = symbol_vol_data[symb][day_num][data_indx]

            symbol_vol_data[symb][day_num][tar_indx] = alpha * curr_vol + (1 - alpha) * prev_ewma



def calculate_wma(data_field, target_data_field,symbol_vol_data,total_days,sma_len):
    data_indx = fields.index(data_field)
    tar_indx = fields.index(target_data_field)
    # calculate sma
    for symb in symbol_vol_data:
        for day_num in range(1,total_days+1):
            window_size = min(day_num, sma_len)
            window_start = max(1,  day_num - sma_len)
            window = []
            for i in range(window_start,day_num+1):
                window.append(symbol_vol_data[symb][i][data_indx])

            weights = list(range(1, window_size+1))
            weighted_sum = sum(w * p for w, p in zip(weights, window))

            symbol_vol_data[symb][day_num][tar_indx] = (weighted_sum / sum(weights))

'''

def calculate_rma(data_field, target_data_field,symbol_vol_data,total_days,ewma_len,from_index=1):
   # calculate ewma
    data_indx = fields.index(data_field)
    tar_indx = fields.index(target_data_field)

    alpha = 1 /ewma_len
    start_indx = (from_index +1) if from_index == 1 else from_index
    for symb in symbol_vol_data:
        if from_index == 1:
            symbol_vol_data[symb][1][tar_indx] = symbol_vol_data[symb][1][data_indx]

        for day_num in range(start_indx,total_days+1):
            prev_ewma = symbol_vol_data[symb][day_num-1][tar_indx]
            curr_vol = symbol_vol_data[symb][day_num][data_indx]
            symbol_vol_data[symb][day_num][tar_indx] = alpha * curr_vol + (1 - alpha) * prev_ewma



# ================= CACHE LOADER =================


def process_files_for_timeframe(tf_arg):
    """Run process_files for a specific timeframe ('3', '15', 'D'). Returns (symbols_data, symbols_avg)."""
    global timeframe, one_day_intervals , timeframe_str
 
    # Clear utils caches when timeframe changes
    
    clear_utils_caches()
    
    timeframe_str = tf_arg
    timeframe, one_day_intervals = get_one_day_intervals(tf_arg)

    tf_data = process_files()

    return _build_cache_entries(tf_data)
 

def _build_cache_entries(tf_data):
    """Build symbols_data and symbols_avg from processed symbol_vol_data."""

    ret_tf_data = None
    if tf_data:
        symbol_vol_data, refresh_time, start_time, sorted_dates = tf_data['vol_data'], tf_data['last_file_dt_obj'], tf_data['first_file_dt_obj'], tf_data['sorted_dates']

        
        start_indx = get_index_from_dtobj(start_time,sorted_dates)
        last_indx = get_index_from_dtobj(refresh_time,sorted_dates)


        symbols_data = {}

        for f_indx in range(start_indx, last_indx+1):
            ts_str = get_dt_obj_from_fileindex(f_indx,sorted_dates).strftime(dt_str_frmt)
            ts =ts_str.split('_')[0] if one_day_intervals == 1 else  ts_str.split('_')[-1] 
            for symb, vol_data in symbol_vol_data.items():
                row = vol_data[f_indx]
                if f_indx == 1:
                    symbols_data.setdefault(symb, []).append(cache_symbols_fields)
                
                data_list = [None] * len(cache_symbols_fields)

                data_list[ch_ts_indx] = ts
                data_list[ch_tsf_indx]= f'{f_indx}: {ts_str}'
                data_list[ch_vol_cumul_indx] = row[vol_cumul_indx]
                data_list[ch_vol_indx] = row[vol_indx]
                data_list[ch_vols_indx] = row[vol_slow_indx]
                data_list[ch_volf_indx] = row[vol_fast_indx]
                data_list[ch_ltp_indx] = row[ltp_indx]
                 
                symbols_data.setdefault(symb, []).append(data_list)


        symbols_avg = []
        symbols_avg.append(cache_index_fields)
        chix_symb_indx, chix_vols_indx = cache_index_fields.index('symbol'), cache_index_fields.index('volume_slow')
        chix_volf_indx, chix_volsurge_indx = cache_index_fields.index('volume_fast'), cache_index_fields.index('vol_surge')
        chix_ltp_indx, chix_vol_indx = cache_index_fields.index('ltp'), cache_index_fields.index('volume')

        for symb, indx_data_list in symbols_data.items():
            last_indx_data = indx_data_list[-1]

            slow = last_indx_data[ch_vols_indx]
            fast = last_indx_data[ch_volf_indx]

            data_list= [None] * len(cache_index_fields)

            data_list[chix_symb_indx] = symb
            data_list[chix_vols_indx] = slow
            data_list[chix_volf_indx] = fast
            data_list[chix_volsurge_indx] = (fast - slow)
            data_list[chix_ltp_indx] = last_indx_data[ch_ltp_indx]
            data_list[chix_vol_indx] = last_indx_data[ch_vol_indx]

            symbols_avg.append(data_list)
 
        symbols_avg[1:] = sorted(symbols_avg[1:],key=lambda x: x[chix_volsurge_indx], reverse=True)

        ret_tf_data = {'symbols_data':symbols_data , 'symbols_avg':symbols_avg}
        ret_tf_data.update(tf_data)

    return ret_tf_data


def clear_processed_cache():
    global CACHE
    # Safely clear and reinitialize the processed cache for MIN_TF
    processed = CACHE[MIN_TF].get('processed')
    if processed is not None:
        processed.clear()
    # Re-establish required keys, set to initial values
    processed['refresh_time'] = None
    processed['sorted_dates'] = None
    processed['start_time'] = None
    processed['vol_data'] = None


def _do_load_into_cache():
    """Load data from disk for all 3 timeframes and update CACHE. Caller must hold CACHE_LOCK."""
    global CACHE_READY
    print(f"⚙️ {datetime.now().strftime('%M:%S')} : Loading Data...")
    for tf in TF_KEYS:
        tf_data = process_files_for_timeframe(tf)
        CACHE[tf]["symbols_data"] = tf_data['symbols_data']
        CACHE[tf]["symbols_avg"] = tf_data['symbols_avg']
        if tf == MIN_TF:
            CACHE[MIN_TF]['processed']['start_time'] = tf_data.get('first_file_dt_obj')
            CACHE[MIN_TF]['processed']['refresh_time'] = tf_data.get('last_file_dt_obj')
            CACHE[MIN_TF]['processed']['sorted_dates'] = tf_data.get('sorted_dates')
            CACHE[MIN_TF]['processed']['vol_data'] = tf_data.get('vol_data')
    
  
    CACHE["refresh_time"] = CACHE[MIN_TF]['processed']['refresh_time']
    CACHE_READY = True

    clear_processed_cache()
    
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
    print(f"🔄 {datetime.now().strftime('%M:%S')} : Reloading data ...")
    global CACHE , CACHE_READY

    if not CACHE_READY:
        print(f'CACHE is not ready. No Refresh')
        return


    new_cache = {tf: {} for tf in TF_KEYS}
    
    # clear all
    #CACHE[MIN_TF]['processed_data']=None
    new_update = True
    for tf in TF_KEYS:
        tf_data = process_files_for_timeframe(tf)
        if not tf_data:
            new_update= False
            break
        
        new_cache[tf]["symbols_data"] = tf_data['symbols_data']
        new_cache[tf]["symbols_avg"] = tf_data['symbols_avg']

        if tf == MIN_TF:
            CACHE[MIN_TF]['processed']['start_time'] = tf_data.get('first_file_dt_obj')
            CACHE[MIN_TF]['processed']['refresh_time'] = tf_data.get('last_file_dt_obj')
            CACHE[MIN_TF]['processed']['sorted_dates'] = tf_data.get('sorted_dates')
            CACHE[MIN_TF]['processed']['vol_data'] = tf_data.get('vol_data')

    
    if new_update:     
        with CACHE_LOCK:
            for tf in TF_KEYS:
                merge_cache_data(CACHE[tf]["symbols_data"],new_cache[tf]["symbols_data"],tf)
                CACHE[tf]["symbols_avg"] = new_cache[tf]["symbols_avg"]
                
            CACHE["refresh_time"] = CACHE[MIN_TF]['processed']['refresh_time']

        print(f'Update with last file timestamp {CACHE["refresh_time"].strftime('%d%m%Y-%H%M')}')
        clear_processed_cache()
    else:
        print("No updates")

    print(f"✅ {datetime.now().strftime('%M:%S')} : Reload complete.")



def merge_cache_data(main_cache_data, incremental_data, tf_str):
    global CACHE
    refresh_time, start_time, sorted_dates = CACHE[MIN_TF]['processed']['refresh_time'], CACHE[MIN_TF]['processed']['start_time'], CACHE[MIN_TF]['processed']['sorted_dates']

    start_indx = get_index_from_dtobj(start_time,sorted_dates,tf_str)
    total_indices = get_index_from_dtobj(refresh_time,sorted_dates,tf_str)

    
    for sym, incre_data in incremental_data.items():
        orig_main_len = len(main_cache_data[sym])
        inc_indx = 0
        for indx in range(start_indx,total_indices+1):
            if indx < orig_main_len:
                main_cache_data[sym][indx] = incre_data[indx-start_indx]
                inc_indx+=1
            else:
                main_cache_data[sym].extend(incre_data[(indx-start_indx):])
                break


def dump_index(symbols_list,tf, ref_t,fut_flag=False):
    os.makedirs(OUT_DIR, exist_ok=True)
    ch_sym_ndx = cache_index_fields.index('symbol')
    fut_str= '1!' if fut_flag else ''
    out_file = f'{OUT_DIR}/candidates.txt'
    top=500
 
    with open(out_file, 'w') as out:
        count=-1
        out.write(f"Timeframe: {tf}  |  Refresh Time: {ref_t}\n")
        for sym_data in symbols_list:
            count+=1
            if count == 0:
                continue
            out.write(f"{sym_data[ch_sym_ndx]}{fut_str}\n")
            if count == top:
                break

def filter_list(symbols_list,filter):
    if filter:
        start, end = 0,0
        try:
            start, end = [int(l) for l in  filter.split('-')]
        except:
            filter=''

    if filter:
        newsymblist= [symbols_list[0]]
        chix_ltp_indx = cache_index_fields.index('ltp')
        for sym_data in symbols_list[1:]:
            if sym_data[chix_ltp_indx] and sym_data[chix_ltp_indx] >= start and sym_data[chix_ltp_indx] <= end:
                newsymblist.append(sym_data)
        
        return newsymblist

    return symbols_list



# ================= FLASK =================

app = Flask(__name__)

def periodic_reload():
    buffertime = 15  # seconds after scheduled wall clock minute
    print(f"🔁 Auto reload enabled: every {RELOAD_INTERVAL_MINUTES} minutes with buffertime={buffertime} seconds")

    while True:
        now = datetime.now()
        # minute and second components
        minutes = now.minute
        seconds = now.second

        min_per_hour = 60
        interval = RELOAD_INTERVAL_MINUTES

        # Calculate next wall clock interval
        next_trigger_minute = ((minutes // interval) + 1) * interval
        if next_trigger_minute >= min_per_hour:
            # Next trigger lands in next hour
            next_time = (now.replace(minute=0, second=0) 
                         + timedelta(hours=1, seconds=buffertime))
        else:
            # This hour
            next_time = now.replace(minute=next_trigger_minute, second=0) + timedelta(seconds=buffertime)

        wait_seconds = (next_time - now).total_seconds()
        if wait_seconds < 0.5:
            # Already passed, nudge to next interval
            if next_trigger_minute >= min_per_hour:
                next_time += timedelta(hours=1)
            else:
                next_time += timedelta(minutes=interval)
            wait_seconds = (next_time - now).total_seconds()

        time.sleep(wait_seconds)
        t0 = time.time()
        force_reload()
        print(f"⏱ Reload took {time.time()-t0:.2f}s")

def _tf_safe(tf):
    return tf if tf in TF_KEYS else MIN_TF

def _sort_safe(sort):
    return sort if sort in INDX_SORT_KEYS else ''


@app.route('/')
def index():
    load_data_once()
    tf = _tf_safe(request.args.get('tf', MIN_TF))
    filter = request.args.get('filter', '')

    symbols_list = filter_list(CACHE[tf]["symbols_avg"],filter)

    sort_key = _sort_safe(request.args.get('sort', 'vol_surge'))
    desc = True
    if request.args.get('order', '') == 'asc':
        desc =False

    order_by='desc' if desc else 'asc'

    if sort_key:
        symbols_list[1:] = sorted(symbols_list[1:],key=lambda x:  x[cache_index_fields.index(sort_key)], reverse=desc)
            
    refresh_dt_obj = CACHE.get("refresh_time")
    ref_t = refresh_dt_obj.strftime(refresh_dt_pat) if refresh_dt_obj else '-'

    fut_flag = True if filter else False 
    dump_index(symbols_list,tf,ref_t,fut_flag)

    return render_template('index.html', symbols=symbols_list, count=len(symbols_list), refresh_time=ref_t, timeframe=tf,sort=sort_key,order=order_by,filter=filter)



@app.route('/symbol/<symbol_name>')
def symbol_detail(symbol_name):
    load_data_once()
    tf = _tf_safe(request.args.get('tf', MIN_TF))
    symbols_data = CACHE[tf]["symbols_data"]
    if symbol_name not in symbols_data:
        return "Symbol not found", 404

    return render_template('symbol.html', symbol=symbol_name, data=symbols_data[symbol_name], timeframe=tf)

@app.route('/api/symbol/<symbol_name>')
def api_symbol(symbol_name):
    load_data_once()
    tf = _tf_safe(request.args.get('tf', MIN_TF))
    symbols_data = CACHE[tf]["symbols_data"]
    if symbol_name not in symbols_data:
        return jsonify({'error': 'Symbol not found'}), 404
    return jsonify({
        symb_col: symbol_name,
        'data': symbols_data[symbol_name]
    })

# ================= MAIN =================

if __name__ == '__main__':

    if not (len(sys.argv) == 2 or len(sys.argv) == 1):
        print("Usage: python app.py <reload_time>")
        sys.exit(1)

    if len(sys.argv) == 2:
        RELOAD_INTERVAL_MINUTES = int(sys.argv[1]) if sys.argv[1] != '0' else None


    if RELOAD_INTERVAL_MINUTES:
        t = threading.Thread(
            target=periodic_reload,
            daemon=True
        )
        t.start()

    t0 = time.time()
    load_data_once()
    print(f"⏱ Loading data took {time.time()-t0:.2f}s")

    app.template_folder = f'{TEMPLATES_PARENT_DIR}/template_vol'
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.jinja_env.auto_reload = True
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
