"""
app.py  –  NSE Intraday Flask Web Portal
-----------------------------------------
All routes and request arguments identical to the original.
"""

import os
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
from data_processor import INDEX_FIELDS, SYMB_COL
from cache_manager import CacheManager, MIN_TF, TF_KEYS
import argparse

from sys import path as _syspath
_syspath.append(os.path.abspath("../../")) # analysis dir
from web_scripts.data_scripts.sync_data import PARENT_DIR,REMOTE_DIR, OUT_DIR, NSE_INTRADAY_DIR_PATH, REMOTE_INTRADAY_DIR_PATH,NSE_DATA_DIR, \
    INTRADAY_DIR,TEMPLATES_PARENT_DIR, sync_data_args

from sector_loader import load_sector_symbols, UNIQ_CATEGORIES_CSV, CATEGORIES_CSV



# ── CONFIG ────────────────────────────────────────────────────────────────────

# ── Session config ─────────────────────────────────────────────────────────────
start_session = '0915'
end_session   = '1530'

RELOAD_INTERVAL_MINUTES = None
remote_sync_flag=False
REFRESH_DT_PAT = 'Date: %d  Time: %H:%M'

# ── GLOBALS ───────────────────────────────────────────────────────────────────

CACHE = CacheManager()

# ── FLASK ─────────────────────────────────────────────────────────────────────

app = Flask(__name__)

# ── HELPERS ───────────────────────────────────────────────────────────────────

def _tf_safe(tf):
    return tf if tf in TF_KEYS else MIN_TF


def _sort_safe(sort):
    return sort if sort in INDEX_FIELDS else ''


def filter_list(symbols_list, filt):
    start, end = 0, float('inf')
    pos_count = 0
    neg_count = 0
    neut_count = 0 

    if filt:
        try:
            start, end = [int(x) for x in filt.split('-')]
        except Exception:
            return symbols_list, pos_count, neg_count, neut_count
    
    ltp_idx = INDEX_FIELDS.index('ltp')
    pma_idx = INDEX_FIELDS.index('price_ma_action')

    filtered = [symbols_list[0]]
 
    for sym_data in symbols_list[1:]:
        val = sym_data[ltp_idx]
        if val is not None and start <= val <= end:
            pma = sym_data[pma_idx]
            if pma == 1:
                pos_count+=1
            elif pma == -1:
                neg_count +=1
            else:
                neut_count+=1
            filtered.append(sym_data)

    return filtered, pos_count, neg_count, neut_count


def dump_index(symbols_list, tf, ref_t, fut_flag=False):
    os.makedirs(OUT_DIR, exist_ok=True)
    sym_idx = INDEX_FIELDS.index('symbol')
    fut_str = '1!' if fut_flag else ''
    with open(f'{OUT_DIR}/candidates.txt', 'w') as out:
        out.write(f"Timeframe: {tf}  |  Refresh Time: {ref_t}\n")
        for sym_data in symbols_list[1:]:
            out.write(f"{sym_data[sym_idx]}{fut_str}\n")
    
 
def dump_merge(filt, sort_key, ref_t,order_by):
    import heapq
    fut_flag = bool(filt)
    merge_tf_list = ('3', '15')
    top = 25
    sym_idx = INDEX_FIELDS.index('symbol')
    sidx = INDEX_FIELDS.index('volume_fast') if not sort_key else INDEX_FIELDS.index(sort_key)

    # Use defaultdict for smarter max assignment, and avoid two-pass set logic
    from collections import defaultdict

    symbols_map = defaultdict(int)  # maps symbol -> max volume_fast (or sort_key val)

    # Inline per-tf processing to minimise storage
    for tf in merge_tf_list:
        symbols_list,_,_,_ = filter_list(CACHE.get_symbols_avg(tf), filt)
        # Only operate on the top N after sorting (skips making a copy of entire [1:])
        # Use heapq.nlargest for efficient top-N selection
        top_syms = heapq.nlargest(top, symbols_list[1:], key=lambda x: x[sidx])
        for sym_data in top_syms:
            sy = sym_data[sym_idx]
            val = sym_data[sidx]
            # Only keep the maximum value per symbol
            if val > symbols_map[sy]:
                symbols_map[sy] = val
            # No need to store all symbols, just propagate max in one dict

    # Now collect and output, sort once
    desc     = request.args.get('order', '') != 'asc'
    sorted_symbols = sorted(symbols_map.items(), key=lambda item: item[1], reverse=desc)
    fut_str = '1!' if fut_flag else ''
    with open(f'{OUT_DIR}/candidates_merge.txt', 'w') as out:
        out.write(f"Merge Timeframes: {merge_tf_list} | sorted by {sort_key} {order_by}  | Filter ltp {filt} | Refresh Time: {ref_t}\n")
        for sym, _ in sorted_symbols:
            out.write(f"{sym}{fut_str}\n")


def sync_data_from_remote():

    if not remote_sync_flag:
        return

    sync_data_args(REMOTE_INTRADAY_DIR_PATH,NSE_INTRADAY_DIR_PATH)
    

def _load():
    sync_data_from_remote()
    CACHE.load_files(NSE_INTRADAY_DIR_PATH)


# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.route('/')
def index(sector_list=None,sector_name=None):
    if not CACHE.is_ready:
        _load()

    tf       = _tf_safe(request.args.get('tf', MIN_TF))
    filt     = request.args.get('filter', '')
    sort_key = _sort_safe(request.args.get('sort', ''))
    desc     = request.args.get('order', '') != 'asc'
    order_by = 'desc' if desc else 'asc'

    
    symbols_list,pos,neg,neut = filter_list(CACHE.get_symbols_avg(tf), filt) if not sector_list else filter_list(sector_list, filt)

    if sort_key:
        sidx = INDEX_FIELDS.index(sort_key)
        symbols_list[1:] = sorted(symbols_list[1:], key=lambda x: x[sidx], reverse=desc)

    refresh_dt_obj = CACHE.get_refresh_time()
    ref_t = refresh_dt_obj.strftime(REFRESH_DT_PAT) if refresh_dt_obj else '-'

    dump_index(symbols_list, tf, ref_t, fut_flag=bool(filt))
    dump_merge(filt,sort_key,ref_t,order_by)

    return render_template(
        'index.html',
        symbols=symbols_list,
        count=len(symbols_list),
        refresh_time=ref_t,
        timeframe=tf,
        sort=sort_key,
        order=order_by,
        filter=filt,
        pos_count=pos,
        neg_count=neg,
        neut_count=neut,
        sector_name=sector_name
    )


@app.route('/symbol/<symbol_name>')
def symbol_detail(symbol_name):
    if not CACHE.is_ready:
        _load()

    tf           = _tf_safe(request.args.get('tf', MIN_TF))
    symbols_data = CACHE.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        return "Symbol not found", 404
    return render_template(
        'symbol.html',
        symbol=symbol_name,
        data=symbols_data[symbol_name],   # lazy conversion on first access
        timeframe=tf,
    )


@app.route('/api/symbol/<symbol_name>')
def api_symbol(symbol_name):
    if not CACHE.is_ready:
        _load()

    tf           = _tf_safe(request.args.get('tf', MIN_TF))
    symbols_data = CACHE.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        return jsonify({'error': 'Symbol not found'}), 404
    return jsonify({
        SYMB_COL: symbol_name,
        'data':   symbols_data[symbol_name],
    })



@app.route('/sectors/<sector>')
@app.route('/sectors/<sector>/')
def sector_index(sector):
    """
    Sector-wise average volume_fast heatmap.
    Reads all_categories.csv for sector -> symbol mapping, then looks up
    each symbol's volume_fast from the cache for the requested timeframe.
    """
    if not CACHE.is_ready:
        _load()

    tf = _tf_safe(request.args.get('tf', MIN_TF))

    # ── load sector -> symbol mapping from CSV ─────────────────────────────────
    uniq_cat_flag = 'uniq_cat' in request.args
    csv_path = CATEGORIES_CSV if  not uniq_cat_flag else UNIQ_CATEGORIES_CSV
    sector_symbols = load_sector_symbols(csv_path=csv_path)


    all_syms_data = CACHE.get_symbols_avg(tf)

    sector_syms_set = set(sector_symbols.get(sector,[]))
    sector_list = []
    sector_list.append(all_syms_data[0])
    sym_idx = INDEX_FIELDS.index('symbol')

    for sym_data in all_syms_data[1:]:
        if sym_data[sym_idx] in sector_syms_set:
            sector_list.append(sym_data)
    
    return index(sector_list=sector_list,sector_name=sector)

    




@app.route('/sectors/')
@app.route('/sectors')
def sectors():
    """
    Sector-wise average volume_fast heatmap.
    Reads all_categories.csv for sector -> symbol mapping, then looks up
    each symbol's volume_fast from the cache for the requested timeframe.
    """
    if not CACHE.is_ready:
        _load()

    tf = _tf_safe(request.args.get('tf', MIN_TF))

    # ── load sector -> symbol mapping from CSV ─────────────────────────────────
    uniq_cat_flag = 'uniq_cat' in request.args
    csv_path = CATEGORIES_CSV if  not uniq_cat_flag else UNIQ_CATEGORIES_CSV
    sector_symbols = load_sector_symbols(csv_path=csv_path)

    # Build a fast lookup dict: symbol -> volume_fast value
    sort_key = _sort_safe(request.args.get('sort', 'volume_fast'))
    order_by = request.args.get('order', 'desc')
    desc_flag     = order_by != 'asc'

    sort_v_idx  = INDEX_FIELDS.index(sort_key)
    sym_idx = INDEX_FIELDS.index('symbol')

    filt     = request.args.get('filter', '')
    avg_rows,_,_,_ = filter_list(CACHE.get_symbols_avg(tf), filt)

    vol_lookup: dict = {}
    for row in avg_rows[1:]:        # skip header row
        sym = row[sym_idx]
        val = row[sort_v_idx]
        if sym and val is not None:
            vol_lookup[sym] = val

    # ── compute per-sector stats ───────────────────────────────────────────────
    sector_list = []
    for sector_name, syms in sector_symbols.items():
        if not syms:
            continue
        
        syms = [s for s in syms if s in vol_lookup]

        vols = [vol_lookup[s] for s in syms]
        avg_vol = (sum(vols) / len(vols)) if vols else None

        top_sym = None
        
        sorted_syms = sorted(syms, key=lambda s: vol_lookup[s], reverse=desc_flag)

        if sorted_syms:
            top_sym = sorted_syms[0]

            
        if len(sorted_syms) > 0:
            sector_list.append({
                'name':              sector_name,
                'symbols':           sorted_syms,
                'symbol_count':      len(sorted_syms),
                'avg_volume_fast':   round(avg_vol, 2) if avg_vol is not None else None,
                'top_symbol':        top_sym,
                'heat_pct':          0.0,   # normalised below
            })

    # ── normalise heat_pct to [0, 1] for CSS colouring ────────────────────────
    valid_vols = [s['avg_volume_fast'] for s in sector_list if s['avg_volume_fast'] is not None]
    if valid_vols:
        min_v, max_v = min(valid_vols), max(valid_vols)
        span = (max_v - min_v) or 1
        for s in sector_list:
            s['heat_pct'] = ((s['avg_volume_fast'] - min_v) / span) \
                if s['avg_volume_fast'] is not None else 0.0

    # Default sort: highest avg volume_fast first
    sector_list.sort(key=lambda s: s['avg_volume_fast'] or 0, reverse=desc_flag)

    refresh_dt_obj = CACHE.get_refresh_time()
    ref_t = refresh_dt_obj.strftime(REFRESH_DT_PAT) if refresh_dt_obj else '-'

    
 
    return render_template(
        'sectoral_index.html',
        sectors=sector_list,
        timeframe=tf,
        refresh_time=ref_t,
        filter=filt,
        sort= sort_key,
        order=order_by
    )




# ── PERIODIC RELOAD ───────────────────────────────────────────────────────────

def periodic_reload():
    buffertime = 15
    print(f"🔁 Reloads every {RELOAD_INTERVAL_MINUTES} minutes: Bufferime: {buffertime} seconds")

    while True:
        now      = datetime.now()
        interval = RELOAD_INTERVAL_MINUTES
        next_trigger = ((now.minute // interval) + 1) * interval
        if next_trigger >= 60:
            next_time = now.replace(minute=0, second=0) + timedelta(hours=1, seconds=buffertime)
        else:
            next_time = now.replace(minute=next_trigger, second=0) + timedelta(seconds=buffertime)

        wait = (next_time - now).total_seconds()
        if wait < 0.5:
            next_time += timedelta(minutes=interval)
            wait = (next_time - now).total_seconds()

        time.sleep(wait)

        t0 = time.time()
        # Do not reload after 15:30
        current_time = datetime.now().time()
        start_session_time = datetime.strptime(start_session, "%H%M").time()
        cutoff = datetime.strptime(end_session, "%H%M").time()
        if current_time > cutoff or current_time < start_session_time:
            print(f"⏹ Reload skipped: Session time {start_session} to {end_session}. Current time: {current_time.strftime('%H%M')}")
            continue
        _load()
        print(f"⏱ Reload took {time.time()-t0:.2f}s")


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='NSE Intraday Flask Web Portal')
    parser.add_argument('-ri', '--reload-interval', type=int, help='Reload interval in minutes')
    parser.add_argument('-sy', '--sync', action='store_true', help='Enable remote sync')
    parser.add_argument('-pd', '--parent-dir', type=str, default=None, help='Parent dir of nse_data')
    parser.add_argument('-rd', '--remote-dir', type=str, default=None, help='Remote dir of nse_data')



    # Only parse args if running as main script (not under flask reloader)
    args, unknown = parser.parse_known_args()

    if args.reload_interval is not None:
        RELOAD_INTERVAL_MINUTES = args.reload_interval

    if args.sync:
        print("remote sync enabled")
        remote_sync_flag = True

    if args.parent_dir:
        PARENT_DIR = os.path.abspath(args.parent_dir)
        NSE_INTRADAY_DIR_PATH = f'{PARENT_DIR}/{NSE_DATA_DIR}/{INTRADAY_DIR}'

    if args.remote_dir:
        REMOTE_DIR = os.path.abspath(args.remote_dir)
        REMOTE_INTRADAY_DIR_PATH = f'{REMOTE_DIR}/{NSE_DATA_DIR}/{INTRADAY_DIR}'

    if RELOAD_INTERVAL_MINUTES:
        threading.Thread(target=periodic_reload, daemon=True).start()

  
    t0 = time.time()
    _load()
    print(f"⏱ Loading data took {time.time()-t0:.2f}s")

    app.template_folder = f'{TEMPLATES_PARENT_DIR}/template_vol'
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.jinja_env.auto_reload = True
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
