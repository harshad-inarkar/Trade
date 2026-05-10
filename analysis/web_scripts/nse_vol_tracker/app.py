"""
app.py  –  NSE Intraday FastAPI Web Portal
------------------------------------------
Migrated from Flask to FastAPI.

Key changes vs the Flask version
─────────────────────────────────
• Jinja2Templates replaces render_template(); every TemplateResponse receives
  the FastAPI Request object so Jinja2 can call url_for() etc. from templates.
• Query-string parameters are declared as typed function arguments with
  Query() – no more request.args.get() scattered through the code.
• JSON routes return plain dicts; FastAPI serialises them automatically.
  HTTPException replaces the old (body, 404) two-tuple pattern.
• A single @asynccontextmanager lifespan hook replaces the lazy
  `if not CACHE.is_ready: _load()` guard that was duplicated in every route.
• sector_index() used to call the index() route function directly.
  That pattern breaks with FastAPI's dependency injection, so the shared
  rendering logic now lives in _render_index_response() which both routes call.
• app.template_folder was set at runtime in __main__; here the Jinja2Templates
  instance is rebuilt during startup once the CLI argument is known.
• uvicorn.run() replaces app.run(); all CLI flags are preserved.

Run:
    python app.py [options]
    uvicorn app:app --host 0.0.0.0 --port 5000
"""

import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import argparse
import uvicorn

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates

from data_processor import INDEX_FIELDS, SYMB_COL
from cache_manager import CacheManager, MIN_TF, TF_KEYS

from utils.utility import wait_next_wall_clock

from utils.data.paths import (
    ROOT_DIR, REMOTE_DIR, OUT_DIR,
    NSE_INTRADAY_DIR_PATH, REMOTE_INTRADAY_DIR_PATH,
    NSE_DATA_DIR, INTRADAY_DIR, TEMPLATES_PARENT_DIR
)

from utils.data.sync_data import sync_data_args
from sector_loader import load_sector_symbols, UNIQ_CATEGORIES_CSV, CATEGORIES_CSV


# ── CONFIG ─────────────────────────────────────────────────────────────────────


start_session = '0915'
end_session   = '1530'

RELOAD_INTERVAL_MINUTES = None
buffertime = 15

merge_filter_ltp = None
merge_sort_key_list   = None
last_n_days      = None
remote_sync_flag = False
REFRESH_DT_PAT   = 'Date: %d  Time: %H:%M'


# ── GLOBALS ────────────────────────────────────────────────────────────────────

CACHE = CacheManager()

# Rebuilt at startup once the CLI --parent-dir / template path is known.
# Defaulting here so type checkers are happy; overwritten before first request.
templates = Jinja2Templates(directory=f'{TEMPLATES_PARENT_DIR}/template_vol')


# ── HELPERS ────────────────────────────────────────────────────────────────────

def _tf_safe(tf: str) -> str:
    return tf if tf in TF_KEYS else MIN_TF


def _sort_safe(sort: str) -> str:
    return sort if sort in INDEX_FIELDS else 'volume_fast'


def filter_list(symbols_list, filt: str):
    start, end = 0, float('inf')
    pos_count = neg_count = neut_count = 0

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
                pos_count += 1
            elif pma == -1:
                neg_count += 1
            else:
                neut_count += 1
            filtered.append(sym_data)

    return filtered, pos_count, neg_count, neut_count


def dump_index():
    print('Dump Full Index')
    refresh_dt_obj = CACHE.get_refresh_time()
    ref_t = refresh_dt_obj.strftime(REFRESH_DT_PAT) if refresh_dt_obj else '-'

    tf = MIN_TF

    symbols_list = CACHE.get_symbols_avg(tf)

    os.makedirs(OUT_DIR, exist_ok=True)
    sym_idx = INDEX_FIELDS.index('symbol')
    with open(f'{OUT_DIR}/candidates.txt', 'w') as out:
        for sym_data in symbols_list[1:]:
            out.write(f"{sym_data[sym_idx]}\n")
        
        out.write(f"Timeframes: {tf} | Refresh Time: {ref_t}\n")


def dump_merge(tf,filt: str, sort_key_list,ref_t: str, order_by: str, from_web: bool = False):
    import heapq
    import csv as _csv
    from collections import defaultdict

    fut_flag     = bool(filt)

    top          = 40
    sym_idx      = INDEX_FIELDS.index('symbol')
    

    desc           = order_by != 'asc'
    fut_str        =  '' #'1!' if fut_flag else ''
    initiator      = 'Web' if from_web else 'Refresh'

    full_symb_list = CACHE.get_symbols_avg(tf)

    # Read list of symbols from 'candidates_force.txt'
    force_syms_path = os.path.join(OUT_DIR, 'candidates_force.txt')
    forced_symbols = set()
    if os.path.exists(force_syms_path):
        with open(force_syms_path, 'r') as f:
            for line in f:
                sym = line.strip()
                if sym:
                    forced_symbols.add(sym)

    with open(os.path.join(OUT_DIR, 'sym_table.csv'), 'w', newline='') as out_csv, \
         open(os.path.join(OUT_DIR, 'candidates_merge.txt'), 'w') as out:

        writer = _csv.writer(out_csv)
        writer.writerow(INDEX_FIELDS)

 
        # 1. Build the merged map (your existing logic)
        symbol_full_row_map: dict = {}
        for skey in sort_key_list:
            if skey:
                sidx = INDEX_FIELDS.index(_sort_safe(skey))
                symbols_list, _, _, _ = filter_list(full_symb_list, filt)
                # 'top' here is likely 30 as per your description
                top_syms = heapq.nlargest(top, symbols_list[1:], key=lambda x: x[sidx])
                for row in top_syms:
                    sy = row[sym_idx]
                    symbol_full_row_map[sy] = row

        # 2. Identify indices for both keys
        sidx1 = INDEX_FIELDS.index(_sort_safe(sort_key_list[0]))

        # 3. Create Rank Maps for the merged pool
        # We rank descending (reverse=True) so the highest value gets rank 1
        items = list(symbol_full_row_map.items())
        
        sorted_sidx1 = sorted(items, key=lambda x: x[1][sidx1], reverse=True)
        sidx1_rank_map = {item[0]: rank for rank, item in enumerate(sorted_sidx1, 1)}

        sidx2_rank_map = {}
        sidx2_exist = bool(sort_key_list[1])
        if sidx2_exist:
            sidx2 = INDEX_FIELDS.index(_sort_safe(sort_key_list[1]))
            sorted_sidx2 = sorted(items, key=lambda x: x[1][sidx2], reverse=True)
            sidx2_rank_map = {item[0]: rank for rank, item in enumerate(sorted_sidx2, 1)}

        # 4. Generate the Final Sorted List based on Composite Rank
        # Logic: Score = Rank_Avg + Rank_Surge (Lowest score = Best performer)
        sorted_symbols = sorted(
            symbol_full_row_map.items(),
            key=lambda item: sidx1_rank_map[item[0]] + (0 if not sidx2_exist else sidx2_rank_map[item[0]]),
            reverse=False  # Crucial: Lower rank sum is better
        )


        # Write the forced_symbols set contents to the output file
        if forced_symbols:
            for fsym in sorted(forced_symbols):
                out.write(f"{fsym}\n")

        for sym, _ in sorted_symbols:
            row = symbol_full_row_map.get(sym)
            if row is not None:
                writer.writerow(row)
                out.write(f"{sym}{fut_str}\n")

        out.write(
            f"{initiator}|Timeframes: {tf} | sorted by {sort_key_list} {order_by}"
            f"  | Filter ltp {filt} | Refresh Time: {ref_t}\n"
        )


def sync_data_from_remote():
    if not remote_sync_flag:
        return
    sync_data_args(REMOTE_INTRADAY_DIR_PATH, NSE_INTRADAY_DIR_PATH)


def _load():
    sync_data_from_remote()
    CACHE.load_files(NSE_INTRADAY_DIR_PATH, last_n_days)
    refresh_dt_obj = CACHE.get_refresh_time()
    ref_t = refresh_dt_obj.strftime(REFRESH_DT_PAT) if refresh_dt_obj else '-'
    dump_merge(MIN_TF,merge_filter_ltp,merge_sort_key_list, ref_t, 'desc')
    


# ── SHARED RENDER HELPER ───────────────────────────────────────────────────────
# Extracted so both GET / and GET /sectors/{sector} can call it without one
# route function calling the other (which breaks FastAPI's Request injection).

def _render_index_response(
    request: Request,
    tf: str,
    filt: str,
    sort_key: str,
    order_by: str,
    sector_list=None,
    sector_name: Optional[str] = None,
):
    desc = order_by != 'asc'

    symbols_list, pos, neg, neut = (
        filter_list(sector_list, filt)
        if sector_list is not None
        else filter_list(CACHE.get_symbols_avg(tf), filt)
    )

    if sort_key:
        sidx = INDEX_FIELDS.index(sort_key)
        symbols_list[1:] = sorted(symbols_list[1:], key=lambda x: x[sidx], reverse=desc)

    refresh_dt_obj = CACHE.get_refresh_time()
    ref_t = refresh_dt_obj.strftime(REFRESH_DT_PAT) if refresh_dt_obj else '-'

    dump_merge(tf,filt, [sort_key,None], ref_t, order_by, from_web=True)

    return templates.TemplateResponse(
        request,
        'index.html',
        {
            'symbols':     symbols_list,
            'count':       len(symbols_list),
            'refresh_time': ref_t,
            'timeframe':   tf,
            'sort':        sort_key,
            'order':       order_by,
            'filter':      filt,
            'pos_count':   pos,
            'neg_count':   neg,
            'neut_count':  neut,
            'sector_name': sector_name,
        },
    )


# ── LIFESPAN ───────────────────────────────────────────────────────────────────
# Replaces the `if not CACHE.is_ready: _load()` guard that was duplicated
# inside every Flask route handler.

@asynccontextmanager
async def lifespan(app: FastAPI):
    t0 = time.time()
    _load()
    dump_index()
    print(f"⏱ Loading data took {time.time() - t0:.2f}s")
    yield
    # Nothing to clean up on shutdown.


# ── APP ────────────────────────────────────────────────────────────────────────

app = FastAPI(title='NSE Intraday Portal', lifespan=lifespan)


# ── ROUTES ─────────────────────────────────────────────────────────────────────

@app.get('/')
def index(
    request: Request,
    tf:    str = Query(default=MIN_TF),
    filter: str = Query(default=''),   # noqa: A002 – kept for URL compat
    sort:  str = Query(default=''),
    order: str = Query(default=''),
):
    tf       = _tf_safe(tf)
    sort_key = _sort_safe(sort)
    order_by = 'asc' if order == 'asc' else 'desc'
    return _render_index_response(request, tf, filter, sort_key, order_by)


@app.get('/symbol/{symbol_name}')
def symbol_detail(
    request: Request,
    symbol_name: str,
    tf: str = Query(default=MIN_TF),
):
    tf           = _tf_safe(tf)
    symbols_data = CACHE.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        raise HTTPException(status_code=404, detail='Symbol not found')
    return templates.TemplateResponse(
        request,
        'symbol.html',
        {
            'symbol':    symbol_name,
            'data':      symbols_data[symbol_name],   # lazy conversion on first access
            'timeframe': tf,
        },
    )


@app.get('/api/symbol/{symbol_name}')
def api_symbol(
    symbol_name: str,
    tf: str = Query(default=MIN_TF),
):
    tf           = _tf_safe(tf)
    symbols_data = CACHE.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        raise HTTPException(status_code=404, detail='Symbol not found')
    return {SYMB_COL: symbol_name, 'data': symbols_data[symbol_name]}


@app.get('/sectors/{sector}')
def sector_index(
    request: Request,
    sector: str,
    tf:       str = Query(default=MIN_TF),
    uniq_cat: bool = Query(default=False),  # present as ?uniq_cat in Flask
    filter:   str = Query(default=''),
    sort:     str = Query(default=''),
    order:    str = Query(default=''),
):
    """
    Sector-specific symbol listing.
    Filters CACHE to only the symbols that belong to *sector*, then delegates
    to the shared _render_index_response() helper (same output as the / route).
    """
    tf       = _tf_safe(tf)
    sort_key = _sort_safe(sort)
    order_by = 'asc' if order == 'asc' else 'desc'

    csv_path       = UNIQ_CATEGORIES_CSV if uniq_cat else CATEGORIES_CSV
    sector_symbols = load_sector_symbols(csv_path=csv_path)

    all_syms_data  = CACHE.get_symbols_avg(tf)
    sector_syms_set = set(sector_symbols.get(sector, []))
    sym_idx        = INDEX_FIELDS.index('symbol')

    sector_list = [all_syms_data[0]]  # header row
    for sym_data in all_syms_data[1:]:
        if sym_data[sym_idx] in sector_syms_set:
            sector_list.append(sym_data)

    return _render_index_response(request, tf, filter, sort_key, order_by,
                                  sector_list=sector_list, sector_name=sector)


@app.get('/sectors')
@app.get('/sectors/')
def sectors(
    request: Request,
    tf:       str = Query(default=MIN_TF),
    uniq_cat: bool = Query(default=False),
    filter:   str = Query(default=''),
    sort:     str = Query(default=''),
    order:    str = Query(default='desc'),
):
    """
    Sector heatmap – shows one row per sector with avg volume_fast + top symbol.
    """
    tf       = _tf_safe(tf)
    sort_key = _sort_safe(sort)
    order_by = 'asc' if order == 'asc' else 'desc'
    desc_flag = order_by != 'asc'

    csv_path       = UNIQ_CATEGORIES_CSV if uniq_cat else CATEGORIES_CSV
    sector_symbols = load_sector_symbols(csv_path=csv_path)

    sort_v_idx = INDEX_FIELDS.index(sort_key)
    sym_idx    = INDEX_FIELDS.index('symbol')

    avg_rows, _, _, _ = filter_list(CACHE.get_symbols_avg(tf), filter)

    vol_lookup: dict = {}
    for row in avg_rows[1:]:
        sym = row[sym_idx]
        val = row[sort_v_idx]
        if sym and val is not None:
            vol_lookup[sym] = val

    sector_list = []
    for sector_name, syms in sector_symbols.items():
        if not syms:
            continue

        syms = [s for s in syms if s in vol_lookup]
        vols = [vol_lookup[s] for s in syms]
        if not vols:
            continue

        avg_vol     = sum(vols) / len(vols)
        sorted_syms = sorted(syms, key=lambda s: vol_lookup[s], reverse=desc_flag)
        top_sym     = sorted_syms[0] if sorted_syms else None

        sector_list.append({
            'name':            sector_name,
            'symbols':         sorted_syms,
            'symbol_count':    len(sorted_syms),
            'avg_volume_fast': round(avg_vol, 2),
            'top_symbol':      top_sym,
            'heat_pct':        0.0,   # normalised below
        })

    # Normalise heat_pct → [0, 1] for CSS colouring
    valid_vols = [s['avg_volume_fast'] for s in sector_list if s['avg_volume_fast'] is not None]
    if valid_vols:
        min_v, max_v = min(valid_vols), max(valid_vols)
        span = (max_v - min_v) or 1
        for s in sector_list:
            s['heat_pct'] = (
                (s['avg_volume_fast'] - min_v) / span
                if s['avg_volume_fast'] is not None else 0.0
            )

    sector_list.sort(key=lambda s: s['avg_volume_fast'] or 0, reverse=desc_flag)

    refresh_dt_obj = CACHE.get_refresh_time()
    ref_t = refresh_dt_obj.strftime(REFRESH_DT_PAT) if refresh_dt_obj else '-'

    return templates.TemplateResponse(
        request,
        'sectoral_index.html',
        {
            'sectors':      sector_list,
            'timeframe':    tf,
            'refresh_time': ref_t,
            'filter':       filter,
            'sort':         sort_key,
            'order':        order_by,
        },
    )


# ── PERIODIC RELOAD ────────────────────────────────────────────────────────────


def periodic_reload():
    print(f"🔁 Reloads every {RELOAD_INTERVAL_MINUTES} minutes – buffer: {buffertime}s")
    while True:
        wait_next_wall_clock(RELOAD_INTERVAL_MINUTES, buffertime)
        current_time       = datetime.now().time()
        start_session_time = datetime.strptime(start_session, '%H%M').time()
        cutoff             = datetime.strptime(end_session,   '%H%M').time()
        if current_time > cutoff or current_time < start_session_time:
            print(
                f"⏹ Reload skipped: outside session {start_session}–{end_session}. "
                f"Current: {current_time.strftime('%H%M')}"
            )
            continue
        t0 = time.time()
        _load()
        print(f"⏱ Reload took {time.time() - t0:.2f}s")


# ── MAIN ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NSE Intraday FastAPI Web Portal')
    parser.add_argument('-ri', '--reload-interval', type=int,
                        help='Reload interval in minutes')
    parser.add_argument('-sy', '--sync', action='store_true',
                        help='Enable remote sync')
    parser.add_argument('-pd', '--parent-dir', type=str, default=None,
                        help='Parent dir of nse_data')
    parser.add_argument('-rd', '--remote-dir', type=str, default=None,
                        help='Remote dir of nse_data')
    parser.add_argument('-fl', '--filter-ltp', type=str, default='400-6000',
                        help='Merge File Filter Ltp range e.g. 500-1000')
    parser.add_argument('-s1', '--sort-key1', type=str, default='volume_fast',
                        help=f'Merge File Sort Key e.g. {INDEX_FIELDS}')
    parser.add_argument('-s2', '--sort-key2', type=str, default='vol_surge',
                        help=f'Merge File Sort Key e.g. {INDEX_FIELDS}')
    parser.add_argument('-dy', '--last-ndays', type=int, default=None,
                        help='Last n days data')

    args, _ = parser.parse_known_args()

    if args.reload_interval is not None:
        RELOAD_INTERVAL_MINUTES = args.reload_interval

    if args.sync:
        print('Remote sync enabled')
        remote_sync_flag = True

    if args.parent_dir:
        ROOT_DIR             = os.path.abspath(args.parent_dir)
        NSE_INTRADAY_DIR_PATH  = f'{ROOT_DIR}/{NSE_DATA_DIR}/{INTRADAY_DIR}'

    if args.remote_dir:
        REMOTE_DIR                 = os.path.abspath(args.remote_dir)
        REMOTE_INTRADAY_DIR_PATH   = f'{REMOTE_DIR}/{NSE_DATA_DIR}/{INTRADAY_DIR}'

    merge_filter_ltp = args.filter_ltp
    merge_sort_key_list   = [args.sort_key1,args.sort_key2]
    last_n_days      = args.last_ndays

    # Point Jinja2 at the correct template folder (may depend on --parent-dir).
    TEMPLATES_PARENT_DIR = os.path.join(ROOT_DIR,'web_scripts/templates')
    templates = Jinja2Templates(directory=os.path.join(TEMPLATES_PARENT_DIR,'template_vol'))

    if RELOAD_INTERVAL_MINUTES:
        threading.Thread(target=periodic_reload, daemon=True).start()

    uvicorn.run(app, host='0.0.0.0', port=5000, log_level='info')