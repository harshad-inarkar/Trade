"""
data_processor.py
-----------------
Core stateless NSE processing, fully vectorised with NumPy.

Internal storage
----------------
All per-symbol time-series are held as a single (N_SYMBOLS, TOTAL_INTERVALS, 5)
float64 ndarray.  The 5 columns map to VOL_CUMUL / LTP / VOL / VOL_SLOW / VOL_FAST.
This avoids building O(N * T) Python list objects and cuts cache-build time by ~100x.

Gap-fill
--------
The original algorithm (interpolate between prev and next available value within
the same trading day, default to 0 at day-start) is reproduced exactly using
np.interp per day-segment row.  Results are numerically identical to the
original dict-based implementation.

Output format
-------------
symbols_data[sym]   -> tuple (CACHE_FIELDS header, ts_list, tsf_list, numpy_matrix)
                       where numpy_matrix is a (TOTAL, 5) float64 view.
                       The /symbol/<n> route converts to list-of-lists lazily.
symbols_avg         -> list-of-lists identical to the original (header + one row per sym).
"""

import csv
import glob
import math
import os
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor



# ── field schemas ─────────────────────────────────────────────────────────────

VOL_CUMUL    = 0   # index into per-interval row
PRICE        = 1
VOL          = 2
VOL_SLOW     = 3
VOL_FAST     = 4
VOL_BASE     = 5
PRICE_FAST = 6   # price RMA (fast period, same as rma_fast_len)
PRICE_SLOW = 7   # price RMA (slow period, same as rma_slow_len)
NFIELDS      = 8

# Cache / output field orders
CACHE_FIELDS = ['timestamp_full', 'timestamp', 'volume_cumulative', 'volume',
                'volume_slow', 'volume_fast', 'volume_base', 'ltp',
                'ltp_rma_fast', 'ltp_rma_slow']
CH_TSF, CH_TS, CH_VCUM, CH_VOL, CH_VSLOW, CH_VFAST, CH_VBASE, CH_PRICE = 0, 1, 2, 3, 4, 5, 6, 7
CH_PRICE_FAST, CH_PRICE_SLOW = 8, 9
N_CACHE = len(CACHE_FIELDS)


INDEX_FIELDS = ['symbol', 'volume_fast', 'vol_surge', 'ltp','price_surge','price_ma_action']   # price_ma_action -1, 0 ,1
IX_SYM, IX_VFAST, IX_SURGE, IX_LTP, IX_PS, IX_PMA  = 0, 1, 2, 3, 4, 5
N_INDEX = len(INDEX_FIELDS)

# ── CSV column names ──────────────────────────────────────────────────────────

SYMB_COL  = 'symbol'
VALUE_COL = 'vol_cum'
LTP_COL   = 'price'

# ── session / date constants ──────────────────────────────────────────────────

START_SESSION = '0915'
END_SESSION   = '1530'
DATE_PATTERN  = r'.*(\d{2})(\d{2})(\d{4})/nse_data_(\d{2})(\d{2}).csv'
DT_FRMT       = '%d%m%Y%H%M'
DT_STR_FRMT   = '%d/%m_%H%M'


# --- RMA Len -----

rma_fast_len = 8 
rma_slow_len = 21
rma_base_len = 89  # 55, 89, 144, 233

new_symb_map = {'LTIM' : 'LTM'}
# Workers for parallel file reading.  I/O-bound, so more than cpu_count
# is fine.  Cap at 16 to avoid hammering the filesystem.
_READ_WORKERS = min(16, (os.cpu_count() or 4) + 4)

# ── helpers ───────────────────────────────────────────────────────────────────

def calculate_intervals(tf, start_time_str=START_SESSION, end_time_str=END_SESSION):
    start = datetime.strptime(start_time_str, '%H%M')
    end   = datetime.strptime(end_time_str,   '%H%M')
    if start >= end:
        return 0
    total_minutes = (end - start).total_seconds() / 60
    return math.ceil(total_minutes / tf)
    

def check_valid_session(curr_time):
    return 0 < calculate_intervals(tf=1, end_time_str=curr_time) <= calculate_intervals(tf=1)


def get_one_day_intervals(tf_str):
    """Return (tf_int, intervals_per_day) for '3', '15', or 'D'."""
    tf = calculate_intervals(tf=1) if tf_str == 'D' else int(tf_str)
    return tf, calculate_intervals(tf=tf)


def get_dt_obj_from_fileindex(indx, sorted_dates, tf, odi):
    ninterval  = (indx - 1) % odi
    dayindx    = (indx - 1) // odi
    start_date = datetime.strptime(sorted_dates[dayindx] + START_SESSION, '%d%m%Y%H%M')
    return start_date + timedelta(minutes=(ninterval + 1) * tf)


def get_index_from_dtobj(dt_obj, sorted_dates, tf, odi):
    nday          = sorted_dates.index(dt_obj.strftime('%d%m%Y'))
    ceil_interval = calculate_intervals(end_time_str=dt_obj.strftime('%H%M'),tf=tf)
    return nday * odi + ceil_interval


# ── file discovery ────────────────────────────────────────────────────────────

def discover_files(data_dir,last_n_days=None):
    """Scan data_dir for valid CSVs.  Returns (sorted_files, sorted_dates)."""
    csv_files = glob.glob(os.path.join(data_dir, '**/*.csv'), recursive=True)
    files_with_dates = []
    uniq_dates = set()

    for filename in csv_files:
        m = re.match(DATE_PATTERN, filename)
        if not m:
            continue
        date_str  = ''.join(m.groups())
        if not check_valid_session(date_str[-4:]):
            continue
        file_date = datetime.strptime(date_str, DT_FRMT)
        files_with_dates.append({'filename': filename,
                                  'date_str': file_date.strftime(DT_STR_FRMT),
                                  'date_obj': file_date})
        uniq_dates.add(file_date.date())

    sorted_files = sorted(files_with_dates, key=lambda x: x['date_obj'])
    sorted_dates_all = [d.strftime('%d%m%Y') for d in sorted(uniq_dates)]
    if last_n_days is not None and last_n_days > 0 and len(sorted_dates_all) > last_n_days:
        use_dates = set(sorted_dates_all[-last_n_days:])
        sorted_files = [f for f in sorted_files if f['date_obj'].strftime('%d%m%Y') in use_dates]
        sorted_dates = sorted_dates_all[-last_n_days:]
        
    else:
        sorted_dates = sorted_dates_all

    print(f'Start Date : {sorted_dates[0]}')
    return sorted_files, sorted_dates


# ── NEW HELPER (add before read_csv_files_to_arrays) ──────────────────────────
 
def _read_single_file(finfo, sorted_dates, tf, odi,
                      sym_to_idx, known_symbols,
                      vcum_arr, vltp_arr):
    """
    Read one CSV snapshot and write values directly into the pre-allocated
    shared arrays.
 
    Thread-safety guarantee
    ───────────────────────
    Each file maps to a *unique* column index `n_file` via
    get_index_from_dtobj().  Therefore no two threads ever write to the
    same memory location.  NumPy element-wise assignment to non-overlapping
    locations is safe under concurrent access.
 
    In full-load mode (known_symbols is None) the symbol list is already
    fixed before this function is called, so no array resizing occurs.
 
    Returns
    ───────
    set of symbol strings that were present in the file but absent from
    sym_to_idx (i.e. excluded symbols for this file).
    """
    n_file   = get_index_from_dtobj(finfo['date_obj'], sorted_dates, tf, odi)
    excluded = set()
 
    try:
        # pandas C-parser is ~10× faster than csv.DictReader for this workload
        # and releases the GIL during I/O, giving real parallel benefit.
        df = pd.read_csv(
            finfo['filename'],
            encoding='utf-8-sig',
            dtype={SYMB_COL: str},     # keep symbol as string; parse numerics below
            usecols=[SYMB_COL, VALUE_COL, LTP_COL],
        )
    except Exception as exc:
        print(f"Warning: could not read {finfo['filename']}: {exc}")
        return excluded
 
    # ── symbol mapping ────────────────────────────────────────────────────────
    syms    = df[SYMB_COL].str.strip('"').map(lambda s: new_symb_map.get(s, s))
    indices = syms.map(sym_to_idx)          # NaN for unknown symbols
 
    unknown_mask = indices.isna()
    if known_symbols is None and unknown_mask.any():
        excluded.update(syms[unknown_mask].tolist())
 
    valid = ~unknown_mask
    if not valid.any():
        return excluded
 
    # ── vectorised write into shared arrays ───────────────────────────────────
    si_arr    = indices[valid].astype(int).values
 
    vcum_vals = pd.to_numeric(df.loc[valid, VALUE_COL], errors='coerce').values
    # LTP column may be quoted (e.g. "1234.5") in some file variants
    ltp_series = df.loc[valid, LTP_COL].astype(str).str.strip('"')
    vltp_vals  = pd.to_numeric(ltp_series, errors='coerce').values
 
    # Single vectorised assignment – one C-level memcpy per array column
    vcum_arr[si_arr, n_file] = vcum_vals
    vltp_arr[si_arr, n_file] = vltp_vals
 
    return excluded
 
 
def _read_chunk(chunk_items, sym_to_idx, vcum_arr, vltp_arr):
    """
    Process a slice of (n_file, finfo) pairs.
 
    Thread-safety
    ─────────────
    Each file maps to a unique column n_file.  No two threads write to the
    same column, so no locking is needed on vcum_arr / vltp_arr.
    File open/read releases the GIL → threads genuinely overlap on I/O.
 
    Uses csv.reader + pre-computed column indices instead of DictReader
    to avoid creating a Python dict (~300 bytes) for every row.
 
    Returns a set of symbol strings that were in the file but absent from
    sym_to_idx (excluded symbols for this chunk).
    """
    excluded = set()
 
    for n_file, finfo in chunk_items:
        try:
            with open(finfo['filename'], 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                header = next(reader)
 
                # Locate column indices once per file, not once per row.
                sym_i = header.index(SYMB_COL)
                val_i = header.index(VALUE_COL)
                ltp_i = header.index(LTP_COL)
 
                for row in reader:
                    if len(row) <= max(sym_i, val_i, ltp_i):
                        continue                          # malformed row
 
                    sy  = row[sym_i].strip('"')
                    sym = new_symb_map.get(sy, sy)
                    si  = sym_to_idx.get(sym)
                    if si is None:
                        excluded.add(sym)
                        continue
 
                    try:
                        vcum_arr[si, n_file] = float(row[val_i])
                        vltp_arr[si, n_file] = float(row[ltp_i].strip('"'))
                    except ValueError:
                        pass                              # skip unparseable rows
 
        except Exception as exc:
            print(f"Warning: could not read {finfo['filename']}: {exc}")
 
    return excluded
 
 
# ── REPLACEMENT for read_csv_files_to_arrays ──────────────────────────────────
# Drop-in replacement – signature is identical to the original.
 
def read_csv_files_to_arrays(sorted_files, sorted_dates, tf, odi,
                              from_index=1, known_symbols=None):
    """
    Read CSV files → (sym_list, vcum_arr, vltp_arr, total_files).
 
    Full load  : parallel chunk-based reading (I/O bottleneck → real speedup).
    Incremental: sequential, unchanged from original (only a handful of new
                 files; dynamic vstack for new symbols must stay serial).
    """
    print(f'Updating data for {len(sorted_files)} files')
 
    # ── Pre-compute every file's column index ONCE ────────────────────────────
    # Original code called get_index_from_dtobj() inside the loop; that
    # function does sorted_dates.index() (O(N) scan) + 4 datetime operations
    # per file.  Replacing the O(N) scan with an O(1) dict lookup and hoisting
    # the datetime math out of the hot loop eliminates ~24 000 datetime ops.
    dates_to_idx = {d: i for i, d in enumerate(sorted_dates)}
 
    def fast_col_index(dt_obj):
        nday          = dates_to_idx[dt_obj.strftime('%d%m%Y')]
        ceil_interval = calculate_intervals(end_time_str=dt_obj.strftime('%H%M'), tf=tf)
        return nday * odi + ceil_interval
 
    file_col = {
        finfo['filename']: fast_col_index(finfo['date_obj'])
        for finfo in sorted_files
    }
 
    total_files = file_col[sorted_files[-1]['filename']]
 
    # ── Symbol universe ───────────────────────────────────────────────────────
    if known_symbols is not None:
        sym_list   = list(known_symbols)
        sym_to_idx = {s: i for i, s in enumerate(sym_list)}
    else:
        sym_set          = set()
        previous_dt_file = sorted_files[-1]
        for i in range(len(sorted_files) - 1, 0, -1):
            if (sorted_files[i    ]['date_obj'].strftime('%d%m%Y') !=
                sorted_files[i - 1]['date_obj'].strftime('%d%m%Y')):
                previous_dt_file = sorted_files[i - 1]
                break
 
        print(f"Symbols List From : {previous_dt_file['date_obj']}")
        with open(previous_dt_file['filename'], 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            sym_i  = header.index(SYMB_COL)
            for row in reader:
                if len(row) > sym_i:
                    sy = row[sym_i].strip('"')
                    sym_set.add(new_symb_map.get(sy, sy))
 
        sym_list   = sorted(sym_set)
        sym_to_idx = {s: i for i, s in enumerate(sym_list)}
 
    n_syms   = len(sym_list)
    vcum_arr = np.full((n_syms, total_files + 1), np.nan)
    vltp_arr = np.full((n_syms, total_files + 1), np.nan)
    excluded_sym_set: set = set()
 
    # ── INCREMENTAL: sequential (unchanged logic) ─────────────────────────────
    if known_symbols is not None:
        for finfo in sorted_files:
            n_file = file_col[finfo['filename']]
            with open(finfo['filename'], 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                header = next(reader)
                sym_i  = header.index(SYMB_COL)
                val_i  = header.index(VALUE_COL)
                ltp_i  = header.index(LTP_COL)
                for row in reader:
                    if len(row) <= max(sym_i, val_i, ltp_i):
                        continue
                    sy  = row[sym_i].strip('"')
                    sym = new_symb_map.get(sy, sy)
                    si  = sym_to_idx.get(sym)
                    if si is None:
                        # New symbol: must vstack (requires serial execution)
                        si = len(sym_list)
                        sym_list.append(sym)
                        sym_to_idx[sym] = si
                        vcum_arr = np.vstack([vcum_arr, np.full((1, vcum_arr.shape[1]), np.nan)])
                        vltp_arr = np.vstack([vltp_arr, np.full((1, vltp_arr.shape[1]), np.nan)])
                    try:
                        vcum_arr[si, n_file] = float(row[val_i])
                        vltp_arr[si, n_file] = float(row[ltp_i].strip('"'))
                    except ValueError:
                        pass
 
        if excluded_sym_set:
            print(f'Excluded symbols {excluded_sym_set}')
        return sym_list, vcum_arr, vltp_arr, total_files
 
    # ── FULL LOAD: chunked parallel I/O ──────────────────────────────────────
    # Pair each file with its pre-computed column index.
    items = [(file_col[finfo['filename']], finfo) for finfo in sorted_files]
 
    # Divide into _READ_WORKERS equal chunks.
    # Each thread processes its slice serially with csv.reader.
    # GIL is released during file open/read → genuine I/O parallelism.
    chunk_size = max(1, len(items) // _READ_WORKERS)
    chunks = [items[i: i + chunk_size] for i in range(0, len(items), chunk_size)]
 
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        futures = [
            executor.submit(_read_chunk, chunk, sym_to_idx, vcum_arr, vltp_arr)
            for chunk in chunks
        ]
        for future in futures:
            try:
                excluded_sym_set.update(future.result())
            except Exception as exc:
                print(f"Chunk error: {exc}")
 
    if excluded_sym_set:
        print(f'Excluded symbols {excluded_sym_set}')
 
    return sym_list, vcum_arr, vltp_arr, total_files


def fill_gaps_numpy(vcum_1indexed, vltp_1indexed, from_index, total_files, odi):
    """
    Fill NaN gaps in vcum and vltp in a single pass.

    At every day-start col:
      - vcum: use 0 as virtual left anchor so interp ramps from 0 toward the
              first real cumulative value; restore the original value after interp
              so the committed seed is not overwritten.
      - vltp: the previous day's last col is included in the segment (start_db=db-1)
              so np.interp naturally uses it as the left anchor.

    All other gaps: interp between prev/next known; forward-fill trailing NaN.

    Incremental: from_index decremented by 1 to include the committed anchor col.
    """
    if from_index > 1:
        from_index = from_index - 1

    n_cols = total_files - from_index + 1
    if n_cols <= 0:
        return

    vcum_work = vcum_1indexed[:, from_index: total_files + 1].copy()
    vltp_work = vltp_1indexed[:, from_index: total_files + 1].copy()

    def _interp_seg(seg):
        n, seg_len = seg.shape
        if seg_len == 0:
            return
        idx_f = np.arange(seg_len, dtype=float)
        for si in range(n):
            row = seg[si]
            finite = np.isfinite(row)
            if finite.all():
                continue
            if not finite.any():
                row[:] = 0.0
                continue
            seg[si] = np.interp(idx_f, idx_f[finite], row[finite])

    # Build day-boundary list (0-indexed into work)
    day_boundaries = []
    c = 0
    pos_in_day = (from_index - 1) % odi
    if pos_in_day == 0:
        day_boundaries.append(0)
        c = odi
    else:
        day_boundaries.append(0)
        c = odi - pos_in_day
    while c < n_cols:
        day_boundaries.append(c)
        c += odi

    for i, db in enumerate(day_boundaries):
        next_db  = day_boundaries[i + 1] if i + 1 < len(day_boundaries) else n_cols
        # Include previous day's last col as left anchor for vltp interp.
        # For db==0 (first segment) there is no previous col; start at 0.
        start_db = db - 1 if db > 0 else 0

        vcum_seg = vcum_work[:, start_db:next_db]
        vltp_seg = vltp_work[:, start_db:next_db]
        if vcum_seg.shape[1] == 0:
            continue

        abs_col      = from_index + db
        is_day_start = (abs_col - 1) % odi == 0

        if is_day_start and  db > 0 :
            # Save real value, plant 0 as left anchor, interp, then restore.
            # This makes vcum ramp from 0 toward next known without corrupting
            # the committed seed stored at start_db.
            # save_vcum = vcum_seg[:, 0].copy()
            save_vcum = vcum_seg[:, 0].copy()
            vcum_seg[:, 0] = 0.0


        if is_day_start and db == 0:
            vcum_seg[:, 0] = np.where(np.isnan(vcum_seg[:, 0]), 0.0, vcum_seg[:, 0])


  

        _interp_seg(vcum_seg)
        _interp_seg(vltp_seg)

        if is_day_start and db > 0:   # skip restore when db==0
            vcum_seg[:, 0] = save_vcum

    vcum_1indexed[:, from_index: total_files + 1] = vcum_work
    vltp_1indexed[:, from_index: total_files + 1] = vltp_work


# ── volume delta ──────────────────────────────────────────────────────────────

def compute_volume_delta(vcum_1indexed, from_index, total_files, odi):
    """
    Return vol array (same shape as vcum_1indexed) with per-interval volume deltas.
    Day-start intervals keep cumulative value (match original: vol = cumul at start).
    """
    vol = vcum_1indexed.copy()
    for fi in range(from_index, total_files + 1):
        if (fi - 1) % odi != 0:
            prev = vcum_1indexed[:, fi - 1]
            curr = vcum_1indexed[:, fi]
            delta = curr - prev
            vol[:, fi] = np.where(delta > 0, delta, 0)
    return vol


# ── RMA (Wilder's moving average) ─────────────────────────────────────────────

def compute_rma(vol_1indexed, from_index, total_files, period, rma_seed=None):
    """
    Compute RMA (Wilder MA).  Returns result array (same shape as vol_1indexed).

    Full load (from_index==1): seed col 1 with vol[:,1], accumulate from col 2.
    Incremental: if rma_seed is provided it already has the warm-start value at
    from_index-1; accumulate from from_index onward.
    """
    alpha = 1 / period

    if rma_seed is not None:
        rma   = rma_seed          # seed already planted at from_index-1
        start = from_index
    elif from_index == 1:
        rma        = np.zeros_like(vol_1indexed)
        rma[:, 1]  = vol_1indexed[:, 1]
        start      = 2
    else:
        rma                      = np.zeros_like(vol_1indexed)
        rma[:, from_index - 1]   = vol_1indexed[:, from_index - 1]
        start                    = from_index

    for i in range(start, total_files + 1):
        rma[:, i] = alpha * vol_1indexed[:, i] + (1 - alpha) * rma[:, i - 1]
    return rma


# ── post-process ──────────────────────────────────────────────────────────────

def compute_price_rma(vltp_1indexed, from_index, total_files, period, rma_seed=None):
    """
    RMA (Wilder MA) over price (ltp).  Mirrors compute_rma but seeds from ltp
    instead of vol so the warm-start carries correctly across session boundaries.

    Full load  (from_index == 1): seed col 1 with ltp[:, 1], accumulate from 2.
    Incremental: rma_seed is a pre-allocated array with the warm-start value
                 already planted at from_index-1; accumulate from from_index.
    """
    alpha = 1.0 / period

    if rma_seed is not None:
        rma   = rma_seed          # seed already planted at from_index-1
        start = from_index
    elif from_index == 1:
        rma        = np.zeros_like(vltp_1indexed)
        rma[:, 1]  = vltp_1indexed[:, 1]
        start      = 2
    else:
        rma                     = np.zeros_like(vltp_1indexed)
        rma[:, from_index - 1]  = vltp_1indexed[:, from_index - 1]
        start                   = from_index

    for i in range(start, total_files + 1):
        rma[:, i] = alpha * vltp_1indexed[:, i] + (1 - alpha) * rma[:, i - 1]
    return rma


def post_process(vcum_1indexed, from_index, total_files, odi,
                 seed_rma_fast=None, seed_rma_slow=None, seed_rma_base=None,
                 seed_price_rma_fast=None, seed_price_rma_slow=None,
                 vltp_1indexed=None, seed_slot=1):
    """
    Given filled vcum (and optionally vltp), return:
        (vol, rma_fast, rma_slow, rma_base, price_rma_fast, price_rma_slow)

    Price RMAs are computed when vltp_1indexed is provided; otherwise they are
    returned as zero arrays so callers that don't need them pay no cost.

    For incremental calls pass the corresponding seed_* ((N,2) arrays) and
    seed_slot (0 = second-to-last, 1 = last).  Seeds are planted at
    from_index-1 so compute_rma / compute_price_rma warm-start correctly.
    """
    vol = compute_volume_delta(vcum_1indexed, from_index, total_files, odi)

    if seed_rma_fast is not None and from_index > 1:
        n = min(seed_rma_fast.shape[0], vol.shape[0])
        rma_fast = np.zeros_like(vol)
        rma_slow = np.zeros_like(vol)
        rma_base = np.zeros_like(vol)
        rma_fast[:n, from_index - 1] = seed_rma_fast[:n, seed_slot]
        rma_slow[:n, from_index - 1] = seed_rma_slow[:n, seed_slot]
        rma_base[:n, from_index - 1] = seed_rma_base[:n, seed_slot]
        rma_fast = compute_rma(vol, from_index, total_files, rma_fast_len, rma_seed=rma_fast)
        rma_slow = compute_rma(vol, from_index, total_files, rma_slow_len, rma_seed=rma_slow)
        rma_base = compute_rma(vol, from_index, total_files, rma_base_len, rma_seed=rma_base)
    else:
        rma_fast = compute_rma(vol, from_index, total_files, rma_fast_len)
        rma_slow = compute_rma(vol, from_index, total_files, rma_slow_len)
        rma_base = compute_rma(vol, from_index, total_files, rma_base_len)

    # ── price RMAs ────────────────────────────────────────────────────────────
    if vltp_1indexed is not None:
        if seed_price_rma_fast is not None and from_index > 1:
            n = min(seed_price_rma_fast.shape[0], vltp_1indexed.shape[0])
            prf = np.zeros_like(vltp_1indexed)
            prs = np.zeros_like(vltp_1indexed)
            prf[:n, from_index - 1] = seed_price_rma_fast[:n, seed_slot]
            prs[:n, from_index - 1] = seed_price_rma_slow[:n, seed_slot]
            price_rma_fast = compute_price_rma(vltp_1indexed, from_index, total_files, rma_fast_len, rma_seed=prf)
            price_rma_slow = compute_price_rma(vltp_1indexed, from_index, total_files, rma_slow_len, rma_seed=prs)
        else:
            price_rma_fast = compute_price_rma(vltp_1indexed, from_index, total_files, rma_fast_len)
            price_rma_slow = compute_price_rma(vltp_1indexed, from_index, total_files, rma_slow_len)
    else:
        price_rma_fast = np.zeros_like(vol)
        price_rma_slow = np.zeros_like(vol)

    return vol, rma_fast, rma_slow, rma_base, price_rma_fast, price_rma_slow


# ── timestamp generation ──────────────────────────────────────────────────────

def build_timestamps(from_index, total_files, sorted_dates, tf, odi):
    """Return (ts_list, tsf_list) for indices from_index..total_files (1-indexed)."""
    is_daily = (odi == 1)
    ts_list  = []
    tsf_list = []
    for fi in range(from_index, total_files + 1):
        dt_obj = get_dt_obj_from_fileindex(fi, sorted_dates, tf, odi)
        ts_str = dt_obj.strftime(DT_STR_FRMT)
        ts     = ts_str.split('_')[0] if is_daily else ts_str.split('_')[-1]
        ts_list.append(ts)
        tsf_list.append(f'{fi}: {ts_str}')
    return ts_list, tsf_list


# ── symbols_avg builder ───────────────────────────────────────────────────────


def rma(arr, length):
    """
    Computes the Running Moving Average (RMA) of a 1D numpy array.
    If input is 2D, computes along axis=1.
    """
    arr = np.asarray(arr)
    if arr.ndim == 1:
        out = np.zeros_like(arr, dtype=np.float64)
        n = arr.shape[0]
        if n == 0:
            return out
        out[0] = arr[0]
        alpha = 1.0 / length
        for i in range(1, n):
            out[i] = (1 - alpha) * out[i - 1] + alpha * arr[i]
        return out
    elif arr.ndim == 2:
        out = np.zeros_like(arr, dtype=np.float64)
        for row in range(arr.shape[0]):
            out[row] = rma(arr[row], length)
        return out
    else:
        raise ValueError("Input array must be 1D or 2D.")



def calc_pma(ps, fa, sig):
    pma = 0

    if ps > 0 and fa > sig:
        pma = 1
    elif ps < 0 and fa < sig:
        pma = -1

    return pma


def build_symbols_avg(sym_list, num_data, last_col_idx):
    """
    Build symbols_avg list-of-lists from last interval of num_data.
    num_data shape: (N_SYMS, N_INTERVALS, 8)
      cols: VCUM, LTP, VOL, VSLOW, VFAST, VBASE, LTP_RMA_FAST, LTP_RMA_SLOW
    last_col_idx: 0-based index into axis 1 for the last valid interval.
    """
 

    last  = num_data[:, last_col_idx, :]
          # (N_SYMS, 8)
    vbase_last = last[:, VOL_BASE]
    vslow_last = last[:, VOL_SLOW] # (N_SYMS,)
    vfast_last = last[:, VOL_FAST]   # (N_SYMS,)
    ltp = last[:, PRICE]
  
    pfast_last = last[:, PRICE_FAST]
    pslow_last = last[:, PRICE_SLOW]


    # Safe division: pre-fill result with 0 then overwrite only where denominator != 0.
    # This avoids the RuntimeWarning that np.where triggers because both branches
    # are evaluated eagerly even when the condition is False.
    vslow = np.zeros(len(sym_list), dtype=np.float64)
    vfast = np.zeros(len(sym_list), dtype=np.float64)
    nonzero_base = vbase_last != 0
    vslow[nonzero_base] = (vslow_last[nonzero_base] * 100) / vbase_last[nonzero_base]
    vfast[nonzero_base] = (vfast_last[nonzero_base] * 100) / vbase_last[nonzero_base]



    vol_surge = 1000 * (vfast_last - vslow_last) / vslow_last
    price_surge = 1000 * (pfast_last - pslow_last) / pslow_last


    result = [INDEX_FIELDS]
    for si in range(len(sym_list)):
        row = [None] * N_INDEX
        row[IX_SYM]   = sym_list[si]
        row[IX_VFAST] = round(vfast[si],2)                        # already 1-D scalar
        row[IX_SURGE] = round(vol_surge[si],2)             # last interval of smooth series
        row[IX_LTP]   = round(ltp[si],2)
        ps            = float(price_surge[si])       # last interval
        row[IX_PS]    = round(ps,2)
        row[IX_PMA]   = calc_pma(ps, pfast_last[si], pslow_last[si])
        result.append(row)
    return result


# ── lazy export (for /symbol/<n> route) ───────────────────────────────────────

def numpy_to_cache_rows(header, ts_list, tsf_list, num_matrix):
    """
    Convert a (TOTAL, 8) numpy matrix + timestamp lists to list-of-lists
    in CACHE_FIELDS order:
        [tsf, ts, vcum, vol, vslow, vfast, vbase, ltp, ltp_rma_fast, ltp_rma_slow].
    Called lazily on the symbol detail route.
    """
    rows = [header]
    # num_matrix columns: [VCUM, LTP, VOL, VSLOW, VFAST, VBASE, LTP_RMA_FAST, LTP_RMA_SLOW]
    for i in range(num_matrix.shape[0]):
        row = [None] * N_CACHE
        row[CH_TSF]          = tsf_list[i]
        row[CH_TS]           = ts_list[i]
        row[CH_VCUM]         = float(num_matrix[i, VOL_CUMUL])
        row[CH_VOL]          = float(num_matrix[i, VOL])
        base = float(num_matrix[i, VOL_BASE])
        row[CH_VBASE]        = base
        row[CH_VSLOW]        = 0 if base == 0 else (float(num_matrix[i, VOL_SLOW]) * 100) / base
        row[CH_VFAST]        = 0 if base == 0 else (float(num_matrix[i, VOL_FAST]) * 100) / base
        row[CH_PRICE]          = float(num_matrix[i, PRICE])
        row[CH_PRICE_FAST] = float(num_matrix[i, PRICE_FAST])
        row[CH_PRICE_SLOW] = float(num_matrix[i, PRICE_SLOW])
        rows.append(row)
    return rows