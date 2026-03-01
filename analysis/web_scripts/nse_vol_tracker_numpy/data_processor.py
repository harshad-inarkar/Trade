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

# ── field schemas ─────────────────────────────────────────────────────────────

VOL_CUMUL = 0   # index into per-interval row
LTP_F     = 1
VOL       = 2
VOL_SLOW  = 3
VOL_FAST  = 4
NFIELDS   = 5

# Cache / output field orders
CACHE_FIELDS = ['timestamp_full', 'timestamp', 'volume_cumulative', 'volume',
                'volume_slow', 'volume_fast', 'ltp']
CH_TSF, CH_TS, CH_VCUM, CH_VOL, CH_VSLOW, CH_VFAST, CH_LTP = 0, 1, 2, 3, 4, 5, 6
N_CACHE = 7


INDEX_FIELDS = ['symbol', 'volume_slow', 'volume_fast', 'vol_surge', 'ltp', 'volume']
IX_SYM, IX_VSLOW, IX_VFAST, IX_SURGE, IX_LTP, IX_VOL = 0, 1, 2, 3, 4, 5

# ── CSV column names ──────────────────────────────────────────────────────────

SYMB_COL  = 'symbol'
VALUE_COL = 'vol_val_cum'
LTP_COL   = 'price'

# ── session / date constants ──────────────────────────────────────────────────

START_SESSION = '0915'
END_SESSION   = '1530'
DATE_PATTERN  = r'.*(\d{2})(\d{2})(\d{4})/nse_data_(\d{2})(\d{2}).csv'
DT_FRMT       = '%d%m%Y%H%M'
DT_STR_FRMT   = '%d_%H%M'


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

def discover_files(data_dir):
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
    sorted_dates = [d.strftime('%d%m%Y') for d in sorted(uniq_dates)]

    return sorted_files, sorted_dates


# ── CSV reading → dense numpy arrays ─────────────────────────────────────────

def read_csv_files_to_arrays(sorted_files, sorted_dates, tf, odi,
                              from_index=1, known_symbols=None):
    """
    Read CSV files and return:
        sym_list   : list[str]  (stable order, matching array axis 0)
        vcum_arr   : float64 (N_SYMS, TOTAL_INTERVALS)  – NaN where absent
        vltp_arr   : float64 (N_SYMS, TOTAL_INTERVALS)  – NaN where absent
        total_files: int
    """

    print(f'Updating data for {len(sorted_files)} files')
    # First pass: discover all symbols so we can pre-allocate
    last_dt     = sorted_files[-1]['date_obj']
    first_dt    = sorted_files[0]['date_obj']
    total_files = get_index_from_dtobj(last_dt, sorted_dates, tf, odi)

    # Symbol ordering: preserve existing order if given (incremental mode)
    if known_symbols is not None:
        sym_list   = list(known_symbols)
        sym_to_idx = {s: i for i, s in enumerate(sym_list)}
    else:
        # Two-pass: discover symbols first
        sym_set = set()
        for finfo in sorted_files:
            with open(finfo['filename'], 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sym_set.add(row[SYMB_COL].strip('"'))
        sym_list   = sorted(sym_set)
        sym_to_idx = {s: i for i, s in enumerate(sym_list)}

    n_syms   = len(sym_list)

    vcum_arr = np.full((n_syms, total_files + 1), np.nan)  # 1-indexed (col 0 unused)
    vltp_arr = np.full((n_syms, total_files + 1), np.nan)

    for finfo in sorted_files:
        n_file = get_index_from_dtobj(finfo['date_obj'], sorted_dates, tf, odi)
        with open(finfo['filename'], 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = row[SYMB_COL].strip('"')
                si  = sym_to_idx.get(sym)
                if si is None:
                    # new symbol seen in incremental mode
                    si = len(sym_list)
                    sym_list.append(sym)
                    sym_to_idx[sym] = si
                    # expand arrays
                    vcum_arr = np.vstack([vcum_arr, np.full((1, vcum_arr.shape[1]), np.nan)])
                    vltp_arr = np.vstack([vltp_arr, np.full((1, vltp_arr.shape[1]), np.nan)])
                vcum_arr[si, n_file] = float(row[VALUE_COL])
                vltp_arr[si, n_file] = float(row[LTP_COL].strip('"'))

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

def post_process(vcum_1indexed, from_index, total_files, odi,
                 seed_rma_fast=None, seed_rma_slow=None, seed_slot=1):
    """
    Given filled vcum, return (vol, rma_fast, rma_slow) arrays (1-indexed).

    For incremental calls, pass seed_rma_fast/slow ((N,2) arrays saved from the
    last two committed cols) and seed_slot (0 = second-to-last, 1 = last).
    Seeds are planted at from_index-1 so compute_rma warm-starts correctly.
    """
    vol = compute_volume_delta(vcum_1indexed, from_index, total_files, odi)

    if seed_rma_fast is not None and from_index > 1:
        n = min(seed_rma_fast.shape[0], vol.shape[0])
        # Pre-allocate rma arrays and plant seed at from_index-1
        rma_fast = np.zeros_like(vol)
        rma_slow = np.zeros_like(vol)
        rma_fast[:n, from_index - 1] = seed_rma_fast[:n, seed_slot]
        rma_slow[:n, from_index - 1] = seed_rma_slow[:n, seed_slot]
        # compute_rma will read from_index-1 as its warm-start
        rma_fast = compute_rma(vol, from_index, total_files, 8,  rma_seed=rma_fast)
        rma_slow = compute_rma(vol, from_index, total_files, 21, rma_seed=rma_slow)
    else:
        rma_fast = compute_rma(vol, from_index, total_files, 8)
        rma_slow = compute_rma(vol, from_index, total_files, 21)

    return vol, rma_fast, rma_slow


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

def build_symbols_avg(sym_list, num_data, last_col_idx):
    """
    Build symbols_avg list-of-lists from last interval of num_data.
    num_data shape: (N_SYMS, N_INTERVALS, 5)  cols: VCUM, LTP, VOL, VSLOW, VFAST
    last_col_idx: 0-based index into axis 1 for the last valid interval.
    """
    last = num_data[:, last_col_idx, :]        # (N_SYMS, 5)
    vslow = last[:, VOL_SLOW]
    vfast = last[:, VOL_FAST]
    surge = vfast - vslow
    ltp   = last[:, LTP_F]
    vol   = last[:, VOL]

    order = np.argsort(-surge)
    result = [INDEX_FIELDS]
    for si in order:
        result.append([
            sym_list[si],
            float(vslow[si]),
            float(vfast[si]),
            float(surge[si]),
            float(ltp[si]),
            float(vol[si]),
        ])
    return result


# ── lazy export (for /symbol/<n> route) ───────────────────────────────────────

def numpy_to_cache_rows(header, ts_list, tsf_list, num_matrix):
    """
    Convert a (TOTAL, 5) numpy matrix + timestamp lists to list-of-lists
    in CACHE_FIELDS order: [tsf, ts, vcum, vol, vslow, vfast, ltp].
    Called lazily on the symbol detail route.
    """
    rows = [header]
    # num_matrix columns: [VCUM, LTP, VOL, VSLOW, VFAST]
    for i in range(num_matrix.shape[0]):
        row = [None] * N_CACHE
        row[CH_TSF]   = tsf_list[i]
        row[CH_TS]    = ts_list[i]
        row[CH_VCUM]  = float(num_matrix[i, VOL_CUMUL])
        row[CH_VOL]   = float(num_matrix[i, VOL])
        row[CH_VSLOW] = float(num_matrix[i, VOL_SLOW])
        row[CH_VFAST] = float(num_matrix[i, VOL_FAST])
        row[CH_LTP]   = float(num_matrix[i, LTP_F])
        rows.append(row)
    return rows