"""
data_processor.py
-----------------
Core stateless NSE processing. Calculates ONLY raw Volume and Price.

Changes from original:
  - BUG 4 fixed: symbol discovery now unions symbols across last-file of EVERY day,
    not just the second-to-last day's last file.
  - BUG 5 fixed: fill_gaps_numpy no longer zeroes the carry column at day boundaries;
    intra-day interpolation runs on columns 1+ only, leaving col-0 (carry) intact.
  - PERF 4: excluded symbols from parallel _read_chunk are now logged.
  - Minor: _interp_seg extracted to module level to avoid repeated closure creation.
"""

import csv
import math
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from utils.logging.log_utils import out
from utils.time.time_utils import INDIA_TZ

# 🚀 OPTIMIZATION: Dropped VOL_CUMUL. Reduced memory footprint by 33%.
PRICE = 0
VOL = 1
NFIELDS = 2

SYMB_COL = "symbol"
VALUE_COL = "vol_cum"
LTP_COL = "price"

START_SESSION = "0915"
END_SESSION = "1530"
DATE_PATTERN = r".*(\d{2})(\d{2})(\d{4})/nse_data_(\d{2})(\d{2}).csv"
DT_FRMT = "%d%m%Y%H%M"
DT_STR_FRMT = "%d/%m_%H%M"

new_symb_map = {"LTIM": "LTM"}
_READ_WORKERS = min(16, (os.cpu_count() or 4) + 4)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_col_indices(header: list[str]) -> tuple[int, int, int]:
    """Return (symbol_col, value_col, ltp_col) indices from a CSV header row."""
    return header.index(SYMB_COL), header.index(VALUE_COL), header.index(LTP_COL)


def _interp_seg(seg: np.ndarray) -> None:
    """
    Forward-fill NaNs in `seg` (shape: n_syms x n_cols) via linear interpolation.
    Rows that are entirely NaN are zeroed. Operates in-place.
    """
    if seg.shape[1] == 0:
        return
    idx_f = np.arange(seg.shape[1], dtype=float)
    for si in range(seg.shape[0]):
        row = seg[si]
        finite = np.isfinite(row)
        if not finite.any():
            row[:] = 0.0
        elif not finite.all():
            seg[si] = np.interp(idx_f, idx_f[finite], row[finite])


# ---------------------------------------------------------------------------
# Interval / session utilities
# ---------------------------------------------------------------------------


def calculate_intervals(
    tf: int, start_time_str: str = START_SESSION, end_time_str: str = END_SESSION
) -> int:
    start = datetime.strptime(start_time_str, "%H%M").replace(tzinfo=INDIA_TZ)
    end = datetime.strptime(end_time_str, "%H%M").replace(tzinfo=INDIA_TZ)
    if start >= end:
        return 0
    return math.ceil((end - start).total_seconds() / 60 / tf)


def check_valid_session(curr_time: str) -> bool:
    return (
        0
        < calculate_intervals(tf=1, end_time_str=curr_time)
        <= calculate_intervals(tf=1)
    )


def get_one_day_intervals(tf_str: str) -> tuple[int, int]:
    tf = calculate_intervals(tf=1) if tf_str == "D" else int(tf_str)
    return tf, calculate_intervals(tf=tf)


def get_dt_obj_from_fileindex(
    indx: int, sorted_dates: list[str], tf: int, odi: int
) -> datetime:
    ninterval = (indx - 1) % odi
    dayindx = (indx - 1) // odi
    start_date = datetime.strptime(
        sorted_dates[dayindx] + START_SESSION, "%d%m%Y%H%M"
    ).replace(tzinfo=INDIA_TZ)
    return start_date + timedelta(minutes=(ninterval + 1) * tf)


def get_index_from_dtobj(
    dt_obj: datetime, sorted_dates: list[str], tf: int, odi: int
) -> int:
    nday = sorted_dates.index(dt_obj.strftime("%d%m%Y"))
    ceil_interval = calculate_intervals(end_time_str=dt_obj.strftime("%H%M"), tf=tf)
    return nday * odi + ceil_interval


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------


def discover_files(
    data_dir: str | Path, last_n_days: int | None = None
) -> tuple[list[dict], list[str]]:
    csv_files = [str(p) for p in Path(data_dir).rglob("*.csv")]
    files_with_dates: list[dict] = []
    uniq_dates: set = set()

    for filename in csv_files:
        m = re.match(DATE_PATTERN, filename)
        if not m:
            continue
        date_str = "".join(m.groups())
        if not check_valid_session(date_str[-4:]):
            continue

        file_date = datetime.strptime(date_str, DT_FRMT).replace(tzinfo=INDIA_TZ)
        files_with_dates.append(
            {
                "filename": filename,
                "date_str": file_date.strftime(DT_STR_FRMT),
                "date_obj": file_date,
            }
        )
        uniq_dates.add(file_date.date())

    sorted_files = sorted(files_with_dates, key=lambda x: x["date_obj"])
    sorted_dates_all = [d.strftime("%d%m%Y") for d in sorted(uniq_dates)]

    if last_n_days and last_n_days > 0 and len(sorted_dates_all) > last_n_days:
        use_dates = set(sorted_dates_all[-last_n_days:])
        sorted_files = [
            f for f in sorted_files if f["date_obj"].strftime("%d%m%Y") in use_dates
        ]
        sorted_dates = sorted_dates_all[-last_n_days:]
    else:
        sorted_dates = sorted_dates_all

    return sorted_files, sorted_dates


# ---------------------------------------------------------------------------
# CSV readers
# ---------------------------------------------------------------------------


def _read_chunk(
    chunk_items: list[tuple[int, dict]],
    sym_to_idx: dict[str, int],
    vcum_arr: np.ndarray,
    vltp_arr: np.ndarray,
) -> set[str]:
    excluded: set[str] = set()
    for n_file, finfo in chunk_items:
        try:
            with Path(finfo["filename"]).open(encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                sym_i, val_i, ltp_i = _get_col_indices(next(reader))

                for row in reader:
                    if len(row) <= max(sym_i, val_i, ltp_i):
                        continue
                    sym = new_symb_map.get(row[sym_i].strip('"'), row[sym_i].strip('"'))
                    if (si := sym_to_idx.get(sym)) is None:
                        excluded.add(sym)
                        continue
                    try:
                        vcum_arr[si, n_file] = float(row[val_i])
                        vltp_arr[si, n_file] = float(row[ltp_i].strip('"'))
                    except ValueError:
                        pass
        except (OSError, csv.Error, UnicodeDecodeError, ValueError) as exc:
            out(f"Read error: {exc}")

    return excluded


def read_csv_files_to_arrays(
    sorted_files: list[dict],
    sorted_dates: list[str],
    tf: int,
    odi: int,
    known_symbols: list[str] | None = None,
) -> tuple[list[str], np.ndarray, np.ndarray, int]:
    dates_to_idx = {d: i for i, d in enumerate(sorted_dates)}
    file_col = {
        finfo["filename"]: dates_to_idx[finfo["date_obj"].strftime("%d%m%Y")] * odi
        + calculate_intervals(end_time_str=finfo["date_obj"].strftime("%H%M"), tf=tf)
        for finfo in sorted_files
    }
    total_files = file_col[sorted_files[-1]["filename"]]

    if known_symbols is not None:
        sym_list = list(known_symbols)
    else:
        # BUG 4 FIX: union symbols from the last file of EVERY unique trading day.
        # Previously only used a single file (second-to-last day's last entry),
        # which silently dropped symbols that only appeared on other days.
        day_last: dict[str, dict] = {}
        for f in sorted_files:
            day_key = f["date_obj"].strftime("%d%m%Y")
            day_last[day_key] = f  # last file per day wins

        sym_set: set[str] = set()
        for finfo in day_last.values():
            try:
                with Path(finfo["filename"]).open(encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    sym_i = _get_col_indices(next(reader))[0]
                    for row in reader:
                        if len(row) > sym_i:
                            sym_set.add(
                                new_symb_map.get(
                                    row[sym_i].strip('"'), row[sym_i].strip('"')
                                )
                            )
            except (OSError, csv.Error, UnicodeDecodeError) as exc:
                out(f"Symbol discovery read error ({finfo['filename']}): {exc}")

        sym_list = sorted(sym_set)

    sym_to_idx: dict[str, int] = {s: i for i, s in enumerate(sym_list)}
    vcum_arr = np.full((len(sym_list), total_files + 1), np.nan)
    vltp_arr = np.full((len(sym_list), total_files + 1), np.nan)

    if known_symbols is not None:
        # Sequential read with dynamic symbol expansion for incremental updates
        for finfo in sorted_files:
            n_file = file_col[finfo["filename"]]
            with Path(finfo["filename"]).open(encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                sym_i, val_i, ltp_i = _get_col_indices(next(reader))
                for row in reader:
                    if len(row) <= max(sym_i, val_i, ltp_i):
                        continue
                    sym = new_symb_map.get(row[sym_i].strip('"'), row[sym_i].strip('"'))
                    if (si := sym_to_idx.get(sym)) is None:
                        # New symbol appeared during incremental window
                        # — expand arrays
                        si = len(sym_list)
                        sym_list.append(sym)
                        sym_to_idx[sym] = si
                        new_row = np.full(
                            (1, vcum_arr.shape[1]), np.nan, dtype=vcum_arr.dtype
                        )
                        vcum_arr = np.vstack((vcum_arr, new_row))
                        vltp_arr = np.vstack((vltp_arr, new_row))
                    try:
                        vcum_arr[si, n_file] = float(row[val_i])
                        vltp_arr[si, n_file] = float(row[ltp_i].strip('"'))
                    except ValueError:
                        pass
    else:
        # Parallel read for full initial load (symbol set is pre-determined)
        items = [(file_col[f["filename"]], f) for f in sorted_files]
        chunk_size = max(1, len(items) // _READ_WORKERS)
        chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]
        all_excluded: set[str] = set()
        with ThreadPoolExecutor(max_workers=_READ_WORKERS) as executor:
            futures = [
                executor.submit(_read_chunk, chunk, sym_to_idx, vcum_arr, vltp_arr)
                for chunk in chunks
            ]
            for fut in futures:
                all_excluded |= fut.result()

        # PERF 4: log symbols present in CSVs but absent from sym_to_idx
        if all_excluded:
            sample = sorted(all_excluded)[:10]
            suffix = "..." if len(all_excluded) > 10 else ""
            out(
                f"  ⚠ Full load: skipped {len(all_excluded)} unknown symbols "
                f"(not in any day-end file): {sample}{suffix}"
            )

    return sym_list, vcum_arr, vltp_arr, total_files


# ---------------------------------------------------------------------------
# Gap filling
# ---------------------------------------------------------------------------


def fill_gaps_numpy(
    vcum_1indexed: np.ndarray,
    vltp_1indexed: np.ndarray,
    from_index: int,
    total_files: int,
    odi: int,
) -> None:
    """
    Interpolate missing (NaN) values within each trading day's candle range.

    BUG 5 FIX: The original code temporarily zeroed column-0 of each day-boundary
    segment (the carry-over candle from the prior day) to prevent cross-day bleed
    during interpolation, then restored it. The zero briefly appeared as a data
    point inside _interp_seg, distorting interpolated values for the first few
    candles of the new day.

    Fix: at every day-start boundary, run _interp_seg only on columns 1+ (the
    intra-day range), leaving column-0 (the carry candle) entirely untouched.
    The save/restore sentinel is eliminated.
    """
    if from_index > 1:
        from_index -= 1
    n_cols = total_files - from_index + 1
    if n_cols <= 0:
        return

    vcum_work = vcum_1indexed[:, from_index : total_files + 1]
    vltp_work = vltp_1indexed[:, from_index : total_files + 1]

    # Build day boundary offsets within vcum_work
    day_bounds = [0]
    pos_in_day = (from_index - 1) % odi
    c = odi if pos_in_day == 0 else odi - pos_in_day
    while c < n_cols:
        day_bounds.append(c)
        c += odi

    for i, db in enumerate(day_bounds):
        next_db = day_bounds[i + 1] if i + 1 < len(day_bounds) else n_cols
        start_db = max(db - 1, 0)

        vcum_seg = vcum_work[:, start_db:next_db]
        vltp_seg = vltp_work[:, start_db:next_db]
        if vcum_seg.shape[1] == 0:
            continue

        # is_day_start: this segment starts with a carry column from the prior day
        is_day_start = (from_index + db - 1) % odi == 0 and db > 0

        if is_day_start:
            # BUG 5 FIX: skip col-0 (carry); interpolate only the intra-day slice
            _interp_seg(vcum_seg[:, 1:])
            _interp_seg(vltp_seg[:, 1:])
        else:
            # First segment: seed day-start NaNs to 0 so interp has a left anchor
            if db == 0 and (from_index + db - 1) % odi == 0:
                vcum_seg[:, 0] = np.where(np.isnan(vcum_seg[:, 0]), 0.0, vcum_seg[:, 0])
            _interp_seg(vcum_seg)
            _interp_seg(vltp_seg)

    vcum_1indexed[:, from_index : total_files + 1] = vcum_work
    vltp_1indexed[:, from_index : total_files + 1] = vltp_work


# ---------------------------------------------------------------------------
# Volume delta
# ---------------------------------------------------------------------------


def compute_volume_delta(
    vcum_1indexed: np.ndarray, from_index: int, total_files: int, odi: int
) -> np.ndarray:
    # 🚀 OPTIMIZATION: 100% Vectorized Array Math. Removed the python for loop.
    vol = np.zeros_like(vcum_1indexed)

    diffs = (
        vcum_1indexed[:, from_index : total_files + 1]
        - vcum_1indexed[:, from_index - 1 : total_files]
    )
    vol[:, from_index : total_files + 1] = np.where(diffs > 0, diffs, 0)

    # Restore the first candle of each day — it equals cumulative volume, not delta
    day_starts = np.array(
        [fi for fi in range(from_index, total_files + 1) if (fi - 1) % odi == 0]
    )
    if len(day_starts) > 0:
        vol[:, day_starts] = vcum_1indexed[:, day_starts]

    return vol


# ---------------------------------------------------------------------------
# Timestamp building
# ---------------------------------------------------------------------------


def build_timestamps(
    from_index: int, total_files: int, sorted_dates: list[str], tf: int, odi: int
) -> tuple[list[str], list[str]]:
    is_daily = odi == 1
    ts_list, tsf_list = [], []
    for fi in range(from_index, total_files + 1):
        dt_str = get_dt_obj_from_fileindex(fi, sorted_dates, tf, odi).strftime(
            DT_STR_FRMT
        )
        ts_list.append(dt_str.split("_")[0] if is_daily else dt_str.split("_")[-1])
        tsf_list.append(f"{fi}: {dt_str}")
    return ts_list, tsf_list
