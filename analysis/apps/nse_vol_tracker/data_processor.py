"""
data_processor.py
-----------------
Core stateless NSE processing. Calculates ONLY raw Volume, Price, and Cumul.
MA logic deferred strictly to presentation layer.
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

# Reduced to ONLY raw data points. No MA storage.
VOL_CUMUL = 0
PRICE = 1
VOL = 2
NFIELDS = 3

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


def discover_files(
    data_dir: str | Path, last_n_days: int | None = None
) -> tuple[list[dict], list[str]]:
    csv_files = [str(p) for p in Path(data_dir).rglob("*.csv")]
    files_with_dates, uniq_dates = [], set()

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

    sorted_files = sorted(
        files_with_dates,
        key=lambda x: (
            x["date_obj"] if hasattr(x["date_obj"], "__lt__") else str(x["date_obj"])
        ),
    )
    sorted_dates_all = [d.strftime("%d%m%Y") for d in sorted(uniq_dates)]

    if last_n_days and last_n_days > 0 and len(sorted_dates_all) > last_n_days:
        use_dates = set(sorted_dates_all[-last_n_days:])
        sorted_files = [
            f
            for f in sorted_files
            if hasattr(f["date_obj"], "strftime")
            and f["date_obj"].strftime("%d%m%Y") in use_dates
        ]
        sorted_dates = sorted_dates_all[-last_n_days:]
    else:
        sorted_dates = sorted_dates_all

    return sorted_files, sorted_dates


def _read_chunk(
    chunk_items: list[tuple[int, dict]],
    sym_to_idx: dict,
    vcum_arr: np.ndarray,
    vltp_arr: np.ndarray,
) -> set[str]:
    excluded = set()
    for n_file, finfo in chunk_items:
        try:
            with Path(finfo["filename"]).open(encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header = next(reader)
                sym_i, val_i, ltp_i = (
                    header.index(SYMB_COL),
                    header.index(VALUE_COL),
                    header.index(LTP_COL),
                )

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
        sym_set = set()
        prev_f = next(
            (
                sorted_files[i - 1]
                for i in range(len(sorted_files) - 1, 0, -1)
                if sorted_files[i]["date_obj"].strftime("%d%m%Y")
                != sorted_files[i - 1]["date_obj"].strftime("%d%m%Y")
            ),
            sorted_files[-1],
        )
        with Path(prev_f["filename"]).open(encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            sym_i = next(reader).index(SYMB_COL)
            for row in reader:
                if len(row) > sym_i:
                    sym_set.add(
                        new_symb_map.get(row[sym_i].strip('"'), row[sym_i].strip('"'))
                    )
        sym_list = sorted(sym_set)

    sym_to_idx = {s: i for i, s in enumerate(sym_list)}
    vcum_arr = np.full((len(sym_list), total_files + 1), np.nan)
    vltp_arr = np.full((len(sym_list), total_files + 1), np.nan)
    excluded = set()

    if known_symbols is not None:
        for finfo in sorted_files:
            n_file = file_col[finfo["filename"]]
            with Path(finfo["filename"]).open(encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header = next(reader)
                sym_i, val_i, ltp_i = (
                    header.index(SYMB_COL),
                    header.index(VALUE_COL),
                    header.index(LTP_COL),
                )
                for row in reader:
                    if len(row) <= max(sym_i, val_i, ltp_i):
                        continue
                    sym = new_symb_map.get(row[sym_i].strip('"'), row[sym_i].strip('"'))
                    if (si := sym_to_idx.get(sym)) is None:
                        si = len(sym_list)
                        sym_list.append(sym)
                        sym_to_idx[sym] = si
                        vcum_arr = np.vstack(
                            (
                                vcum_arr,
                                np.full(
                                    (1, vcum_arr.shape[1]), np.nan, dtype=vcum_arr.dtype
                                ),
                            )
                        )

                        vltp_arr = np.vstack(
                            (
                                vltp_arr,
                                np.full(
                                    (1, vltp_arr.shape[1]), np.nan, dtype=vltp_arr.dtype
                                ),
                            )
                        )

                    try:
                        vcum_arr[si, n_file] = float(row[val_i])
                        vltp_arr[si, n_file] = float(row[ltp_i].strip('"'))
                    except ValueError:
                        pass
    else:
        items = [(file_col[f["filename"]], f) for f in sorted_files]
        chunks = [
            items[i : i + max(1, len(items) // _READ_WORKERS)]
            for i in range(0, len(items), max(1, len(items) // _READ_WORKERS))
        ]
        with ThreadPoolExecutor() as executor:
            for fut in [
                executor.submit(_read_chunk, chunk, sym_to_idx, vcum_arr, vltp_arr)
                for chunk in chunks
            ]:
                excluded.update(fut.result())

    return sym_list, vcum_arr, vltp_arr, total_files


def fill_gaps_numpy(
    vcum_1indexed: np.ndarray,
    vltp_1indexed: np.ndarray,
    from_index: int,
    total_files: int,
    odi: int,
) -> None:
    if from_index > 1:
        from_index -= 1
    n_cols = total_files - from_index + 1
    if n_cols <= 0:
        return

    vcum_work = vcum_1indexed[:, from_index : total_files + 1]
    vltp_work = vltp_1indexed[:, from_index : total_files + 1]

    def _interp_seg(seg: np.ndarray) -> None:
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

    day_bounds = [0]
    pos_in_day = (from_index - 1) % odi
    c = odi if pos_in_day == 0 else odi - pos_in_day
    while c < n_cols:
        day_bounds.append(c)
        c += odi

    for i, db in enumerate(day_bounds):
        next_db = day_bounds[i + 1] if i + 1 < len(day_bounds) else n_cols
        start_db = max(db - 1, 0)

        vcum_seg, vltp_seg = (
            vcum_work[:, start_db:next_db],
            vltp_work[:, start_db:next_db],
        )
        if vcum_seg.shape[1] == 0:
            continue

        if (from_index + db - 1) % odi == 0 and db > 0:
            save_vcum = vcum_seg[:, 0].copy()
            vcum_seg[:, 0] = 0.0

        if (from_index + db - 1) % odi == 0 and db == 0:
            vcum_seg[:, 0] = np.where(np.isnan(vcum_seg[:, 0]), 0.0, vcum_seg[:, 0])

        _interp_seg(vcum_seg)
        _interp_seg(vltp_seg)

        if (from_index + db - 1) % odi == 0 and db > 0:
            vcum_seg[:, 0] = save_vcum

    vcum_1indexed[:, from_index : total_files + 1] = vcum_work
    vltp_1indexed[:, from_index : total_files + 1] = vltp_work


def compute_volume_delta(
    vcum_1indexed: np.ndarray, from_index: int, total_files: int, odi: int
) -> np.ndarray:
    vol = vcum_1indexed.copy()
    for fi in range(from_index, total_files + 1):
        if (fi - 1) % odi != 0:
            delta = vcum_1indexed[:, fi] - vcum_1indexed[:, fi - 1]
            vol[:, fi] = np.where(delta > 0, delta, 0)
    return vol


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
