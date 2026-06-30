"""
cache_manager.py
----------------
Thread-safe multi-timeframe cache holding RAW series data only.
"""

import math
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np

from apps.nse_vol_tracker.data_processor import (
    NFIELDS,
    PRICE,
    VOL,
    build_timestamps,
    compute_volume_delta,
    discover_files,
    fill_gaps_numpy,
    get_index_from_dtobj,
    get_one_day_intervals,
    read_csv_files_to_arrays,
)
from utils.logging.log_utils import out
from utils.time.time_utils import INDIA_TZ

MIN_TF = "3"
TF_KEYS = (MIN_TF, "15", "D")
_BUFFER_DAYS = 1


class CacheManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready = False

        self.sym_list: dict[str, list[str]] = {tf: [] for tf in TF_KEYS}
        # O(1) symbol -> array-row index lookup, kept in sync with sym_list
        self.sym_idx_map: dict[str, dict[str, int]] = {tf: {} for tf in TF_KEYS}
        self.num_data: dict[str, np.ndarray | None] = dict.fromkeys(TF_KEYS)
        self.write_ptr: dict[str, int] = dict.fromkeys(TF_KEYS, 0)
        self.ts_list: dict[str, list[str]] = {tf: [] for tf in TF_KEYS}
        self.tsf_list: dict[str, list[str]] = {tf: [] for tf in TF_KEYS}

        self._refresh_time: datetime | None = None
        self._sorted_dates: list[str] | None = None
        self._committed_total: dict[str, int] = dict.fromkeys(TF_KEYS, 0)

        # Retained strictly for boundaries between incremental updates
        self._seed_vcum: dict[str, np.ndarray | None] = dict.fromkeys(TF_KEYS)
        self._seed_vltp: dict[str, np.ndarray | None] = dict.fromkeys(TF_KEYS)

    @property
    def is_ready(self) -> bool:
        return self._ready

    def get_refresh_time(self) -> datetime | None:
        return self._refresh_time

    def load_files(self, data_dir: str | Path, last_n_days: int | None = None) -> None:
        incremental = self._ready and self._refresh_time is not None
        label = "Reloading" if incremental else "Loading data"
        out(f"🔄 {datetime.now(INDIA_TZ).strftime('%H:%M:%S')} : {label}...")

        result = self._load_csv_files(data_dir, last_n_days, incremental=incremental)

        if result is None:
            out(f"✅ {datetime.now(INDIA_TZ).strftime('%H:%M:%S')} : No updates.")
            return

        with self._lock:
            self._apply_result(result, incremental=incremental)

        ref = self._refresh_time
        out(
            f"✅ {datetime.now(INDIA_TZ).strftime('%H:%M:%S')} : Done. "
            f"Last: {ref.strftime('%d%m%Y-%H%M') if ref else '-'}"
        )

    def _load_csv_files(
        self, data_dir: str | Path, last_n_days: int | None, *, incremental: bool
    ) -> dict | None:
        tf_min, odi_min = get_one_day_intervals(MIN_TF)
        sorted_files, sorted_dates = discover_files(data_dir, last_n_days)
        if not sorted_files:
            return None

        last_file_dt = sorted_files[-1]["date_obj"]
        cache_refresh = self._refresh_time

        if incremental:
            if cache_refresh and last_file_dt <= cache_refresh:
                return None
            new_files = (
                [f for f in sorted_files if f["date_obj"] > cache_refresh]
                if cache_refresh
                else sorted_files
            )
            if not new_files:
                return None

            from_index = get_index_from_dtobj(
                new_files[0]["date_obj"], sorted_dates, tf_min, odi_min
            )
            last_committed = (
                get_index_from_dtobj(cache_refresh, sorted_dates, tf_min, odi_min)
                if cache_refresh
                else 0
            )

            # Determine which seed column to use for boundary bridging.
            # Passed explicitly to derived-tf workers to avoid shared mutable state.
            seed_slot = 0 if from_index == last_committed else 1
            from_index = (
                last_committed + 1 if from_index != last_committed else from_index
            )

            files_to_read = new_files
            known_symbols: list[str] | None = self.sym_list[MIN_TF]
        else:
            from_index, files_to_read, known_symbols = 1, sorted_files, None
            seed_slot = 1  # unused for full load; defined for consistent signature

        sym_list, vcum, vltp, total = read_csv_files_to_arrays(
            files_to_read, sorted_dates, tf_min, odi_min, known_symbols
        )
        n_syms = len(sym_list)

        if incremental and from_index > 1:
            if (sv_vcum := self._seed_vcum[MIN_TF]) is not None:
                vcum[: min(sv_vcum.shape[0], n_syms), from_index - 1] = sv_vcum[
                    : min(sv_vcum.shape[0], n_syms), seed_slot
                ]
            if (sv_vltp := self._seed_vltp[MIN_TF]) is not None:
                vltp[: min(sv_vltp.shape[0], n_syms), from_index - 1] = sv_vltp[
                    : min(sv_vltp.shape[0], n_syms), seed_slot
                ]

        fill_gaps_numpy(vcum, vltp, from_index, total, odi_min)
        vol = compute_volume_delta(vcum, from_index, total, odi_min)
        ts_list, tsf_list = build_timestamps(
            from_index, total, sorted_dates, tf_min, odi_min
        )

        result: dict = {
            MIN_TF: {
                "sym_list": sym_list,
                "vcum": vcum,
                "vltp": vltp,
                "vol": vol,
                "from_index": from_index,
                "total": total,
                "ts_list": ts_list,
                "tsf_list": tsf_list,
                "sorted_dates": sorted_dates,
                "refresh_time": last_file_dt,
                "odi": odi_min,
            }
        }

        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {
                tf_str: ex.submit(
                    self._load_derived_tf,
                    tf_str,
                    sym_list,
                    vcum,
                    vltp,
                    total,
                    sorted_dates,
                    from_index,
                    seed_slot,
                    incremental=incremental,
                )
                for tf_str in ("15", "D")
            }
            for tf_str, fut in futures.items():
                result[tf_str] = fut.result()

        return result

    def _load_derived_tf(
        self,
        tf_str: str,
        sym_list: list[str],
        min_vcum: np.ndarray,
        min_vltp: np.ndarray,
        min_total: int,
        sorted_dates: list[str],
        min_from_index: int,
        seed_slot: int,
        *,
        incremental: bool,
    ) -> dict:
        tf, odi = get_one_day_intervals(tf_str)
        tfratio = tf // int(MIN_TF)
        n_syms = len(sym_list)

        if incremental:
            old_total = self._committed_total.get(tf_str, 0)
            total = math.ceil(min_total / tfratio)
            from_idx = math.ceil(min_from_index / tfratio)

            if from_idx == old_total:
                seed_slot = 0
                from_idx = old_total
            else:
                seed_slot = 1
                from_idx = old_total + 1

            if total < from_idx:
                return {
                    "sym_list": sym_list,
                    "vcum": None,
                    "from_index": from_idx,
                    "total": total,
                    "ts_list": [],
                    "tsf_list": [],
                    "odi": odi,
                }
        else:
            total, from_idx = math.ceil(min_total / tfratio), 1

        vcum_d = np.full((n_syms, total + 1), np.nan)
        vltp_d = np.full((n_syms, total + 1), np.nan)

        for nfi in range(from_idx, total + 1):
            if (mi := min(nfi * tfratio, min_total)) >= min_from_index:
                vcum_d[:, nfi] = min_vcum[:n_syms, mi]
                vltp_d[:, nfi] = min_vltp[:n_syms, mi]

        if incremental and from_idx > 1:
            if (sv_vcum := self._seed_vcum.get(tf_str)) is not None:
                vcum_d[: min(sv_vcum.shape[0], n_syms), from_idx - 1] = sv_vcum[
                    : min(sv_vcum.shape[0], n_syms), seed_slot
                ]
            if (sv_vltp := self._seed_vltp.get(tf_str)) is not None:
                vltp_d[: min(sv_vltp.shape[0], n_syms), from_idx - 1] = sv_vltp[
                    : min(sv_vltp.shape[0], n_syms), seed_slot
                ]

        fill_gaps_numpy(vcum_d, vltp_d, from_idx, total, odi)
        vol_d = compute_volume_delta(vcum_d, from_idx, total, odi)
        ts_list, tsf_list = build_timestamps(from_idx, total, sorted_dates, tf, odi)

        return {
            "sym_list": sym_list,
            "vcum": vcum_d,
            "vltp": vltp_d,
            "vol": vol_d,
            "from_index": from_idx,
            "total": total,
            "ts_list": ts_list,
            "tsf_list": tsf_list,
            "odi": odi,
        }

    def _apply_result(self, result: dict, *, incremental: bool) -> None:
        self._sorted_dates = result[MIN_TF]["sorted_dates"]
        self._refresh_time = result[MIN_TF]["refresh_time"]

        for tf_str in TF_KEYS:
            res = result.get(tf_str)
            if res is None or res["vcum"] is None:
                continue

            sym_list: list[str] = res["sym_list"]
            n_syms = len(sym_list)
            vcum_1idx: np.ndarray = res["vcum"]
            vltp_1idx: np.ndarray = res["vltp"]
            vol_1idx: np.ndarray = res["vol"]
            from_index: int = res["from_index"]
            total: int = res["total"]

            if (n_new := total - from_index + 1) <= 0:
                continue
            odi_tf = get_one_day_intervals(tf_str)[1]

            # overwrite is only meaningful in the incremental branch;
            # initialise to False so mypy sees it as always bound.
            overwrite = False

            if not incremental:
                nd: np.ndarray = np.empty(
                    (n_syms, total + _BUFFER_DAYS * odi_tf, NFIELDS), dtype=np.float64
                )
                wptr_start = 0
            else:
                existing_nd = self.num_data[tf_str]
                wptr = self.write_ptr[tf_str]
                overwrite = from_index == self._committed_total[tf_str]
                if overwrite:
                    wptr = max(wptr - 1, 0)
                    n_new = total - from_index + 1

                if existing_nd is None or wptr + n_new > existing_nd.shape[1]:
                    extra = max(n_new, _BUFFER_DAYS * odi_tf)
                    nd = (
                        np.empty((n_syms, n_new + extra, NFIELDS), dtype=np.float64)
                        if existing_nd is None
                        else np.concatenate(
                            [
                                existing_nd[:, :wptr, :],
                                np.empty(
                                    (existing_nd.shape[0], extra, NFIELDS),
                                    dtype=np.float64,
                                ),
                            ],
                            axis=1,
                        )
                    )
                else:
                    nd = existing_nd

                if n_syms > nd.shape[0]:
                    nd = np.concatenate(
                        [nd, np.zeros((n_syms - nd.shape[0], nd.shape[1], NFIELDS))],
                        axis=0,
                    )
                wptr_start = wptr

            # 🚀 OPTIMIZATION: Stopped injecting vcum_1idx into num_data array
            for c, fi in enumerate(range(from_index, total + 1)):
                nd[:n_syms, wptr_start + c, PRICE] = vltp_1idx[:n_syms, fi]
                nd[:n_syms, wptr_start + c, VOL] = vol_1idx[:n_syms, fi]

            new_wptr = wptr_start + n_new
            self.num_data[tf_str] = nd
            self.sym_list[tf_str] = sym_list
            self.sym_idx_map[tf_str] = {s: i for i, s in enumerate(sym_list)}
            self.write_ptr[tf_str] = new_wptr
            self._committed_total[tf_str] = total

            if not incremental:
                self.ts_list[tf_str] = list(res["ts_list"])
                self.tsf_list[tf_str] = list(res["tsf_list"])
            else:
                if overwrite:
                    self.ts_list[tf_str] = self.ts_list[tf_str][:-1]
                    self.tsf_list[tf_str] = self.tsf_list[tf_str][:-1]
                self.ts_list[tf_str] += list(res["ts_list"])
                self.tsf_list[tf_str] += list(res["tsf_list"])

            # Save cumulative values strictly for bridging boundaries
            idx1 = max(total - 1, 0)
            idx2 = total
            self._seed_vcum[tf_str] = vcum_1idx[:n_syms, [idx1, idx2]].copy()
            self._seed_vltp[tf_str] = vltp_1idx[:n_syms, [idx1, idx2]].copy()

        self._ready = True
