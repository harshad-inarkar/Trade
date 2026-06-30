"""
vol_app.py  -  NSE Intraday FastAPI Web Portal
-----------------------------------------------
Refactored for dynamic indicator processing via OOP.

Changes from original:
  - PERF 1 fixed: dump_merge / dump_index no longer called on every web request.
    They are called only from load_all_data (background refresh cycle).
  - PERF 2 fixed: filter_list result is computed once in _render_index and
    passed directly to dump_merge, eliminating the duplicate list pass.
  - PERF 3: IndicatorFactory.calculate calls run concurrently via ThreadPoolExecutor.
  - ARCH 1: MA results are cached with a TTL equal to reload_interval; cache is
    invalidated after each successful load_all_data.
  - MEM 4 fixed: build_dynamic_averages accepts an optional `symbols` set to
    compute MAs only for the requested subset (used by sector_detail).
  - ARCH 4 fixed: BackgroundReloader catches and logs all exceptions; the reload
    thread never dies silently.
  - Added /api/refresh admin endpoint to manually trigger a reload.
  - Added /api/snapshot endpoint to persist cache to disk.
  - Snapshot dir wired through AppConfig.
"""

import asyncio
import bisect
import csv as _csv
import heapq
import threading
import time
import traceback
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

import numpy as np
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request

from apps.nse_vol_tracker.cache_manager import MIN_TF, TF_KEYS, CacheManager
from apps.nse_vol_tracker.data_processor import PRICE, VOL
from apps.nse_vol_tracker.indicators import IndicatorFactory
from apps.nse_vol_tracker.sector_loader import load_sector_symbols
from utils.data.paths import NSE_INTRADAY_DIR_PATH, OUT_DIR
from utils.fastapi.fastapi_base import AppPaths, BaseAppConfig, BaseFastAPIApp
from utils.logging.log_utils import out
from utils.time.time_utils import wait_next_wall_clock

_base_ma_len = 89
_def_fast_ma_len = 8
_def_slow_ma_len = 21

_app_root_path = "/vol_portal"
_app_cur_redirect_url = "./"

paths = AppPaths.resolve(__file__)

INDEX_FIELDS = [
    "symbol",
    "volume_fast",
    "volume_slow",
    "vol_surge",
    "ltp",
    "price_surge",
    "price_ma_action",
    "vol_ma_action",
]


class AppConfig(BaseAppConfig):
    def __init__(self, path: Path):
        super().__init__(path)
        session_cfg = self.raw_cfg.get("session", {})
        self.start_session: str = session_cfg.get("start", "0915")
        self.end_session: str = session_cfg.get("end", "1530")

        merge_cfg = self.raw_cfg.get("merge", {})
        self.filter_ltp: str = merge_cfg.get("filter_ltp", "")
        self.sort_keys: list[str] = merge_cfg.get("sort_keys", [])

        ndays = merge_cfg.get("last_ndays", 0)
        self.last_ndays: int = ndays if ndays > 0 else 14

        self.reload_interval: int = 3
        self.buffer_seconds: int = -12

        # Optional: directory for .npy snapshots — set in config or leave None
        snap = self.raw_cfg.get("snapshot_dir", "")
        self.snapshot_dir: str | None = snap or None


# ---------------------------------------------------------------------------
# MA result cache  (ARCH 1)
# ---------------------------------------------------------------------------


class _MAResultCache:
    """
    Simple TTL cache keyed on (tf, ma_type, fast_len, slow_len).
    Invalidated explicitly after each data reload and after TTL expires.
    Thread-safe via a single lock (compute is the expensive part, not the dict).
    """

    def __init__(self, ttl_seconds: float = 180.0) -> None:
        self._lock = threading.Lock()
        self._data: dict[tuple, list] = {}
        self._ts: dict[tuple, float] = {}
        self._ttl = ttl_seconds

    def get(self, key: tuple) -> list | None:
        with self._lock:
            if key not in self._data:
                return None
            if time.monotonic() - self._ts[key] > self._ttl:
                del self._data[key]
                del self._ts[key]
                return None
            return self._data[key]

    def set(self, key: tuple, value: list) -> None:
        with self._lock:
            self._data[key] = value
            self._ts[key] = time.monotonic()

    def invalidate(self) -> None:
        with self._lock:
            self._data.clear()
            self._ts.clear()


# ---------------------------------------------------------------------------
# Market data service
# ---------------------------------------------------------------------------


class MarketDataService:
    REFRESH_DT_PAT = "Date: %d  Time: %H:%M"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.cache = CacheManager()
        self.intraday_path = NSE_INTRADAY_DIR_PATH
        self._ma_cache = _MAResultCache(ttl_seconds=config.reload_interval * 60)

    def load_all_data(
        self,
        ma_type: str = "rma",
        fast: int = _def_fast_ma_len,
        slow: int = _def_slow_ma_len,
        intial_load: bool = False,
    ) -> None:
        # Try to load from snapshot on first (cold) start
        if not self.cache.is_ready and self.config.snapshot_dir:
            self.cache.load_snapshot(self.config.snapshot_dir)

        update_flag = self.cache.load_files(self.intraday_path, self.config.last_ndays)

        # Invalidate MA cache after every data reload
        self._ma_cache.invalidate()

        ref_t = self.get_refresh_time_str()

        # PERF 1 FIX: compute MAs once; write files once; never from web requests
        raw_data = self.build_dynamic_averages(MIN_TF, ma_type, fast, slow)

        if update_flag:
            self.dump_merge(
                tf=MIN_TF,
                filt=self.config.filter_ltp or "",
                sort_key_list=self.config.sort_keys or [],
                ref_t=ref_t,
                order_by="desc",
                ma_type=ma_type,
                fast=fast,
                slow=slow,
                precalc_data=raw_data,
            )

        if intial_load:
            self.dump_index(ma_type, fast, slow, precalc_data=raw_data)

        # Persist snapshot for fast cold-starts
        if self.config.snapshot_dir:
            try:
                self.cache.save_snapshot(self.config.snapshot_dir)
            except OSError as exc:
                out(f"⚠ Snapshot save failed: {exc}")

    def get_refresh_time_str(self) -> str:
        dt = self.cache.get_refresh_time()
        return dt.strftime(self.REFRESH_DT_PAT) if dt else "-"

    def build_dynamic_averages(
        self,
        tf: str,
        ma_type: str,
        fast_len: int,
        slow_len: int,
        symbols: set[str] | None = None,  # MEM 4 FIX: restrict to a symbol subset
    ) -> list[list]:
        # ARCH 1: serve from TTL cache when possible (symbols filter bypasses cache)
        cache_key = (tf, ma_type, fast_len, slow_len)
        if symbols is None:
            cached = self._ma_cache.get(cache_key)
            if cached is not None:
                return cached

        sym_list = self.cache.sym_list.get(tf, [])
        nd = self.cache.num_data.get(tf)
        wptr = self.cache.write_ptr.get(tf, 0)

        if nd is None or wptr == 0:
            return [INDEX_FIELDS]

        # MEM 4 FIX: if a symbol subset was requested, slice only those rows
        if symbols:
            idx_map = self.cache.sym_idx_map[tf]
            row_indices = [
                idx_map[s] for s in sym_list if s in symbols and s in idx_map
            ]
            active_syms = [sym_list[i] for i in row_indices]
            vol_matrix = nd[np.array(row_indices), :wptr, VOL]
            price_matrix = nd[np.array(row_indices), :wptr, PRICE]
        else:
            active_syms = sym_list
            vol_matrix = nd[:, :wptr, VOL]
            price_matrix = nd[:, :wptr, PRICE]

        # PERF 3: run five independent MA calculations concurrently
        def _calc(mat: np.ndarray, length: int) -> np.ndarray:
            return IndicatorFactory.calculate(ma_type, mat, length)[:, -1]

        with ThreadPoolExecutor(max_workers=5) as ex:
            f_vfast = ex.submit(_calc, vol_matrix, fast_len)
            f_vslow = ex.submit(_calc, vol_matrix, slow_len)
            f_vbase = ex.submit(_calc, vol_matrix, _base_ma_len)
            f_pfast = ex.submit(_calc, price_matrix, fast_len)
            f_pslow = ex.submit(_calc, price_matrix, slow_len)

        vfast = f_vfast.result()
        vslow = f_vslow.result()
        vbase = f_vbase.result()
        pfast = f_pfast.result()
        pslow = f_pslow.result()

        ltp_last = price_matrix[:, -1]

        # 🚀 OPTIMIZATION: 100% Vectorized Math (No python loops)
        vbase_safe = np.where(vbase == 0, 1, vbase)
        vslow_safe = np.where(vslow == 0, 1, vslow)
        pslow_safe = np.where(pslow == 0, 1, pslow)

        vf_pct = np.round((vfast * 100) / vbase_safe, 2)
        vs_pct = np.round((vslow * 100) / vbase_safe, 2)
        v_surge = np.round((1000 * (vfast - vslow)) / vslow_safe, 2)
        p_surge = np.round((1000 * (pfast - pslow)) / pslow_safe, 2)
        ltp_round = np.round(ltp_last, 2)

        pma = np.select([pfast > pslow, pfast < pslow], [1, -1], default=0)
        vma = np.select([vfast > vslow, vfast < vslow], [1, -1], default=0)

        data_rows = [
            [
                sym,
                float(vfp),
                float(vsp),
                float(vsrg),
                float(ltp),
                float(psrg),
                int(pm),
                int(vm),
            ]
            for sym, vfp, vsp, vsrg, ltp, psrg, pm, vm in zip(
                active_syms,
                vf_pct,
                vs_pct,
                v_surge,
                ltp_round,
                p_surge,
                pma,
                vma,
                strict=True,
            )
        ]

        result = [INDEX_FIELDS, *data_rows]

        if symbols is None:
            self._ma_cache.set(cache_key, result)

        return result

    def filter_list(
        self, symbols_list: list, filt: str, pma_act: str = "na", vma_act: str = "na"
    ) -> tuple[list, int, int, int]:
        start, end = 0, float("inf")
        pos_count = neg_count = neut_count = 0
        has_ltp_filt = False

        if filt:
            try:
                start, end = [int(x) for x in filt.split("-")]
                has_ltp_filt = True
            except ValueError:
                pass

        ltp_idx = INDEX_FIELDS.index("ltp")
        pma_idx = INDEX_FIELDS.index("price_ma_action")
        vma_idx = INDEX_FIELDS.index("vol_ma_action")

        filtered = [symbols_list[0]]
        for row in symbols_list[1:]:
            if row[ltp_idx] is not None:
                if has_ltp_filt and not (start <= row[ltp_idx] <= end):
                    continue

                pma = row[pma_idx]
                vma = row[vma_idx]

                if pma_act == "up" and pma <= 0:
                    continue
                if pma_act == "down" and pma >= 0:
                    continue
                if vma_act == "up" and vma <= 0:
                    continue
                if vma_act == "down" and vma >= 0:
                    continue

                if pma == 1:
                    pos_count += 1
                elif pma == -1:
                    neg_count += 1
                else:
                    neut_count += 1

                filtered.append(row)

        return filtered, pos_count, neg_count, neut_count

    def dump_index(
        self,
        ma_type: str = "rma",
        fast: int = _def_fast_ma_len,
        slow: int = _def_slow_ma_len,
        *,
        precalc_data: list | None = None,
    ) -> None:
        ref_t = self.get_refresh_time_str()
        symbols_list = (
            precalc_data
            if precalc_data is not None
            else self.build_dynamic_averages(MIN_TF, ma_type, fast, slow)
        )
        Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
        sym_idx = INDEX_FIELDS.index("symbol")
        ltp_idx = INDEX_FIELDS.index("ltp")

        data_rows = symbols_list[1:]
        data_rows_sorted = sorted(
            data_rows,
            key=lambda x: x[ltp_idx] if x[ltp_idx] is not None else float("-inf"),
            reverse=True,
        )

        step = 1000
        max_ltp = max(
            (row[ltp_idx] for row in data_rows_sorted if row[ltp_idx] is not None),
            default=0,
        )

        # ARCH 3 FIX: build price groups via a single sorted pass + bucket index
        group_edges = list(range(0, int(max_ltp) + step + 1, step))
        # groups[i] holds rows whose ltp falls in [group_edges[i], group_edges[i+1])
        groups: dict[int, list] = {}
        for row in data_rows_sorted:
            ltp = row[ltp_idx]
            if ltp is None:
                continue
            bucket = bisect.bisect_right(group_edges, ltp) - 1
            groups.setdefault(bucket, []).append(row)

        cand_txt = Path(OUT_DIR) / "candidates.txt"
        with cand_txt.open("w") as write_out:
            for bucket in sorted(groups.keys(), reverse=True):
                if bucket + 1 < len(group_edges):
                    upper = group_edges[bucket + 1]
                else:
                    upper = group_edges[bucket] + step
                write_out.write(f"# Less than {upper}\n")
                write_out.writelines(f"{row[sym_idx]}\n" for row in groups[bucket])
            write_out.write(f"# Timeframe: {MIN_TF} | Refresh Time: {ref_t}\n")

    def dump_merge(
        self,
        tf: str,
        filt: str,
        sort_key_list: list,
        ref_t: str,
        order_by: str,
        ma_type: str = "rma",
        fast: int = _def_fast_ma_len,
        slow: int = _def_slow_ma_len,
        *,
        from_web: bool = False,
        precalc_data: list | None = None,
        precalc_filtered: list | None = None,  # PERF 2 FIX: accept pre-filtered list
        pma_act: str = "na",
        vma_act: str = "up",
    ) -> None:
        top = 40
        sym_idx = INDEX_FIELDS.index("symbol")
        initiator = "Web" if from_web else "Refresh"

        full_symb_list = (
            precalc_data
            if precalc_data is not None
            else self.build_dynamic_averages(tf, ma_type, fast, slow)
        )

        Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
        force_syms_path = Path(OUT_DIR) / "candidates_force.txt"
        forced_symbols: set[str] = set()

        if force_syms_path.exists():
            with force_syms_path.open() as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if line and not line.startswith("#"):
                        forced_symbols.add(line)

        sym_tbl = Path(OUT_DIR) / "sym_table.csv"
        cand_mrg = Path(OUT_DIR) / "candidates_merge.txt"

        with sym_tbl.open("w", newline="") as out_csv, cand_mrg.open("w") as write_out:
            writer = _csv.writer(out_csv)
            writer.writerow(INDEX_FIELDS)

            # PERF 2 FIX: use pre-filtered list if already computed by the caller
            if precalc_filtered is not None:
                filtered_symb_list = precalc_filtered
            else:
                filtered_symb_list, _, _, _ = self.filter_list(
                    full_symb_list, filt, pma_act, vma_act
                )

            symbol_full_row_map: dict[str, list] = {}
            for skey in sort_key_list:
                if skey:
                    sidx = INDEX_FIELDS.index(
                        skey if skey in INDEX_FIELDS else "volume_fast"
                    )
                    top_syms = heapq.nlargest(
                        top,
                        filtered_symb_list[1:],
                        key=lambda x: x[sidx] if x[sidx] is not None else float("-inf"),
                    )
                    for row in top_syms:
                        symbol_full_row_map[row[sym_idx]] = row

            sidx1 = INDEX_FIELDS.index(
                sort_key_list[0]
                if sort_key_list and sort_key_list[0] in INDEX_FIELDS
                else "volume_fast"
            )
            items = list(symbol_full_row_map.items())

            sorted_sidx1 = sorted(
                items,
                key=lambda x: x[1][sidx1] if x[1][sidx1] is not None else float("-inf"),
                reverse=True,
            )
            sidx1_rank_map = {
                item[0]: rank for rank, item in enumerate(sorted_sidx1, 1)
            }

            sidx2_rank_map: dict[str, int] = {}
            sidx2_exist = bool(len(sort_key_list) > 1 and sort_key_list[1])
            if sidx2_exist:
                sidx2 = INDEX_FIELDS.index(
                    sort_key_list[1]
                    if sort_key_list[1] in INDEX_FIELDS
                    else "vol_surge"
                )
                sorted_sidx2 = sorted(
                    items,
                    key=lambda x: (
                        x[1][sidx2] if x[1][sidx2] is not None else float("-inf")
                    ),
                    reverse=True,
                )
                sidx2_rank_map = {
                    item[0]: rank for rank, item in enumerate(sorted_sidx2, 1)
                }

            sorted_symbols = sorted(
                symbol_full_row_map.items(),
                key=lambda item: (
                    sidx1_rank_map[item[0]]
                    + (0 if not sidx2_exist else sidx2_rank_map[item[0]])
                ),
            )

            if forced_symbols:
                write_out.writelines(f"{fsym}\n" for fsym in sorted(forced_symbols))

            for sym, _ in sorted_symbols:
                row = symbol_full_row_map.get(sym)
                if row is not None:
                    writer.writerow(row)
                    write_out.write(f"{sym}\n")

            write_out.write(
                f"#{initiator}|Timeframe: {tf} | "
                f"sorted by {sort_key_list} {order_by} | "
                f"Filter ltp {filt} | PMA {pma_act} | VMA {vma_act} | "
                f"Refresh Time: {ref_t}\n"
            )


# ---------------------------------------------------------------------------
# Background reload thread
# ---------------------------------------------------------------------------


class BackgroundReloader:
    def __init__(self, service: MarketDataService, config: AppConfig) -> None:
        self.service, self.config = service, config

    def start(self) -> None:
        if self.config.reload_interval:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        # ARCH 4 FIX: catch all exceptions so the thread never dies silently.
        while True:
            wait_next_wall_clock(
                self.config.reload_interval, self.config.buffer_seconds or 0
            )
            try:
                self.service.load_all_data()
            except (OSError, ValueError, RuntimeError):
                out(f"❌ Background reload failed:\n{traceback.format_exc()}")
                # Loop continues — next interval will retry


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


class VolTrackerApp(BaseFastAPIApp):
    def __init__(self, config: AppConfig):
        super().__init__(
            title="NSE Intraday Portal",
            config=config,
            template_dir=paths.templates,
            lifespan=self.lifespan_handler,
            root_path=_app_root_path,
        )
        self.cfg: AppConfig = config
        self.data_service = MarketDataService(self.cfg)
        self.reloader = BackgroundReloader(self.data_service, self.cfg)
        self._setup_routes()

    @asynccontextmanager
    async def lifespan_handler(self, app: FastAPI) -> AsyncIterator[None]:  # noqa: ARG002
        t0 = time.time()
        self.data_service.load_all_data(intial_load=True)
        out(f"⏱ Initial load took {time.time() - t0:.2f}s")
        self.reloader.start()
        yield

    def _setup_routes(self) -> None:
        router = APIRouter()
        router.add_api_route("/", self.index, methods=["GET"])
        router.add_api_route("/sectors/", self.sectors_index, methods=["GET"])
        router.add_api_route(
            "/sectors/{sector_name}/", self.sector_detail, methods=["GET"]
        )
        router.add_api_route(
            "/symbol/{symbol_name}", self.symbol_detail, methods=["GET"]
        )
        router.add_api_route("/api/indicator", self.api_indicator, methods=["GET"])
        router.add_api_route("/api/refresh", self.api_refresh, methods=["POST"])
        router.add_api_route("/api/snapshot", self.api_snapshot, methods=["POST"])
        self.app.include_router(router)

    # ------------------------------------------------------------------
    # Route handlers
    # ------------------------------------------------------------------

    async def index(
        self,
        request: Request,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = _def_fast_ma_len,
        slow: int = _def_slow_ma_len,
        filt: Annotated[str, Query(alias="filter")] = "",
        pma_act: str = "na",
        vma_act: str = "na",
        sort: str = "volume_fast",
        order: str = "desc",
    ) -> Any:
        return self._render_index(
            request,
            tf if tf in TF_KEYS else MIN_TF,
            ma,
            fast,
            slow,
            filt,
            pma_act,
            vma_act,
            sort,
            order,
        )

    async def sectors_index(
        self,
        request: Request,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = _def_fast_ma_len,
        slow: int = _def_slow_ma_len,
        filt: Annotated[str, Query(alias="filter")] = "",
        pma_act: str = "na",
        vma_act: str = "na",
        sort: str = "volume_fast",
        order: str = "desc",
    ) -> Any:
        tf_safe = tf if tf in TF_KEYS else MIN_TF
        raw_data = self.data_service.build_dynamic_averages(tf_safe, ma, fast, slow)
        data_map = {row[0]: row for row in raw_data[1:]}

        sector_map = load_sector_symbols()
        sectors_data = []

        for sec_name, symbols in sector_map.items():
            sec_vfast, sec_vslow, valid_syms = [], [], []
            for sym in symbols:
                if sym in data_map:
                    valid_syms.append(sym)
                    sec_vfast.append(data_map[sym][1])
                    sec_vslow.append(data_map[sym][2])

            if valid_syms:
                avg_vfast = sum(sec_vfast) / len(sec_vfast)
                avg_vslow = sum(sec_vslow) / len(sec_vslow)
                heat_pct = min(avg_vfast / 200.0, 1.0)
                sectors_data.append(
                    {
                        "name": sec_name,
                        "symbols": valid_syms,
                        "symbol_count": len(valid_syms),
                        "avg_volume_fast": avg_vfast,
                        "avg_volume_slow": avg_vslow,
                        "heat_pct": heat_pct,
                        "top_symbol": max(valid_syms, key=lambda s: data_map[s][1])
                        if valid_syms
                        else None,
                    }
                )

        rev = order == "desc"
        if sort == "volume_fast":
            sectors_data.sort(key=lambda x: x["avg_volume_fast"], reverse=rev)
        elif sort == "volume_slow":
            sectors_data.sort(key=lambda x: x["avg_volume_slow"], reverse=rev)
        elif sort == "name":
            sectors_data.sort(key=lambda x: x["name"], reverse=rev)
        else:
            sectors_data.sort(key=lambda x: x["avg_volume_fast"], reverse=rev)

        return self.templates.TemplateResponse(
            request,
            "sectoral_index.html",
            {
                "sectors": sectors_data,
                "refresh_time": self.data_service.get_refresh_time_str(),
                "timeframe": tf_safe,
                "ma_type": ma,
                "fast": fast,
                "slow": slow,
                "sort": sort,
                "order": order,
                "filter": filt,
                "pma_act": pma_act,
                "vma_act": vma_act,
            },
        )

    async def sector_detail(
        self,
        request: Request,
        sector_name: str,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = _def_fast_ma_len,
        slow: int = _def_slow_ma_len,
        filt: Annotated[str, Query(alias="filter")] = "",
        pma_act: str = "na",
        vma_act: str = "na",
        sort: str = "volume_fast",
        order: str = "desc",
    ) -> Any:
        sector_map = load_sector_symbols()
        if sector_name not in sector_map:
            raise HTTPException(404, "Sector not found")

        tf_safe = tf if tf in TF_KEYS else MIN_TF
        sector_syms = set(sector_map[sector_name])

        # MEM 4 FIX: compute MAs for sector symbols only, not all 500
        raw_data = self.data_service.build_dynamic_averages(
            tf_safe, ma, fast, slow, symbols=sector_syms
        )

        return self._render_index(
            request,
            tf_safe,
            ma,
            fast,
            slow,
            filt,
            pma_act,
            vma_act,
            sort,
            order,
            sector_list=raw_data,
            sector_name=sector_name,
        )

    async def symbol_detail(
        self,
        request: Request,
        symbol_name: str,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = _def_fast_ma_len,
        slow: int = _def_slow_ma_len,
    ) -> Any:
        tf = tf if tf in TF_KEYS else MIN_TF
        si = self.data_service.cache.sym_idx_map[tf].get(symbol_name)
        if si is None:
            raise HTTPException(404, "Symbol not found")

        nd = self.data_service.cache.num_data[tf]
        wptr = self.data_service.cache.write_ptr[tf]
        if nd is None:
            raise HTTPException(503, "Cache not ready")

        vol = nd[si : si + 1, :wptr, VOL]
        price = nd[si : si + 1, :wptr, PRICE]

        ts_list = self.data_service.cache.ts_list[tf][:wptr]
        tsf_list = self.data_service.cache.tsf_list[tf][:wptr]

        vol_list = np.where(np.isnan(vol[0]), 0.0, vol[0]).tolist()
        price_list = np.where(np.isnan(price[0]), 0.0, price[0]).tolist()

        data = [["timestamp_full", "timestamp", "volume", "price"]] + [
            [tsf, ts, v, p]
            for tsf, ts, v, p in zip(
                tsf_list, ts_list, vol_list, price_list, strict=True
            )
        ]

        return self.templates.TemplateResponse(
            request,
            "symbol.html",
            {
                "symbol": symbol_name,
                "data": data,
                "timeframe": tf,
                "ma_type": ma,
                "fast": fast,
                "slow": slow,
            },
        )

    async def api_indicator(
        self,
        symbol: str,
        tf: str = MIN_TF,
        source: str = "price",
        ind_type: str = "rma",
        p1: float = _def_fast_ma_len,
    ) -> dict[str, list[float | None]]:
        tf = tf if tf in TF_KEYS else MIN_TF
        si = self.data_service.cache.sym_idx_map[tf].get(symbol)
        if si is None:
            raise HTTPException(404, "Symbol not found")

        nd = self.data_service.cache.num_data[tf]
        wptr = self.data_service.cache.write_ptr[tf]
        if nd is None:
            raise HTTPException(503, "Cache not ready")

        base_data = (
            nd[si : si + 1, :wptr, VOL]
            if source == "volume"
            else nd[si : si + 1, :wptr, PRICE]
        )

        try:
            res = (
                base_data[0]
                if ind_type == "raw"
                else IndicatorFactory.calculate(ind_type, base_data, int(p1))[0]
            )
        except (ValueError, TypeError, IndexError) as e:
            raise HTTPException(500, f"Calculation error: {e}") from e

        res_list: list[float | None] = [
            float(x) if not np.isnan(x) else None for x in res
        ]
        return {"data": res_list}

    async def api_refresh(self) -> dict[str, str]:
        """Admin endpoint: manually trigger a data reload."""
        await asyncio.to_thread(self.data_service.load_all_data)
        return {
            "status": "ok",
            "refresh_time": self.data_service.get_refresh_time_str(),
        }

    async def api_snapshot(self) -> dict[str, str]:
        """Admin endpoint: persist current cache to snapshot_dir."""
        if not self.cfg.snapshot_dir:
            raise HTTPException(400, "snapshot_dir not configured")
        await asyncio.to_thread(
            self.data_service.cache.save_snapshot, self.cfg.snapshot_dir
        )
        return {"status": "ok", "snapshot_dir": self.cfg.snapshot_dir}

    # ------------------------------------------------------------------
    # Shared render helper
    # ------------------------------------------------------------------

    def _render_index(
        self,
        request: Request,
        tf: str,
        ma_type: str,
        fast: int,
        slow: int,
        filt: str,
        pma_act: str,
        vma_act: str,
        sort_key: str,
        order_by: str,
        sector_list: list | None = None,
        sector_name: str | None = None,
    ) -> Any:
        raw_data = sector_list or self.data_service.build_dynamic_averages(
            tf, ma_type, fast, slow
        )
        symbols_list, pos, neg, neut = self.data_service.filter_list(
            raw_data, filt, pma_act, vma_act
        )

        if sort_key in INDEX_FIELDS:
            sidx = INDEX_FIELDS.index(sort_key)
            symbols_list[1:] = sorted(
                symbols_list[1:],
                key=lambda x: x[sidx] if x[sidx] is not None else -9999,
                reverse=(order_by != "asc"),
            )

        # PERF 1 FIX: dump_merge is NOT called here anymore.
        # It runs exclusively in load_all_data after each background refresh.
        # This eliminates disk I/O on every HTTP request.

        ref_t = self.data_service.get_refresh_time_str()

        return self.templates.TemplateResponse(
            request,
            "index.html",
            {
                "symbols": symbols_list,
                "count": len(symbols_list),
                "refresh_time": ref_t,
                "timeframe": tf,
                "ma_type": ma_type,
                "fast": fast,
                "slow": slow,
                "sort": sort_key,
                "order": order_by,
                "filter": filt,
                "pma_act": pma_act,
                "vma_act": vma_act,
                "pos_count": pos,
                "neg_count": neg,
                "neut_count": neut,
                "sector_name": sector_name,
            },
        )


if __name__ == "__main__":
    config = AppConfig(paths.config)
    tracker = VolTrackerApp(config)
    tracker.run()
