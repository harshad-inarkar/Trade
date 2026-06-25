"""
app.py  -  NSE Intraday FastAPI Web Portal
------------------------------------------
Refactored for dynamic indicator processing via OOP.
"""

import csv as _csv
import heapq
import math
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request

from apps.nse_vol_tracker.cache_manager import MIN_TF, TF_KEYS, CacheManager
from apps.nse_vol_tracker.data_processor import PRICE, VOL
from apps.nse_vol_tracker.indicators import IndicatorFactory
from apps.nse_vol_tracker.sector_loader import load_sector_symbols
from utils.data.paths import NSE_INTRADAY_DIR_PATH, OUT_DIR
from utils.fastapi.fastapi_base import AppPaths, BaseAppConfig, BaseFastAPIApp
from utils.logging.log_utils import out
from utils.time.time_utils import wait_next_wall_clock

paths = AppPaths.resolve(__file__)

INDEX_FIELDS = [
    "symbol",
    "volume_fast",
    "volume_slow",
    "vol_surge",
    "ltp",
    "price_surge",
    "price_ma_action",
]
CACHE_FIELDS = [
    "timestamp_full",
    "timestamp",
    "volume_slow",
    "volume_fast",
    "ltp",
    "ltp_rma_fast",
    "ltp_rma_slow",
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


class MarketDataService:
    REFRESH_DT_PAT = "Date: %d  Time: %H:%M"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.cache = CacheManager()
        self.intraday_path = NSE_INTRADAY_DIR_PATH

    def load_all_data(
        self, ma_type: str = "rma", fast: int = 8, slow: int = 21
    ) -> None:
        self.cache.load_files(self.intraday_path, self.config.last_ndays)
        ref_t = self.get_refresh_time_str()
        self.dump_merge(
            MIN_TF,
            self.config.filter_ltp or "",
            "na",  # Default MA action
            self.config.sort_keys or [],
            ref_t,
            "desc",
            ma_type,
            fast,
            slow,
        )

    def get_refresh_time_str(self) -> str:
        dt = self.cache.get_refresh_time()
        return dt.strftime(self.REFRESH_DT_PAT) if dt else "-"

    def build_dynamic_averages(
        self, tf: str, ma_type: str, fast_len: int, slow_len: int
    ) -> list[list]:
        sym_list = self.cache.sym_list.get(tf, [])
        nd = self.cache.num_data.get(tf)
        wptr = self.cache.write_ptr.get(tf, 0)

        if nd is None or wptr == 0:
            return [INDEX_FIELDS]

        vol_matrix = nd[:, :wptr, VOL]
        price_matrix = nd[:, :wptr, PRICE]

        vfast = IndicatorFactory.calculate(ma_type, vol_matrix, fast_len)[:, -1]
        vslow = IndicatorFactory.calculate(ma_type, vol_matrix, slow_len)[:, -1]
        vbase = IndicatorFactory.calculate(ma_type, vol_matrix, 89)[:, -1]

        pfast = IndicatorFactory.calculate(ma_type, price_matrix, fast_len)[:, -1]
        pslow = IndicatorFactory.calculate(ma_type, price_matrix, slow_len)[:, -1]
        ltp_last = price_matrix[:, -1]

        result: list[list[Any]] = [INDEX_FIELDS]

        for si, sym in enumerate(sym_list):
            vf_pct = (vfast[si] * 100) / vbase[si] if vbase[si] != 0 else 0
            vs_pct = (vslow[si] * 100) / vbase[si] if vbase[si] != 0 else 0
            v_surge = (
                1000 * (vfast[si] - vslow[si]) / vslow[si] if vslow[si] != 0 else 0
            )
            p_surge = (
                1000 * (pfast[si] - pslow[si]) / pslow[si] if pslow[si] != 0 else 0
            )

            pma = (
                1
                if p_surge > 0 and pfast[si] > pslow[si]
                else (-1 if p_surge < 0 and pfast[si] < pslow[si] else 0)
            )

            result.append(
                [
                    sym,
                    round(float(vf_pct), 2),
                    round(float(vs_pct), 2),
                    round(float(v_surge), 2),
                    round(float(ltp_last[si]), 2),
                    round(float(p_surge), 2),
                    pma,
                ]
            )
        return result

    def filter_list(
        self, symbols_list: list, filt: str, ma_act: str = "na"
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

        filtered = [symbols_list[0]]
        for row in symbols_list[1:]:
            if row[ltp_idx] is not None:
                # LTP Filter
                if has_ltp_filt and not (start <= row[ltp_idx] <= end):
                    continue

                pma = row[pma_idx]

                # MA Action Filter
                if ma_act == "up" and pma <= 0:
                    continue
                if ma_act == "down" and pma >= 0:
                    continue

                if pma == 1:
                    pos_count += 1
                elif pma == -1:
                    neg_count += 1
                else:
                    neut_count += 1

                filtered.append(row)

        return filtered, pos_count, neg_count, neut_count

    def dump_index(self, ma_type: str = "rma", fast: int = 8, slow: int = 21) -> None:
        ref_t = self.get_refresh_time_str()
        symbols_list = self.build_dynamic_averages(MIN_TF, ma_type, fast, slow)
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

        group_ranges = []
        curr = 0
        while curr <= max_ltp:
            group_ranges.append((curr, curr + step))
            curr += step

        cand_txt = Path(OUT_DIR) / "candidates.txt"
        with cand_txt.open("w") as write_out:
            for start, end in reversed(group_ranges):
                group_symbols = [
                    row
                    for row in data_rows_sorted
                    if row[ltp_idx] is not None and start <= row[ltp_idx] < end
                ]
                if not group_symbols:
                    continue
                write_out.write(f"# Less than {end}\n")
                write_out.writelines(f"{row[sym_idx]}\n" for row in group_symbols)
            write_out.write(f"# Timeframe: {MIN_TF} | Refresh Time: {ref_t}\n")

    def dump_merge(
        self,
        tf: str,
        filt: str,
        ma_act: str,
        sort_key_list: list,
        ref_t: str,
        order_by: str,
        ma_type: str = "rma",
        fast: int = 8,
        slow: int = 21,
        *,
        from_web: bool = False,
    ) -> None:
        top = 40
        sym_idx = INDEX_FIELDS.index("symbol")
        initiator = "Web" if from_web else "Refresh"

        full_symb_list = self.build_dynamic_averages(tf, ma_type, fast, slow)
        Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

        force_syms_path = Path(OUT_DIR) / "candidates_force.txt"
        forced_symbols = set()
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

            symbol_full_row_map = {}
            for skey in sort_key_list:
                if skey:
                    sidx = INDEX_FIELDS.index(
                        skey if skey in INDEX_FIELDS else "volume_fast"
                    )
                    symbols_list, _, _, _ = self.filter_list(
                        full_symb_list, filt, ma_act
                    )

                    top_syms = heapq.nlargest(
                        top,
                        symbols_list[1:],
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

            sidx2_rank_map = {}
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
                reverse=False,
            )

            if forced_symbols:
                write_out.writelines(f"{fsym}\n" for fsym in sorted(forced_symbols))

            for sym, _ in sorted_symbols:
                row = symbol_full_row_map.get(sym)
                if row is not None:
                    writer.writerow(row)
                    write_out.write(f"{sym}\n")

            write_out.write(
                f"#{initiator}|Timeframe: {tf} | sorted by {sort_key_list} {order_by} "
                f"| Filter ltp {filt} | MA Act {ma_act} | Refresh Time: {ref_t}\n"
            )


class BackgroundReloader:
    def __init__(self, service: MarketDataService, config: AppConfig) -> None:
        self.service, self.config = service, config

    def start(self) -> None:
        if self.config.reload_interval:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        while True:
            wait_next_wall_clock(
                self.config.reload_interval, self.config.buffer_seconds or 0
            )
            self.service.load_all_data()


class VolTrackerApp(BaseFastAPIApp):
    def __init__(self, config: AppConfig):
        super().__init__(
            title="NSE Intraday Portal",
            config=config,
            template_dir=paths.templates,
            lifespan=self.lifespan_handler,
        )

        self.cfg: AppConfig = config
        self.data_service = MarketDataService(self.cfg)
        self.reloader = BackgroundReloader(self.data_service, self.cfg)
        self._setup_routes()

    @asynccontextmanager
    async def lifespan_handler(self, app: FastAPI) -> AsyncIterator[None]:  # noqa: ARG002
        t0 = time.time()
        self.data_service.load_all_data()
        self.data_service.dump_index()
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
        self.app.include_router(router)

    async def index(
        self,
        request: Request,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = 8,
        slow: int = 21,
        filt: Annotated[str, Query(alias="filter")] = "",
        ma_act: str = "na",
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
            ma_act,
            sort,
            order,
        )

    async def sectors_index(
        self,
        request: Request,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = 8,
        slow: int = 21,
        filt: Annotated[str, Query(alias="filter")] = "",
        ma_act: str = "na",
        sort: str = "volume_fast",
        order: str = "desc",
    ) -> Any:
        tf_safe = tf if tf in TF_KEYS else MIN_TF
        raw_data = self.data_service.build_dynamic_averages(tf_safe, ma, fast, slow)
        data_map = {row[0]: row for row in raw_data[1:]}

        sector_map = load_sector_symbols()
        sectors_data = []

        for sec_name, symbols in sector_map.items():
            sec_vfast = []
            sec_vslow = []
            valid_syms = []
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
                "ma_act": ma_act,
            },
        )

    def sector_detail(
        self,
        request: Request,
        sector_name: str,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = 8,
        slow: int = 21,
        filt: Annotated[str, Query(alias="filter")] = "",
        ma_act: str = "na",
        sort: str = "volume_fast",
        order: str = "desc",
    ) -> Any:

        sector_map = load_sector_symbols()
        if sector_name not in sector_map:
            raise HTTPException(404, "Sector not found")

        tf_safe = tf if tf in TF_KEYS else MIN_TF
        raw_data = self.data_service.build_dynamic_averages(tf_safe, ma, fast, slow)
        sector_syms = set(sector_map[sector_name])
        filtered_data = [INDEX_FIELDS] + [
            row for row in raw_data[1:] if row[0] in sector_syms
        ]

        return self._render_index(
            request,
            tf_safe,
            ma,
            fast,
            slow,
            filt,
            ma_act,
            sort,
            order,
            sector_list=filtered_data,
            sector_name=sector_name,
        )

    def symbol_detail(
        self,
        request: Request,
        symbol_name: str,
        tf: str = MIN_TF,
        ma: str = "rma",
        fast: int = 8,
        slow: int = 21,
    ) -> Any:
        tf = tf if tf in TF_KEYS else MIN_TF
        sym_list = self.data_service.cache.sym_list.get(tf, [])
        if symbol_name not in sym_list:
            raise HTTPException(404, "Symbol not found")

        si = sym_list.index(symbol_name)
        nd = self.data_service.cache.num_data[tf]
        wptr = self.data_service.cache.write_ptr[tf]

        vol = nd[si : si + 1, :wptr, VOL]
        price = nd[si : si + 1, :wptr, PRICE]

        ts_list = self.data_service.cache.ts_list[tf][:wptr]
        tsf_list = self.data_service.cache.tsf_list[tf][:wptr]

        def safe_float(v: float | None) -> float:
            return float(v) if v is not None and not math.isnan(v) else 0.0

        data = [["timestamp_full", "timestamp", "volume", "price"]] + [
            [
                tsf_list[i],
                ts_list[i],
                safe_float(vol[0, i]),
                safe_float(price[0, i]),
            ]
            for i in range(wptr)
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

    def api_indicator(
        self,
        symbol: str,
        tf: str = MIN_TF,
        source: str = "price",
        ind_type: str = "rma",
        p1: float = 8.0,
    ) -> dict[str, list[float | None]]:
        tf = tf if tf in TF_KEYS else MIN_TF
        sym_list = self.data_service.cache.sym_list.get(tf, [])
        if symbol not in sym_list:
            raise HTTPException(404, "Symbol not found")

        si = sym_list.index(symbol)
        nd = self.data_service.cache.num_data[tf]
        wptr = self.data_service.cache.write_ptr[tf]

        if source == "volume":
            base_data = nd[si : si + 1, :wptr, VOL]
        else:
            base_data = nd[si : si + 1, :wptr, PRICE]

        try:
            if ind_type == "raw":
                res = base_data[0]
            else:
                res = IndicatorFactory.calculate(ind_type, base_data, int(p1))[0]
        except (ValueError, TypeError, IndexError) as e:
            raise HTTPException(500, f"Calculation error: {e}") from e

        def safe_float(v: float) -> float | None:
            return float(v) if v is not None and not math.isnan(v) else None

        return {"data": [safe_float(x) for x in res]}

    def _render_index(
        self,
        request: Request,
        tf: str,
        ma_type: str,
        fast: int,
        slow: int,
        filt: str,
        ma_act: str,
        sort_key: str,
        order_by: str,
        sector_list: list | None = None,
        sector_name: str | None = None,
    ) -> Any:
        raw_data = sector_list or self.data_service.build_dynamic_averages(
            tf, ma_type, fast, slow
        )
        symbols_list, pos, neg, neut = self.data_service.filter_list(
            raw_data, filt, ma_act
        )

        if sort_key in INDEX_FIELDS:
            sidx = INDEX_FIELDS.index(sort_key)
            symbols_list[1:] = sorted(
                symbols_list[1:],
                key=lambda x: x[sidx] if x[sidx] is not None else -9999,
                reverse=(order_by != "asc"),
            )

        ref_t = self.data_service.get_refresh_time_str()
        self.data_service.dump_merge(
            tf,
            filt,
            ma_act,
            [sort_key, None],
            ref_t,
            order_by,
            ma_type,
            fast,
            slow,
            from_web=True,
        )

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
                "ma_act": ma_act,
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
