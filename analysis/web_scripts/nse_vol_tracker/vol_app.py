"""
app.py  -  NSE Intraday FastAPI Web Portal
------------------------------------------
Refactored for dynamic indicator processing.
Includes dynamic disk-dumping logic for trade candidates.
"""

import csv as _csv
import heapq
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import tomllib
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from utils.data.paths import (
    NSE_INTRADAY_DIR_PATH,
    OUT_DIR,
    TEMPLATES_ROOT_DIR,
)
from utils.utility import (
    _str_env_or_cfg,
    out,
    set_logger_config,
    wait_next_wall_clock,
)
from web_scripts.nse_vol_tracker.cache_manager import MIN_TF, TF_KEYS, CacheManager
from web_scripts.nse_vol_tracker.data_processor import PRICE, VOL
from web_scripts.nse_vol_tracker.indicators import IndicatorFactory
from web_scripts.nse_vol_tracker.sector_loader import load_sector_symbols

app_config_file = Path(__file__).parent / "vol_app_config.toml"

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


@dataclass
class AppConfig:
    start_session: str | None = "0915"
    end_session: str | None = "1530"
    reload_interval: int | None = 3
    buffer_seconds: int | None = -12
    last_ndays: int | None = 14

    filter_ltp: str | None = None
    sort_keys: list[str] | None = None

    host: str | None = "localhost"
    port: int | None = 5000
    log_level: str | None = "critical"

    @classmethod
    def load_from_toml(cls, path: str | Path) -> "AppConfig":
        config_path = Path(path)
        if not config_path.exists():
            return cls()
        with config_path.open("rb") as f:
            data = tomllib.load(f)
        c = cls()
        c.start_session = data.get("session", {}).get("start", c.start_session)
        c.end_session = data.get("session", {}).get("end", c.end_session)

        merge = data.get("merge", {})
        c.filter_ltp = merge.get("filter_ltp", c.filter_ltp)
        c.sort_keys = merge.get("sort_keys", c.sort_keys)
        ndays = merge.get("last_ndays", 0)
        c.last_ndays = ndays if ndays > 0 else c.last_ndays

        srv = data.get("server", {})
        c.log_level = _str_env_or_cfg("log_level", srv, c.log_level)

        if not bool(c.log_level):
            c.log_level = "critical"

        c.host = srv.get("host", c.host)
        c.port = srv.get("port", c.port)

        return c


class MarketDataService:
    REFRESH_DT_PAT = "Date: %d  Time: %H:%M"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.cache = CacheManager()
        self.intraday_path = NSE_INTRADAY_DIR_PATH
        template_dir = Path(TEMPLATES_ROOT_DIR) / "template_vol"
        self.templates = Jinja2Templates(directory=template_dir)

    def load_all_data(
        self, ma_type: str = "rma", fast: int = 8, slow: int = 21
    ) -> None:
        """Loads cache and immediately triggers the merge dump for external tools."""
        self.cache.load_files(self.intraday_path, self.config.last_ndays)
        ref_t = self.get_refresh_time_str()
        self.dump_merge(
            MIN_TF,
            self.config.filter_ltp or "",
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

    def filter_list(self, symbols_list: list, filt: str) -> tuple[list, int, int, int]:
        start, end = 0, float("inf")
        pos_count = neg_count = neut_count = 0
        if filt:
            try:
                start, end = [int(x) for x in filt.split("-")]
            except ValueError:
                return symbols_list, 0, 0, 0

        ltp_idx = INDEX_FIELDS.index("ltp")
        pma_idx = INDEX_FIELDS.index("price_ma_action")

        filtered = [symbols_list[0]]
        for row in symbols_list[1:]:
            if row[ltp_idx] is not None and start <= row[ltp_idx] <= end:
                if row[pma_idx] == 1:
                    pos_count += 1
                elif row[pma_idx] == -1:
                    neg_count += 1
                else:
                    neut_count += 1
                filtered.append(row)
        return filtered, pos_count, neg_count, neut_count

    def dump_index(self, ma_type: str = "rma", fast: int = 8, slow: int = 21) -> None:
        """Groups symbols by 1000 LTP intervals and writes to candidates.txt"""
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
        sort_key_list: list,
        ref_t: str,
        order_by: str,
        ma_type: str = "rma",
        fast: int = 8,
        slow: int = 21,
        *,
        from_web: bool = False,
    ) -> None:
        """Generates the sym_table.csv and candidates_merge.txt for scanning."""
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
                    symbols_list, _, _, _ = self.filter_list(full_symb_list, filt)

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
                f"| Filter ltp {filt} | Refresh Time: {ref_t}\n"
            )

    def render_index(
        self,
        request: Request,
        tf: str,
        ma_type: str,
        fast: int,
        slow: int,
        filt: str,
        sort_key: str,
        order_by: str,
        sector_list: list | None = None,
        sector_name: str | None = None,
    ) -> Any:
        raw_data = sector_list or self.build_dynamic_averages(tf, ma_type, fast, slow)
        symbols_list, pos, neg, neut = self.filter_list(raw_data, filt)

        if sort_key in INDEX_FIELDS:
            sidx = INDEX_FIELDS.index(sort_key)
            symbols_list[1:] = sorted(
                symbols_list[1:],
                key=lambda x: x[sidx] if x[sidx] is not None else -9999,
                reverse=(order_by != "asc"),
            )

        # Trigger dump for external scanners reflecting user's web view
        ref_t = self.get_refresh_time_str()
        self.dump_merge(
            tf,
            filt,
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
                "pos_count": pos,
                "neg_count": neg,
                "neut_count": neut,
                "sector_name": sector_name,
            },
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


app_cfg = AppConfig.load_from_toml(app_config_file)
if bool(app_cfg.log_level):
    set_logger_config(log_level=app_cfg.log_level)
data_service = MarketDataService(app_cfg)
reloader = BackgroundReloader(data_service, app_cfg)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:  # noqa: ARG001
    t0 = time.time()
    data_service.load_all_data()
    data_service.dump_index()  # Added back the default dump_index trigger
    out(f"⏱ Initial load took {time.time() - t0:.2f}s")
    reloader.start()
    yield


app = FastAPI(title="NSE Intraday Portal", lifespan=lifespan)
app.state.service = data_service
app.mount(
    "/static",
    StaticFiles(directory=str(Path(TEMPLATES_ROOT_DIR) / "template_vol" / "static")),
    name="static",
)


def get_service(request: Request) -> MarketDataService:
    return request.app.state.service


@app.get("/")
def index(
    request: Request,
    service: Annotated[MarketDataService, Depends(get_service)],
    tf: str = MIN_TF,
    ma: str = "rma",
    fast: int = 8,
    slow: int = 21,
    filt: Annotated[str, Query(alias="filter")] = "",
    sort: str = "volume_fast",
    order: str = "desc",
) -> Any:
    return service.render_index(
        request, tf if tf in TF_KEYS else MIN_TF, ma, fast, slow, filt, sort, order
    )


@app.get("/sectors/")
def sectors_index(
    request: Request,
    service: Annotated[MarketDataService, Depends(get_service)],
    tf: str = MIN_TF,
    ma: str = "rma",
    fast: int = 8,
    slow: int = 21,
    filt: Annotated[str, Query(alias="filter")] = "",
    sort: str = "vol",
    order: str = "desc",
) -> Any:
    tf_safe = tf if tf in TF_KEYS else MIN_TF
    raw_data = service.build_dynamic_averages(tf_safe, ma, fast, slow)
    data_map = {row[0]: row for row in raw_data[1:]}

    sector_map = load_sector_symbols()
    sectors_data = []

    for sec_name, symbols in sector_map.items():
        sec_vfast = []
        valid_syms = []
        for sym in symbols:
            if sym in data_map:
                valid_syms.append(sym)
                sec_vfast.append(data_map[sym][1])  # volume_fast

        if valid_syms:
            avg_vfast = sum(sec_vfast) / len(sec_vfast)
            heat_pct = min(avg_vfast / 200.0, 1.0)

            sectors_data.append(
                {
                    "name": sec_name,
                    "symbols": valid_syms,
                    "symbol_count": len(valid_syms),
                    "avg_volume_fast": avg_vfast,
                    "heat_pct": heat_pct,
                    "top_symbol": max(valid_syms, key=lambda s: data_map[s][1])
                    if valid_syms
                    else None,
                }
            )

    rev = order == "desc"
    if sort == "vol":
        sectors_data.sort(key=lambda x: x["avg_volume_fast"], reverse=rev)
    elif sort == "name":
        sectors_data.sort(key=lambda x: x["name"], reverse=rev)

    return service.templates.TemplateResponse(
        request,
        "sectoral_index.html",
        {
            "sectors": sectors_data,
            "refresh_time": service.get_refresh_time_str(),
            "timeframe": tf_safe,
            "ma_type": ma,
            "fast": fast,
            "slow": slow,
            "sort": sort,
            "order": order,
            "current_sort": sort,
            "current_order": order,
            "filter": filt,
        },
    )


@app.get("/sectors/{sector_name}/")
def sector_detail(
    request: Request,
    sector_name: str,
    service: Annotated[MarketDataService, Depends(get_service)],
    tf: str = MIN_TF,
    ma: str = "rma",
    fast: int = 8,
    slow: int = 21,
    filt: Annotated[str, Query(alias="filter")] = "",
    sort: str = "volume_fast",
    order: str = "desc",
) -> Any:
    sector_map = load_sector_symbols()
    if sector_name not in sector_map:
        raise HTTPException(404, "Sector not found")

    tf_safe = tf if tf in TF_KEYS else MIN_TF
    raw_data = service.build_dynamic_averages(tf_safe, ma, fast, slow)
    sector_syms = set(sector_map[sector_name])
    filtered_data = [INDEX_FIELDS] + [
        row for row in raw_data[1:] if row[0] in sector_syms
    ]

    return service.render_index(
        request,
        tf_safe,
        ma,
        fast,
        slow,
        filt,
        sort,
        order,
        sector_list=filtered_data,
        sector_name=sector_name,
    )


@app.get("/symbol/{symbol_name}")
def symbol_detail(
    request: Request,
    symbol_name: str,
    service: Annotated[MarketDataService, Depends(get_service)],
    tf: str = MIN_TF,
    ma: str = "rma",
    fast: int = 8,
    slow: int = 21,
) -> Any:
    tf = tf if tf in TF_KEYS else MIN_TF
    sym_list = service.cache.sym_list.get(tf, [])
    if symbol_name not in sym_list:
        raise HTTPException(404, "Symbol not found")

    si = sym_list.index(symbol_name)
    nd = service.cache.num_data[tf]
    wptr = service.cache.write_ptr[tf]

    vol = nd[si : si + 1, :wptr, VOL]
    price = nd[si : si + 1, :wptr, PRICE]

    vfast = IndicatorFactory.calculate(ma, vol, fast)[0]
    vslow = IndicatorFactory.calculate(ma, vol, slow)[0]
    pfast = IndicatorFactory.calculate(ma, price, fast)[0]
    pslow = IndicatorFactory.calculate(ma, price, slow)[0]

    ts_list = service.cache.ts_list[tf][:wptr]
    tsf_list = service.cache.tsf_list[tf][:wptr]

    data = [CACHE_FIELDS] + [
        [
            tsf_list[i],
            ts_list[i],
            float(vslow[i]),
            float(vfast[i]),
            float(price[0, i]),
            float(pfast[i]),
            float(pslow[i]),
        ]
        for i in range(wptr)
    ]

    return service.templates.TemplateResponse(
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


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=app_cfg.host,
        port=app_cfg.port,
        log_level=app_cfg.log_level,
    )
