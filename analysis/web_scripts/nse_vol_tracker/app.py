"""
app.py  -  NSE Intraday FastAPI Web Portal
------------------------------------------
Refactored for strict Object-Oriented Design, Dependency Injection,
and TOML-based configuration (removing all global state and CLI clutter).
"""

import csv as _csv
import heapq
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import tomllib
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates

from utils.data.paths import (
    OUT_DIR,
    REMOTE_DIR,
    ROOT_DATA_DIR,
    TEMPLATES_ROOT_DIR,
    _intraday_dir,
    _nse_data_dir,
)
from utils.data.sync_data import sync_data_args
from utils.utility import INDIA_TZ, out, wait_next_wall_clock
from web_scripts.nse_vol_tracker.cache_manager import MIN_TF, TF_KEYS, CacheManager

# ─── Custom Imports ───────────────────────────────────────────────────────────
from web_scripts.nse_vol_tracker.data_processor import INDEX_FIELDS, SYMB_COL
from web_scripts.nse_vol_tracker.sector_loader import (
    CATEGORIES_CSV,
    UNIQ_CATEGORIES_CSV,
    load_sector_symbols,
)

app_config_file = Path(__file__).parent / "app_config.toml"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class AppConfig:
    start_session: str | None = None
    end_session: str | None = None

    reload_interval: int | None = None
    buffer_seconds: int | None = None

    remote_sync: bool | None = None

    filter_ltp: str | None = None
    sort_keys: list[str] | None = None
    last_ndays: int | None = None

    host: str | None = None
    port: int | None = None
    log_level: str | None = None

    @classmethod
    def load_from_toml(cls, path: str | Path) -> "AppConfig":
        config_path = Path(path)
        if not config_path.exists():
            out(f"[!] Config file {path} not found. Using defaults.")
            return cls()

        with config_path.open("rb") as f:
            data = tomllib.load(f)

        c = cls()
        session = data.get("session", {})
        c.start_session = session.get("start", c.start_session)
        c.end_session = session.get("end", c.end_session)

        reload = data.get("reload", {})
        c.reload_interval = reload.get("interval_minutes", c.reload_interval)
        c.buffer_seconds = reload.get("buffer_seconds", c.buffer_seconds)

        sync = data.get("sync", {})
        c.remote_sync = sync.get("remote_sync", c.remote_sync)

        merge = data.get("merge", {})
        c.filter_ltp = merge.get("filter_ltp", c.filter_ltp)
        c.sort_keys = merge.get("sort_keys", c.sort_keys)
        ndays = merge.get("last_ndays", 0)
        c.last_ndays = ndays if ndays > 0 else None

        server = data.get("server", {})
        c.host = server.get("host", c.host)
        c.port = server.get("port", c.port)
        c.log_level = server.get("log_level", c.log_level)

        return c


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Market Data Service (Core Logic)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class MarketDataService:
    """Encapsulates all cache loading, filtering, and template rendering logic."""

    REFRESH_DT_PAT = "Date: %d  Time: %H:%M"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.cache = CacheManager()

        # Resolve Paths
        self.root_path = ROOT_DATA_DIR
        self.intraday_path = str(Path(self.root_path) / _nse_data_dir / _intraday_dir)

        self.remote_dir = REMOTE_DIR
        self.remote_intraday_path = str(
            Path(self.remote_dir) / _nse_data_dir / _intraday_dir,
        )

        # Initialize Templates
        template_dir = Path(TEMPLATES_ROOT_DIR) / "template_vol"
        self.templates = Jinja2Templates(directory=template_dir)

    def _sync_data(self) -> None:
        if self.config.remote_sync and self.remote_intraday_path:
            sync_data_args(self.remote_intraday_path, self.intraday_path)

    def load_all_data(self) -> None:
        self._sync_data()
        self.cache.load_files(self.intraday_path, self.config.last_ndays)

        ref_t = self.get_refresh_time_str()
        self.dump_merge(
            MIN_TF,
            self.config.filter_ltp or "",
            self.config.sort_keys or [],
            ref_t,
            "desc",
        )

    def get_refresh_time_str(self) -> str:
        dt = self.cache.get_refresh_time()
        return dt.strftime(self.REFRESH_DT_PAT) if dt else "-"

    def filter_list(self, symbols_list: list, filt: str) -> tuple[list, int, int, int]:
        start, end = 0, float("inf")
        pos_count = neg_count = neut_count = 0

        if filt:
            try:
                start, end = [int(x) for x in filt.split("-")]
            except ValueError:
                return symbols_list, pos_count, neg_count, neut_count

        ltp_idx = INDEX_FIELDS.index("ltp")
        pma_idx = INDEX_FIELDS.index("price_ma_action")

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

    def dump_index(self) -> None:
        ref_t = self.get_refresh_time_str()
        symbols_list = self.cache.get_symbols_avg(MIN_TF) or []
        Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

        sym_idx = INDEX_FIELDS.index("symbol")
        ltp_idx = INDEX_FIELDS.index("ltp")

        # Sort symbols_list (excluding header) in reverse order by 'ltp' field
        data_rows = symbols_list[1:]
        data_rows_sorted = sorted(
            data_rows,
            key=lambda x: x[ltp_idx] if x[ltp_idx] is not None else float("-inf"),
            reverse=True,
        )

        # Group by ltp in steps of 500 and add comments for each range
        step = 1000
        max_ltp = max(
            (row[ltp_idx] for row in data_rows_sorted if row[ltp_idx] is not None),
            default=0,
        )

        # Compute group boundaries
        group_ranges = []
        curr = 0
        while curr <= max_ltp:
            group_ranges.append((curr, curr + step))
            curr += step

        cand_txt = Path(OUT_DIR) / "candidates.txt"
        with cand_txt.open("w") as write_out:
            # Write groups in descending order (from max to min)
            for start, end in reversed(group_ranges):
                group_symbols = [
                    sym_data
                    for sym_data in data_rows_sorted
                    if sym_data[ltp_idx] is not None
                    and start <= sym_data[ltp_idx] < end
                ]
                if not group_symbols:
                    continue  # Skip writing group name if group is empty
                write_out.write(f"# Less than {end}\n")
                write_out.writelines(
                    f"{sym_data[sym_idx]}\n" for sym_data in group_symbols
                )
            write_out.write(f"# Timeframe: {MIN_TF} | Refresh Time: {ref_t}\n")

    def dump_merge(
        self,
        tf: str,
        filt: str,
        sort_key_list: list,
        ref_t: str,
        order_by: str,
        *,
        from_web: bool = False,
    ) -> None:
        top = 40
        sym_idx = INDEX_FIELDS.index("symbol")
        initiator = "Web" if from_web else "Refresh"

        full_symb_list = self.cache.get_symbols_avg(tf)

        force_syms_path = Path(OUT_DIR) / "candidates_force.txt"
        forced_symbols = set()
        if force_syms_path.exists():
            with force_syms_path.open() as f:
                forced_symbols = set()
                for raw_line in f:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
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
                        skey if skey in INDEX_FIELDS else "volume_fast",
                    )
                    symbols_list, _, _, _ = self.filter_list(full_symb_list, filt)
                    top_syms = heapq.nlargest(
                        top,
                        symbols_list[1:],
                        key=lambda x: x[sidx],
                    )
                    for row in top_syms:
                        symbol_full_row_map[row[sym_idx]] = row

            sidx1 = INDEX_FIELDS.index(
                sort_key_list[0] if sort_key_list[0] in INDEX_FIELDS else "volume_fast",
            )
            items = list(symbol_full_row_map.items())

            sorted_sidx1 = sorted(items, key=lambda x: x[1][sidx1], reverse=True)
            sidx1_rank_map = {
                item[0]: rank for rank, item in enumerate(sorted_sidx1, 1)
            }

            sidx2_rank_map = {}
            sidx2_exist = bool(len(sort_key_list) > 1 and sort_key_list[1])
            if sidx2_exist:
                sidx2 = INDEX_FIELDS.index(
                    sort_key_list[1]
                    if sort_key_list[1] in INDEX_FIELDS
                    else "vol_surge",
                )
                sorted_sidx2 = sorted(items, key=lambda x: x[1][sidx2], reverse=True)
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
                f"| Filter ltp {filt} | Refresh Time: {ref_t}\n",
            )

    def render_index(
        self,
        request: Request,
        tf: str,
        filt: str,
        sort_key: str,
        order_by: str,
        sector_list: list | None = None,
        sector_name: str | None = None,
    ) -> Any:
        desc = order_by != "asc"
        symbols_list, pos, neg, neut = self.filter_list(
            sector_list if sector_list is not None else self.cache.get_symbols_avg(tf),
            filt,
        )

        if sort_key:
            sidx = INDEX_FIELDS.index(sort_key)
            symbols_list[1:] = sorted(
                symbols_list[1:],
                key=lambda x: x[sidx],
                reverse=desc,
            )

        ref_t = self.get_refresh_time_str()
        self.dump_merge(tf, filt, [sort_key, None], ref_t, order_by, from_web=True)

        return self.templates.TemplateResponse(
            request,
            "index.html",
            {
                "symbols": symbols_list,
                "count": len(symbols_list),
                "refresh_time": ref_t,
                "timeframe": tf,
                "sort": sort_key,
                "order": order_by,
                "filter": filt,
                "pos_count": pos,
                "neg_count": neg,
                "neut_count": neut,
                "sector_name": sector_name,
            },
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Background Thread Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class BackgroundReloader:
    """Manages the periodic cache reloading loop."""

    def __init__(self, service: MarketDataService, config: AppConfig) -> None:
        self.service = service
        self.config = config

    def start(self) -> None:
        if self.config.reload_interval:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        out(
            f"🔁 Reloads every {self.config.reload_interval} minutes - "
            f"buffer: {self.config.buffer_seconds}s"
        )
        if not self.config.start_session or not self.config.end_session:
            return

        start_session_time = (
            datetime.strptime(self.config.start_session, "%H%M")
            .replace(tzinfo=INDIA_TZ)
            .time()
        )
        cutoff = (
            datetime.strptime(self.config.end_session, "%H%M")
            .replace(tzinfo=INDIA_TZ)
            .time()
        )

        while True:
            wait_next_wall_clock(
                self.config.reload_interval,
                self.config.buffer_seconds or 0,
            )
            current_time = datetime.now(INDIA_TZ).time()

            if current_time > cutoff or current_time < start_session_time:
                out(
                    f"⏹ Reload skipped: outside session {self.config.start_session}-"
                    f"{self.config.end_session}. "
                    f"Current: {current_time.strftime('%H%M')}"
                )
                continue

            t0 = time.time()
            self.service.load_all_data()
            out(f"⏱ Reload took {time.time() - t0:.2f}s")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FastAPI Setup & Dependency Injection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app_cfg = AppConfig.load_from_toml(app_config_file)
data_service = MarketDataService(app_cfg)
reloader = BackgroundReloader(data_service, app_cfg)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:  # noqa: ARG001
    t0 = time.time()
    data_service.load_all_data()
    data_service.dump_index()
    out(f"⏱ Initial load took {time.time() - t0:.2f}s")

    reloader.start()
    yield


app = FastAPI(title="NSE Intraday Portal", lifespan=lifespan)

# Attach service to state for dependency injection
app.state.service = data_service


def get_service(request: Request) -> MarketDataService:
    return request.app.state.service


def _tf_safe(tf: str) -> str:
    return tf if tf in TF_KEYS else MIN_TF


def _sort_safe(sort: str) -> str:
    return sort if sort in INDEX_FIELDS else "volume_fast"


# ── ROUTES ─────────────────────────────────────────────────────────────────────


@app.get("/")
def index(
    request: Request,
    service: Annotated[MarketDataService, Depends(get_service)],
    *,
    tf: Annotated[str, Query()] = MIN_TF,
    filt: Annotated[str, Query(alias="filter")] = "",
    sort: Annotated[str, Query()] = "",
    order: Annotated[str, Query()] = "",
) -> Any:
    return service.render_index(
        request,
        _tf_safe(tf),
        filt,
        _sort_safe(sort),
        "asc" if order == "asc" else "desc",
    )


@app.get("/symbol/{symbol_name}")
def symbol_detail(
    request: Request,
    symbol_name: str,
    service: Annotated[MarketDataService, Depends(get_service)],
    *,
    tf: Annotated[str, Query()] = MIN_TF,
) -> Any:
    tf = _tf_safe(tf)
    symbols_data = service.cache.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        raise HTTPException(status_code=404, detail="Symbol not found")

    return service.templates.TemplateResponse(
        request,
        "symbol.html",
        {
            "symbol": symbol_name,
            "data": symbols_data[symbol_name],
            "timeframe": tf,
        },
    )


@app.get("/api/symbol/{symbol_name}")
def api_symbol(
    symbol_name: str,
    service: Annotated[MarketDataService, Depends(get_service)],
    *,
    tf: Annotated[str, Query()] = MIN_TF,
) -> Any:
    tf = _tf_safe(tf)
    symbols_data = service.cache.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return {SYMB_COL: symbol_name, "data": symbols_data[symbol_name]}


@app.get("/sectors/{sector}")
def sector_index(
    request: Request,
    sector: str,
    service: Annotated[MarketDataService, Depends(get_service)],
    *,
    tf: Annotated[str, Query()] = MIN_TF,
    uniq_cat: Annotated[bool, Query()] = False,
    filt: Annotated[str, Query(alias="filter")] = "",
    sort: Annotated[str, Query()] = "",
    order: Annotated[str, Query()] = "",
) -> Any:
    tf = _tf_safe(tf)
    csv_path = str(UNIQ_CATEGORIES_CSV) if uniq_cat else str(CATEGORIES_CSV)
    sector_symbols = load_sector_symbols(csv_path=csv_path)

    all_syms_data = service.cache.get_symbols_avg(tf) or []
    sector_syms_set = set(sector_symbols.get(sector, []))
    sym_idx = INDEX_FIELDS.index("symbol")

    sector_list = [all_syms_data[0]]  # header
    sector_list.extend(sd for sd in all_syms_data[1:] if sd[sym_idx] in sector_syms_set)

    return service.render_index(
        request,
        tf,
        filt,
        _sort_safe(sort),
        "asc" if order == "asc" else "desc",
        sector_list=sector_list,
        sector_name=sector,
    )


@app.get("/sectors")
@app.get("/sectors/")
def sectors(
    request: Request,
    service: Annotated[MarketDataService, Depends(get_service)],
    *,
    tf: Annotated[str, Query()] = MIN_TF,
    uniq_cat: Annotated[bool, Query()] = False,
    filt: Annotated[str, Query(alias="filter")] = "",
    sort: Annotated[str, Query()] = "",
    order: Annotated[str, Query()] = "desc",
) -> Any:
    tf = _tf_safe(tf)
    sort_key = _sort_safe(sort)
    order_by = "asc" if order == "asc" else "desc"
    desc_flag = order_by != "asc"

    csv_path = str(UNIQ_CATEGORIES_CSV) if uniq_cat else str(CATEGORIES_CSV)
    sector_symbols = load_sector_symbols(csv_path=csv_path)

    sort_v_idx = INDEX_FIELDS.index(sort_key)
    sym_idx = INDEX_FIELDS.index("symbol")

    avg_rows, _, _, _ = service.filter_list(
        service.cache.get_symbols_avg(tf) or [], filt
    )

    vol_lookup = {}
    for row in avg_rows[1:]:
        sym, val = row[sym_idx], row[sort_v_idx]
        if sym and val is not None:
            vol_lookup[sym] = val

    sector_list = []
    for sector_name, syms in sector_symbols.items():
        if not syms:
            continue
        valid_syms = [s for s in syms if s in vol_lookup]
        vols = [vol_lookup[s] for s in valid_syms]
        if not vols:
            continue

        avg_vol = sum(vols) / len(vols)
        sorted_syms = sorted(valid_syms, key=lambda s: vol_lookup[s], reverse=desc_flag)

        sector_list.append(
            {
                "name": sector_name,
                "symbols": sorted_syms,
                "symbol_count": len(sorted_syms),
                "avg_volume_fast": round(avg_vol, 2),
                "top_symbol": sorted_syms[0] if sorted_syms else None,
                "heat_pct": 0.0,
            },
        )

    valid_vols = [
        s["avg_volume_fast"] for s in sector_list if s["avg_volume_fast"] is not None
    ]
    if valid_vols:
        min_v, max_v = min(valid_vols), max(valid_vols)
        span = (max_v - min_v) or 1
        for s in sector_list:
            if s["avg_volume_fast"] is not None:
                s["heat_pct"] = (s["avg_volume_fast"] - min_v) / span

    sector_list.sort(key=lambda s: s["avg_volume_fast"] or 0, reverse=desc_flag)

    return service.templates.TemplateResponse(
        request,
        "sectoral_index.html",
        {
            "sectors": sector_list,
            "timeframe": tf,
            "refresh_time": service.get_refresh_time_str(),
            "filter": filt,
            "sort": sort_key,
            "order": order_by,
        },
    )


# ── RUN ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=app_cfg.host or "localhost",
        port=app_cfg.port or 5000,
        log_level=app_cfg.log_level,
    )
