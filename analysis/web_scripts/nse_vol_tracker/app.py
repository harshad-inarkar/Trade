"""
app.py  –  NSE Intraday FastAPI Web Portal
------------------------------------------
Refactored for strict Object-Oriented Design, Dependency Injection, 
and TOML-based configuration (removing all global state and CLI clutter).
"""

import os
import threading
import time
import heapq
import csv as _csv
import tomllib
from datetime import datetime
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional, Any
from dataclasses import dataclass, field

import uvicorn
import argparse
from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.templating import Jinja2Templates

# ─── Custom Imports ───────────────────────────────────────────────────────────
from web_scripts.nse_vol_tracker.data_processor import INDEX_FIELDS, SYMB_COL
from web_scripts.nse_vol_tracker.cache_manager import CacheManager, MIN_TF, TF_KEYS
from utils.utility import wait_next_wall_clock
from utils.data.paths import (
    ROOT_DATA_DIR, OUT_DIR,
    _nse_data_dir, _intraday_dir, TEMPLATES_ROOT_DIR, REMOTE_DIR
)
from utils.data.sync_data import sync_data_args
from web_scripts.nse_vol_tracker.sector_loader import load_sector_symbols, UNIQ_CATEGORIES_CSV, CATEGORIES_CSV


app_config_file= Path(__file__).parent / "app_config.toml"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class AppConfig:
    start_session: Optional[str] = None
    end_session: Optional[str]   = None
    
    reload_interval: Optional[int] = None
    buffer_seconds: Optional[int]  = None
    
    remote_sync: Optional[bool]    = None
    
    filter_ltp: Optional[str]      = None
    sort_keys: Optional[list[str]] = None
    last_ndays: Optional[int] = None

    host: Optional[str] = None
    port: Optional[int] = None
    log_level: Optional[str] = None

    @classmethod
    def load_from_toml(cls, path: str | Path) -> "AppConfig":
        if not os.path.exists(path):
            print(f"[!] Config file {path} not found. Using defaults.")
            return cls()
            
        with open(path, "rb") as f:
            data = tomllib.load(f)

        c = cls()
        session = data.get("session", {})
        c.start_session = session.get("start", c.start_session)
        c.end_session   = session.get("end", c.end_session)

        reload = data.get("reload", {})
        c.reload_interval = reload.get("interval_minutes", c.reload_interval)
        c.buffer_seconds  = reload.get("buffer_seconds", c.buffer_seconds)

        sync = data.get("sync", {})
        c.remote_sync = sync.get("remote_sync", c.remote_sync)

        merge = data.get("merge", {})
        c.filter_ltp = merge.get("filter_ltp", c.filter_ltp)
        c.sort_keys  = merge.get("sort_keys", c.sort_keys)
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
    REFRESH_DT_PAT = 'Date: %d  Time: %H:%M'

    def __init__(self, config: AppConfig):
        self.config = config
        self.cache = CacheManager()
        
        # Resolve Paths
        self.root_path = ROOT_DATA_DIR
        self.intraday_path = str(Path(self.root_path) / _nse_data_dir / _intraday_dir)
        

        self.remote_dir = REMOTE_DIR
        self.remote_intraday_path = str(Path(self.remote_dir) / _nse_data_dir / _intraday_dir)
 

        # Initialize Templates
        template_dir =  Path(TEMPLATES_ROOT_DIR) / 'template_vol'
        self.templates = Jinja2Templates(directory=template_dir)

    def _sync_data(self):
        if self.config.remote_sync and self.remote_intraday_path:
            sync_data_args(self.remote_intraday_path, self.intraday_path)

    def load_all_data(self):
        self._sync_data()
        self.cache.load_files(self.intraday_path, self.config.last_ndays)
        
        ref_t = self.get_refresh_time_str()
        self.dump_merge(MIN_TF, self.config.filter_ltp, self.config.sort_keys, ref_t, 'desc')

    def get_refresh_time_str(self) -> str:
        dt = self.cache.get_refresh_time()
        return dt.strftime(self.REFRESH_DT_PAT) if dt else '-'

    def filter_list(self, symbols_list: list, filt: str) -> tuple[list, int, int, int]:
        start, end = 0, float('inf')
        pos_count = neg_count = neut_count = 0

        if filt:
            try:
                start, end = [int(x) for x in filt.split('-')]
            except ValueError:
                return symbols_list, pos_count, neg_count, neut_count

        ltp_idx = INDEX_FIELDS.index('ltp')
        pma_idx = INDEX_FIELDS.index('price_ma_action')

        filtered = [symbols_list[0]]
        for sym_data in symbols_list[1:]:
            val = sym_data[ltp_idx]
            if val is not None and start <= val <= end:
                pma = sym_data[pma_idx]
                if pma == 1: pos_count += 1
                elif pma == -1: neg_count += 1
                else: neut_count += 1
                filtered.append(sym_data)

        return filtered, pos_count, neg_count, neut_count

    def dump_index(self):
        ref_t = self.get_refresh_time_str()
        symbols_list = self.cache.get_symbols_avg(MIN_TF)
        os.makedirs(OUT_DIR, exist_ok=True)
        
        sym_idx = INDEX_FIELDS.index('symbol')
        with open(os.path.join(OUT_DIR, 'candidates.txt'), 'w') as out:
            for sym_data in symbols_list[1:]:
                out.write(f"{sym_data[sym_idx]}\n")
            out.write(f"Timeframes: {MIN_TF} | Refresh Time: {ref_t}\n")

    def dump_merge(self, tf: str, filt: str, sort_key_list: list, ref_t: str, order_by: str, from_web: bool = False):
        top = 40
        sym_idx = INDEX_FIELDS.index('symbol')
        initiator = 'Web' if from_web else 'Refresh'

        full_symb_list = self.cache.get_symbols_avg(tf)

        force_syms_path = os.path.join(OUT_DIR, 'candidates_force.txt')
        forced_symbols = set()
        if os.path.exists(force_syms_path):
            with open(force_syms_path, 'r') as f:
                forced_symbols = set()
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    forced_symbols.add(line)
           

        with open(os.path.join(OUT_DIR, 'sym_table.csv'), 'w', newline='') as out_csv, \
             open(os.path.join(OUT_DIR, 'candidates_merge.txt'), 'w') as out:

            writer = _csv.writer(out_csv)
            writer.writerow(INDEX_FIELDS)

            symbol_full_row_map = {}
            for skey in sort_key_list:
                if skey:
                    sidx = INDEX_FIELDS.index(skey if skey in INDEX_FIELDS else 'volume_fast')
                    symbols_list, _, _, _ = self.filter_list(full_symb_list, filt)
                    top_syms = heapq.nlargest(top, symbols_list[1:], key=lambda x: x[sidx])
                    for row in top_syms:
                        symbol_full_row_map[row[sym_idx]] = row

            sidx1 = INDEX_FIELDS.index(sort_key_list[0] if sort_key_list[0] in INDEX_FIELDS else 'volume_fast')
            items = list(symbol_full_row_map.items())
            
            sorted_sidx1 = sorted(items, key=lambda x: x[1][sidx1], reverse=True)
            sidx1_rank_map = {item[0]: rank for rank, item in enumerate(sorted_sidx1, 1)}

            sidx2_rank_map = {}
            sidx2_exist = bool(len(sort_key_list) > 1 and sort_key_list[1])
            if sidx2_exist:
                sidx2 = INDEX_FIELDS.index(sort_key_list[1] if sort_key_list[1] in INDEX_FIELDS else 'vol_surge')
                sorted_sidx2 = sorted(items, key=lambda x: x[1][sidx2], reverse=True)
                sidx2_rank_map = {item[0]: rank for rank, item in enumerate(sorted_sidx2, 1)}

            sorted_symbols = sorted(
                symbol_full_row_map.items(),
                key=lambda item: sidx1_rank_map[item[0]] + (0 if not sidx2_exist else sidx2_rank_map[item[0]]),
                reverse=False
            )

            if forced_symbols:
                for fsym in sorted(forced_symbols):
                    out.write(f"{fsym}\n")

            for sym, _ in sorted_symbols:
                row = symbol_full_row_map.get(sym)
                if row is not None:
                    writer.writerow(row)
                    out.write(f"{sym}\n")

            out.write(f"{initiator}|Timeframes: {tf} | sorted by {sort_key_list} {order_by} "
                      f"| Filter ltp {filt} | Refresh Time: {ref_t}\n")

    def render_index(self, request: Request, tf: str, filt: str, sort_key: str, order_by: str, sector_list=None, sector_name=None):
        desc = order_by != 'asc'
        symbols_list, pos, neg, neut = self.filter_list(
            sector_list if sector_list is not None else self.cache.get_symbols_avg(tf), 
            filt
        )

        if sort_key:
            sidx = INDEX_FIELDS.index(sort_key)
            symbols_list[1:] = sorted(symbols_list[1:], key=lambda x: x[sidx], reverse=desc)

        ref_t = self.get_refresh_time_str()
        self.dump_merge(tf, filt, [sort_key, None], ref_t, order_by, from_web=True)

        return self.templates.TemplateResponse(
            request, 'index.html',
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
            }
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Background Thread Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class BackgroundReloader:
    """Manages the periodic cache reloading loop."""
    def __init__(self, service: MarketDataService, config: AppConfig):
        self.service = service
        self.config = config

    def start(self):
        if self.config.reload_interval:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self):
        print(f"🔁 Reloads every {self.config.reload_interval} minutes – buffer: {self.config.buffer_seconds}s")
        start_session_time = datetime.strptime(self.config.start_session, '%H%M').time()
        cutoff             = datetime.strptime(self.config.end_session, '%H%M').time()

        while True:
            wait_next_wall_clock(self.config.reload_interval, self.config.buffer_seconds)
            current_time = datetime.now().time()
            
            if current_time > cutoff or current_time < start_session_time:
                print(f"⏹ Reload skipped: outside session {self.config.start_session}–{self.config.end_session}. "
                      f"Current: {current_time.strftime('%H%M')}")
                continue
                
            t0 = time.time()
            self.service.load_all_data()
            print(f"⏱ Reload took {time.time() - t0:.2f}s")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FastAPI Setup & Dependency Injection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app_cfg = AppConfig.load_from_toml(app_config_file)
data_service = MarketDataService(app_cfg)
reloader = BackgroundReloader(data_service, app_cfg)

@asynccontextmanager
async def lifespan(app: FastAPI):
    t0 = time.time()
    data_service.load_all_data()
    data_service.dump_index()
    print(f"⏱ Initial load took {time.time() - t0:.2f}s")
    
    reloader.start()
    yield

app = FastAPI(title='NSE Intraday Portal', lifespan=lifespan)

# Attach service to state for dependency injection
app.state.service = data_service

def get_service(request: Request) -> MarketDataService:
    return request.app.state.service

def _tf_safe(tf: str) -> str:
    return tf if tf in TF_KEYS else MIN_TF

def _sort_safe(sort: str) -> str:
    return sort if sort in INDEX_FIELDS else 'volume_fast'


# ── ROUTES ─────────────────────────────────────────────────────────────────────

@app.get('/')
def index(
    request: Request,
    tf:    str = Query(default=MIN_TF),
    filter: str = Query(default=''),  
    sort:  str = Query(default=''),
    order: str = Query(default=''),
    service: MarketDataService = Depends(get_service)
):
    return service.render_index(
        request, _tf_safe(tf), filter, _sort_safe(sort), 
        'asc' if order == 'asc' else 'desc'
    )


@app.get('/symbol/{symbol_name}')
def symbol_detail(
    request: Request,
    symbol_name: str,
    tf: str = Query(default=MIN_TF),
    service: MarketDataService = Depends(get_service)
):
    tf = _tf_safe(tf)
    symbols_data = service.cache.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        raise HTTPException(status_code=404, detail='Symbol not found')
        
    return service.templates.TemplateResponse(
        request, 'symbol.html',
        {
            'symbol':    symbol_name,
            'data':      symbols_data[symbol_name],
            'timeframe': tf,
        }
    )


@app.get('/api/symbol/{symbol_name}')
def api_symbol(
    symbol_name: str,
    tf: str = Query(default=MIN_TF),
    service: MarketDataService = Depends(get_service)
):
    tf = _tf_safe(tf)
    symbols_data = service.cache.get_symbols_data(tf)
    if symbol_name not in symbols_data:
        raise HTTPException(status_code=404, detail='Symbol not found')
    return {SYMB_COL: symbol_name, 'data': symbols_data[symbol_name]}


@app.get('/sectors/{sector}')
def sector_index(
    request: Request,
    sector: str,
    tf:       str = Query(default=MIN_TF),
    uniq_cat: bool = Query(default=False),
    filter:   str = Query(default=''),
    sort:     str = Query(default=''),
    order:    str = Query(default=''),
    service: MarketDataService = Depends(get_service)
):
    tf = _tf_safe(tf)
    csv_path = UNIQ_CATEGORIES_CSV if uniq_cat else CATEGORIES_CSV
    sector_symbols = load_sector_symbols(csv_path=csv_path)

    all_syms_data = service.cache.get_symbols_avg(tf)
    sector_syms_set = set(sector_symbols.get(sector, []))
    sym_idx = INDEX_FIELDS.index('symbol')

    sector_list = [all_syms_data[0]]  # header
    for sym_data in all_syms_data[1:]:
        if sym_data[sym_idx] in sector_syms_set:
            sector_list.append(sym_data)

    return service.render_index(
        request, tf, filter, _sort_safe(sort), 
        'asc' if order == 'asc' else 'desc',
        sector_list=sector_list, sector_name=sector
    )


@app.get('/sectors')
@app.get('/sectors/')
def sectors(
    request: Request,
    tf:       str = Query(default=MIN_TF),
    uniq_cat: bool = Query(default=False),
    filter:   str = Query(default=''),
    sort:     str = Query(default=''),
    order:    str = Query(default='desc'),
    service: MarketDataService = Depends(get_service)
):
    tf = _tf_safe(tf)
    sort_key = _sort_safe(sort)
    order_by = 'asc' if order == 'asc' else 'desc'
    desc_flag = order_by != 'asc'

    csv_path = UNIQ_CATEGORIES_CSV if uniq_cat else CATEGORIES_CSV
    sector_symbols = load_sector_symbols(csv_path=csv_path)

    sort_v_idx = INDEX_FIELDS.index(sort_key)
    sym_idx = INDEX_FIELDS.index('symbol')

    avg_rows, _, _, _ = service.filter_list(service.cache.get_symbols_avg(tf), filter)

    vol_lookup = {}
    for row in avg_rows[1:]:
        sym, val = row[sym_idx], row[sort_v_idx]
        if sym and val is not None:
            vol_lookup[sym] = val

    sector_list = []
    for sector_name, syms in sector_symbols.items():
        if not syms: continue
        syms = [s for s in syms if s in vol_lookup]
        vols = [vol_lookup[s] for s in syms]
        if not vols: continue

        avg_vol = sum(vols) / len(vols)
        sorted_syms = sorted(syms, key=lambda s: vol_lookup[s], reverse=desc_flag)

        sector_list.append({
            'name':            sector_name,
            'symbols':         sorted_syms,
            'symbol_count':    len(sorted_syms),
            'avg_volume_fast': round(avg_vol, 2),
            'top_symbol':      sorted_syms[0] if sorted_syms else None,
            'heat_pct':        0.0,
        })

    valid_vols = [s['avg_volume_fast'] for s in sector_list if s['avg_volume_fast'] is not None]
    if valid_vols:
        min_v, max_v = min(valid_vols), max(valid_vols)
        span = (max_v - min_v) or 1
        for s in sector_list:
            if s['avg_volume_fast'] is not None:
                s['heat_pct'] = (s['avg_volume_fast'] - min_v) / span

    sector_list.sort(key=lambda s: s['avg_volume_fast'] or 0, reverse=desc_flag)

    return service.templates.TemplateResponse(
        request, 'sectoral_index.html',
        {
            'sectors':      sector_list,
            'timeframe':    tf,
            'refresh_time': service.get_refresh_time_str(),
            'filter':       filter,
            'sort':         sort_key,
            'order':        order_by,
        }
    )

# ── RUN ────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # Provide a tiny fallback parser solely for pointing to a custom config file path
 
    uvicorn.run(app, host=app_cfg.host, port=app_cfg.port, log_level=app_cfg.log_level)