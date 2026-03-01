"""
cache_manager.py
----------------
Thread-safe multi-timeframe cache.

Single public entry point:  load_files(data_dir)
  • Cache not ready / no refresh_time  →  full load
  • Cache ready with refresh_time      →  incremental

Both paths go through _load_csv_files() which runs the SAME pipeline:
  read → plant seed boundary col → fill_gaps_numpy → post_process → timestamps

The only incremental difference:
  - Only new files are read (from last_committed+1).
  - Saved O(N) seed vectors are written into column from_index-1 BEFORE
    fill_gaps_numpy runs, giving it a proper left anchor.
  - Saved RMA seeds are written into from_index-1 BEFORE post_process runs
    so compute_rma carries the Wilder MA correctly across the boundary.

_apply_result() is shared: full load replaces the buffer, incremental appends.
"""

import math
import threading
from datetime import datetime

import numpy as np

from data_processor import (
    CACHE_FIELDS,
    VOL_CUMUL, LTP_F, VOL, VOL_SLOW, VOL_FAST, VOL_BASE, NFIELDS,
    get_one_day_intervals, get_index_from_dtobj,
    discover_files, read_csv_files_to_arrays,
    fill_gaps_numpy, post_process,
    build_timestamps, build_symbols_avg, numpy_to_cache_rows,
)

MIN_TF  = '3'
TF_KEYS = (MIN_TF, '15', 'D')
_BUFFER_DAYS = 1


# ── lazy proxy ────────────────────────────────────────────────────────────────

class SymbolsDataProxy:
    __slots__ = ('_sym_list', '_num_data', '_write_ptr',
                 '_ts_list', '_tsf_list', '_sym_to_idx', '_row_cache')

    def __init__(self, sym_list, num_data, write_ptr, ts_list, tsf_list):
        self._sym_list   = sym_list
        self._num_data   = num_data
        self._write_ptr  = write_ptr
        self._ts_list    = ts_list
        self._tsf_list   = tsf_list
        self._sym_to_idx = {s: i for i, s in enumerate(sym_list)}
        self._row_cache  = {}

    def __contains__(self, sym):
        return sym in self._sym_to_idx

    def __getitem__(self, sym):
        if sym in self._row_cache:
            return self._row_cache[sym]
        si = self._sym_to_idx.get(sym)
        if si is None:
            raise KeyError(sym)
        valid = self._num_data[si, :self._write_ptr, :]
        rows  = numpy_to_cache_rows(CACHE_FIELDS, self._ts_list, self._tsf_list, valid)
        self._row_cache[sym] = rows
        return rows

    def keys(self):
        return self._sym_to_idx.keys()

    def items(self):
        for sym in self._sym_list:
            yield sym, self[sym]


# ── CacheManager ─────────────────────────────────────────────────────────────

class CacheManager:
    def __init__(self):
        self._lock  = threading.Lock()
        self._ready = False

        self._sym_list    = {tf: []   for tf in TF_KEYS}
        self._num_data    = {tf: None for tf in TF_KEYS}
        self._write_ptr   = {tf: 0    for tf in TF_KEYS}
        self._ts_list     = {tf: []   for tf in TF_KEYS}
        self._tsf_list    = {tf: []   for tf in TF_KEYS}
        self._symbols_avg = {tf: None for tf in TF_KEYS}

        self._refresh_time    = None
        self._sorted_dates    = None
        self._committed_total = {tf: 0 for tf in TF_KEYS}

        # Last two slots of seeds saved after each commit.
        # seeds[0] = second-to-last col, seeds[1] = last col.
        # When from_index == last_committed (same-interval re-read),
        # seed from slot [0] (last_committed-1) instead of slot [1].
        self._seed_vcum     = {tf: None for tf in TF_KEYS}
        self._seed_vltp     = {tf: None for tf in TF_KEYS}
        self._seed_rma_fast = {tf: None for tf in TF_KEYS}
        self._seed_rma_slow = {tf: None for tf in TF_KEYS}
        self._seed_rma_base = {tf: None for tf in TF_KEYS}

    # ── public read API ───────────────────────────────────────────────────────

    @property
    def is_ready(self):
        return self._ready

    def get_symbols_data(self, tf):
        nd = self._num_data.get(tf)
        if nd is None:
            return {}
        return SymbolsDataProxy(
            self._sym_list[tf], nd, self._write_ptr[tf],
            self._ts_list[tf], self._tsf_list[tf])

    def get_symbols_avg(self, tf):
        return self._symbols_avg.get(tf)

    def get_refresh_time(self):
        return self._refresh_time

    # ── single public entry point ─────────────────────────────────────────────

    def load_files(self, data_dir):
        """
        Call on startup and on every periodic tick.
        Full load if cache is not yet ready; incremental otherwise.
        """
        incremental = self._ready and self._refresh_time is not None
        label = 'Reloading' if incremental else 'Loading data'
        print(f"🔄 {datetime.now().strftime('%M:%S')} : {label}...")

        result = self._load_csv_files(data_dir, incremental=incremental)

        if result is None:
            print(f"✅ {datetime.now().strftime('%M:%S')} : No updates.")
            return

        with self._lock:
            self._apply_result(result, incremental=incremental)

        ref = self._refresh_time
        print(f"✅ {datetime.now().strftime('%M:%S')} : Done. "
              f"Last: {ref.strftime('%d%m%Y-%H%M') if ref else '-'}")

    # ── shared CSV loading pipeline ───────────────────────────────────────────

    def _load_csv_files(self, data_dir, incremental):
        """
        Identical pipeline for full and incremental — only the range differs.

        Incremental correctness:
          1. Seeds written into column from_index-1 BEFORE fill_gaps_numpy →
             gives fill_gaps a left anchor so interpolation matches full load.
          2. RMA seeds written into arrays BEFORE post_process →
             compute_rma reads from_index-1 as its warm-start value, so the
             Wilder MA carries across the session boundary correctly.
        """
        tf_min, odi_min = get_one_day_intervals(MIN_TF)
        sorted_files, sorted_dates = discover_files(data_dir)
        if not sorted_files:
            return None

        last_file_dt  = sorted_files[-1]['date_obj']
        cache_refresh = self._refresh_time

        if incremental:
            if cache_refresh and last_file_dt <= cache_refresh:
                return None
            new_files = ([f for f in sorted_files if f['date_obj'] > cache_refresh]
                         if cache_refresh else sorted_files)
            if not new_files:
                return None
            
            from_index = get_index_from_dtobj(new_files[0]['date_obj'], sorted_dates, tf_min, odi_min)
            last_committed = (get_index_from_dtobj(cache_refresh, sorted_dates, tf_min, odi_min)
                              if cache_refresh else 0)
            # Corner case: same interval re-read (e.g. corrected file).
            # Seed from last_committed-1 so fill_gaps anchor is one step further back.
            self._seed_slot = 0 if from_index == last_committed else 1
            if from_index != last_committed:
                from_index    = last_committed + 1

            files_to_read = new_files
            known_symbols = self._sym_list[MIN_TF]
        else:
            from_index    = 1
            files_to_read = sorted_files
            known_symbols = None

        # ── MIN_TF (3-min) ────────────────────────────────────────────────────
        sym_list, vcum, vltp, total = read_csv_files_to_arrays(
            files_to_read, sorted_dates, tf_min, odi_min,
            from_index=from_index, known_symbols=known_symbols)

        n_syms = len(sym_list)

        slot = getattr(self, '_seed_slot', 1)

        if incremental and from_index > 1:
            # Step 1: plant vcum seed into boundary column so fill_gaps interpolates
            #         correctly rather than treating the left edge as the first point.
            
            sv_vcum = self._seed_vcum[MIN_TF]
            sv_vltp = self._seed_vltp[MIN_TF]
            if sv_vcum is not None:
                n = min(sv_vcum.shape[0], n_syms)
                vcum[:n, from_index - 1] = sv_vcum[:n, slot]
            if sv_vltp is not None:
                n = min(sv_vltp.shape[0], n_syms)
                vltp[:n, from_index - 1] = sv_vltp[:n, slot]
            

        # Identical gap-fill to full load

        fill_gaps_numpy(vcum, vltp, from_index, total, odi_min)

        sv_rf = self._seed_rma_fast[MIN_TF] if (incremental and from_index > 1) else None
        sv_rs = self._seed_rma_slow[MIN_TF] if (incremental and from_index > 1) else None
        sv_rb = self._seed_rma_base[MIN_TF] if (incremental and from_index > 1) else None
        vol, rma_fast, rma_slow, rma_base = post_process(
            vcum, from_index, total, odi_min,
            seed_rma_fast=sv_rf, seed_rma_slow=sv_rs, seed_rma_base=sv_rb, seed_slot=slot)

        ts_list, tsf_list = build_timestamps(from_index, total, sorted_dates, tf_min, odi_min)

        result = {
            MIN_TF: {
                'sym_list':     sym_list,
                'vcum':         vcum,
                'vltp':         vltp,
                'vol':          vol,
                'rma_fast':     rma_fast,
                'rma_slow':     rma_slow,
                'rma_base':     rma_base,
                'from_index':   from_index,
                'total':        total,
                'ts_list':      ts_list,
                'tsf_list':     tsf_list,
                'sorted_dates': sorted_dates,
                'refresh_time': last_file_dt,
                'odi':          odi_min,
            }
        }

        for tf_str in ('15', 'D'):
            result[tf_str] = self._load_derived_tf(
                tf_str, sym_list, vcum, vltp, total,
                sorted_dates, from_index, incremental)

        return result

    def _load_derived_tf(self, tf_str, sym_list, min_vcum, min_vltp,
                         min_total, sorted_dates, min_from_index, incremental):
        """Same fill+RMA pipeline for 15-min and Daily TFs."""
        tf, odi = get_one_day_intervals(tf_str)
        tfratio = tf // int(MIN_TF)
        n_syms  = len(sym_list)

        if incremental:
            old_total = self._committed_total.get(tf_str, 0)
            total     = math.ceil(min_total / tfratio)
            from_idx  = math.ceil(min_from_index / tfratio)

            # Corner case: same interval re-read (corrected/latest file same timestamp).
            # Overwrite old_total in-place: fill from old_total-1 anchor (slot 0),
            # write starting at old_total (n_new covers old_total..total).
            if from_idx == old_total:
                self._seed_slot = 0
                fill_from_idx   = old_total      # fill_gaps anchor includes old_total-1
                from_idx        = old_total      # overwrite old_total in buffer
            else:
                self._seed_slot = 1
                fill_from_idx   = old_total + 1
                from_idx        = old_total + 1

            if total < from_idx:
                return {'sym_list': sym_list, 'vcum': None, 'vltp': None,
                        'vol': None, 'rma_fast': None, 'rma_slow': None, 'rma_base': None,
                        'from_index': from_idx, 'total': total,
                        'ts_list': [], 'tsf_list': [], 'odi': odi}
        else:
            total    = math.ceil(min_total / tfratio)
            from_idx = 1

        # Sample last MIN_TF col of each derived window (1-indexed)
        vcum_d = np.full((n_syms, total + 1), np.nan)
        vltp_d = np.full((n_syms, total + 1), np.nan)
        for nfi in range(from_idx, total + 1):
            mi = min(nfi * tfratio, min_total)
            if mi >= min_from_index:
                vcum_d[:, nfi] = min_vcum[:n_syms, mi]
                vltp_d[:, nfi] = min_vltp[:n_syms, mi]

        # Plant vcum seed for incremental
        slot = getattr(self, '_seed_slot', 1)
        if incremental and from_idx > 1:
            sv_vcum = self._seed_vcum.get(tf_str)
            sv_vltp = self._seed_vltp.get(tf_str)
            if sv_vcum is not None:
                n = min(sv_vcum.shape[0], n_syms)
                vcum_d[:n, from_idx - 1] = sv_vcum[:n, slot]
            if sv_vltp is not None:
                n = min(sv_vltp.shape[0], n_syms)
                vltp_d[:n, from_idx - 1] = sv_vltp[:n, slot]

        _fill_from = fill_from_idx if incremental else from_idx
        fill_gaps_numpy(vcum_d, vltp_d, _fill_from, total, odi)

        sv_rf = self._seed_rma_fast.get(tf_str) if (incremental and from_idx > 1) else None
        sv_rs = self._seed_rma_slow.get(tf_str) if (incremental and from_idx > 1) else None
        sv_rb = self._seed_rma_base.get(tf_str) if (incremental and from_idx > 1) else None
        vol_d, rf_d, rs_d, rb_d = post_process(
            vcum_d, _fill_from, total, odi,
            seed_rma_fast=sv_rf, seed_rma_slow=sv_rs, seed_rma_base=sv_rb, seed_slot=slot)

        ts_list, tsf_list = build_timestamps(from_idx, total, sorted_dates, tf, odi)

        return {
            'sym_list':   sym_list,
            'vcum':       vcum_d,
            'vltp':       vltp_d,
            'vol':        vol_d,
            'rma_fast':   rf_d,
            'rma_slow':   rs_d,
            'rma_base':   rb_d,
            'from_index': from_idx,
            'total':      total,
            'ts_list':    ts_list,
            'tsf_list':   tsf_list,
            'odi':        odi,
        }

    # ── apply result (called under lock) ──────────────────────────────────────

    def _apply_result(self, result, incremental):
        """
        Write processed arrays into the cache buffer.
        Full load replaces the buffer; incremental appends new columns.
        """
        min_res = result[MIN_TF]
        self._sorted_dates = min_res['sorted_dates']
        self._refresh_time = min_res['refresh_time']

        for tf_str in TF_KEYS:
            res = result.get(tf_str)
            if res is None or res['vcum'] is None:
                continue

            sym_list   = res['sym_list']
            n_syms     = len(sym_list)
            vcum_1idx  = res['vcum']
            vltp_1idx  = res['vltp']
            vol_1idx   = res['vol']
            rf_1idx    = res['rma_fast']
            rs_1idx    = res['rma_slow']
            rb_1idx    = res['rma_base']
            from_index = res['from_index']
            total      = res['total']
            ts_list    = res['ts_list']
            tsf_list   = res['tsf_list']
            odi        = res['odi']

            n_new = total - from_index + 1
            if n_new <= 0:
                continue

            _, odi_tf = get_one_day_intervals(tf_str)

            if not incremental:
                cap        = total + _BUFFER_DAYS * odi_tf
                nd         = np.empty((n_syms, cap, NFIELDS), dtype=np.float64)
                wptr_start = 0
            else:
                nd   = self._num_data[tf_str]
                wptr = self._write_ptr[tf_str]

                # Overwrite case: from_index == committed_total means the last
                # committed slot gets updated in-place; step wptr back by 1.
                overwrite = (from_index == self._committed_total[tf_str])
                if overwrite:
                    wptr  = max(wptr - 1, 0)
                    n_new = total - from_index + 1   # recalc to include overwrite slot

                if nd is None or wptr + n_new > nd.shape[1]:
                    extra = max(n_new, _BUFFER_DAYS * odi_tf)
                    if nd is None:
                        nd = np.empty((n_syms, n_new + extra, NFIELDS), dtype=np.float64)
                    else:
                        pad = np.empty((nd.shape[0], extra, NFIELDS), dtype=np.float64)
                        nd  = np.concatenate([nd[:, :wptr, :], pad], axis=1)

                if n_syms > nd.shape[0]:
                    pad = np.zeros((n_syms - nd.shape[0], nd.shape[1], NFIELDS))
                    nd  = np.concatenate([nd, pad], axis=0)

                wptr_start = wptr

            # Copy columns from 1-indexed result arrays into 0-indexed buffer
            for c, fi in enumerate(range(from_index, total + 1)):
                nd[:n_syms, wptr_start + c, VOL_CUMUL] = vcum_1idx[:n_syms, fi]
                nd[:n_syms, wptr_start + c, LTP_F]     = vltp_1idx[:n_syms, fi]
                nd[:n_syms, wptr_start + c, VOL]       = vol_1idx[:n_syms,  fi]
                nd[:n_syms, wptr_start + c, VOL_SLOW]  = rs_1idx[:n_syms,   fi]
                nd[:n_syms, wptr_start + c, VOL_FAST]  = rf_1idx[:n_syms,   fi]
                nd[:n_syms, wptr_start + c, VOL_BASE]  = rb_1idx[:n_syms,   fi]

            new_wptr = wptr_start + n_new

            self._num_data[tf_str]        = nd
            self._sym_list[tf_str]        = sym_list
            self._write_ptr[tf_str]       = new_wptr
            self._committed_total[tf_str] = total

            if not incremental:
                self._ts_list[tf_str]  = list(ts_list)
                self._tsf_list[tf_str] = list(tsf_list)
            else:
                if overwrite:
                    # Drop the last timestamp entry that is being overwritten
                    self._ts_list[tf_str]  = self._ts_list[tf_str][:-1]
                    self._tsf_list[tf_str] = self._tsf_list[tf_str][:-1]
                self._ts_list[tf_str]  += list(ts_list)
                self._tsf_list[tf_str] += list(tsf_list)

            self._symbols_avg[tf_str] = build_symbols_avg(sym_list, nd, new_wptr - 1)

            # Save last two cols as (N, 2) arrays: col 0 = second-to-last, col 1 = last.
            # Slot 1 is the normal seed; slot 0 is used when from_index == last_committed.
            p1 = max(new_wptr - 2, 0)   # second-to-last (clamped to 0)
            p2 = new_wptr - 1            # last
            self._seed_vcum[tf_str]     = nd[:n_syms, [p1, p2], VOL_CUMUL].copy()
            self._seed_vltp[tf_str]     = nd[:n_syms, [p1, p2], LTP_F].copy()
            self._seed_rma_fast[tf_str] = nd[:n_syms, [p1, p2], VOL_FAST].copy()
            self._seed_rma_slow[tf_str] = nd[:n_syms, [p1, p2], VOL_SLOW].copy()
            self._seed_rma_base[tf_str] = nd[:n_syms, [p1, p2], VOL_BASE].copy()

        self._ready = True