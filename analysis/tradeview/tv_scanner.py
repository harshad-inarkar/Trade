#!/usr/bin/env python3
"""
TradingView Table Scanner v4 — macOS (Production)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Silent background execution. Auto-executes trades via Dhan API.
Use --debug to view scan metrics.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
from PIL import Image

# ─── Custom Imports ───────────────────────────────────────────────────────────
from utils.utility import wait_next_wall_clock
from utils.data.paths import OUT_DIR
from tradeapi.dhan_trade import DhanTrader
from utils.ocr.ocr_engine import ocr_pil as _engine_ocr_pil

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pure Utility & Notification
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def clean_symbol(sym: str) -> str:
    sym = sym.upper()
    sym = re.sub(r"1!$", "", sym)
    sym = re.sub(r"!$",  "", sym)
    return re.sub(r"[^A-Z0-9&]", "", sym)

class AlertNotifier:
    @staticmethod
    def notify(title: str, body: str):
        script = f'display notification "{body}" with title "{title}"'
        try:
            subprocess.run(["osascript", "-e", script], check=True, capture_output=True)
            subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"], capture_output=True)
        except Exception:
            pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ScannerConfig:
    """Manages coordinate configs and local 'seen' alerts state."""
    def __init__(self, config_file: Path, seen_file: Path):
        self.config_file = config_file
        self.seen_file   = seen_file
        self.data        = {}
    
    def load(self) -> bool:
        if self.config_file.exists():
            with open(self.config_file) as f:
                self.data = json.load(f)
                return True
        return False

    def save(self):
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, "w") as f:
            json.dump(self.data, f, indent=2)

    def load_seen(self) -> dict:
        if self.seen_file.exists():
            with open(self.seen_file) as f:
                return json.load(f)
        return {}

    def save_seen(self, seen_dict: dict):
        with open(self.seen_file, "w") as f:
            json.dump(seen_dict, f, indent=2)

    def clear_seen(self):
        self.seen_file.unlink(missing_ok=True)

    def setup_interactive(self) -> bool:
        """Runs the PyAutoGUI setup to capture bounding boxes."""
        try:
            import pyautogui
        except ImportError:
            print("Error: pyautogui is required for setup. Run 'pip install pyautogui'")
            return False

        print("\n╔══════════════════════════════════════════════════════╗")
        print("║   TradingView Scanner v4 — Setup (PyAutoGUI)         ║")
        print("╠══════════════════════════════════════════════════════╣")
        print("║  1. Hover TOP-LEFT and press ENTER                   ║")
        print("║  2. Hover BOTTOM-RIGHT and press ENTER               ║")
        print("║  3. Hover SIGNAL COLUMN and press ENTER              ║")
        print("╚══════════════════════════════════════════════════════╝\n")

        def get_coord(label):
            print(f"--> Hover over {label} and press ENTER...", end="", flush=True)
            input()
            x, y = pyautogui.position()
            print(f" Captured: ({x}, {y})")
            return x, y

        x1, y1 = get_coord("TOP-LEFT corner")
        x2, y2 = get_coord("BOTTOM-RIGHT corner")
        sx, _  = get_coord("SIGNAL column")

        self.data = {
            "x1": x1, "y1": y1, 
            "x2": x2, "y2": y2, 
            "signal_col_x": max(0, sx - x1)
        }
        self.save()
        return True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Trigram Index Matcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TrigramIndexMatcher:
    _DICE_CUTOFF = 0.7
    _MAX_EDIT    = 2

    def __init__(self, candidates):
        self.candidates = set(candidates)
        self.index: dict[str, set] = {}
        for cand in self.candidates:
            for tg in self._get_trigrams(cand):
                self.index.setdefault(tg, set()).add(cand)

    @staticmethod
    @lru_cache(maxsize=2048)
    def _get_trigrams(word: str) -> tuple:
        w = f"${word}^"
        return tuple(w[i : i + 3] for i in range(len(w) - 2))

    @staticmethod
    @lru_cache(maxsize=4096)
    def _levenshtein(a: str, b: str) -> int:
        if len(a) < len(b): a, b = b, a
        prev = list(range(len(b) + 1))
        for ca in a:
            curr = [prev[0] + 1]
            for j, cb in enumerate(b):
                curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
            prev = curr
        return prev[-1]

    def match(self, query: str, cutoff=_DICE_CUTOFF) -> Optional[str]:
        query = clean_symbol(query)
        if not query: return None
        if query in self.candidates: return query

        q_tgs = self._get_trigrams(query)
        if not q_tgs: return None

        scores = {}
        for tg in q_tgs:
            for cand in self.index.get(tg, ()):
                scores[cand] = scores.get(cand, 0) + 1

        best_cand, best_score = None, 0.0
        for cand, overlap in scores.items():
            c_len = len(self._get_trigrams(cand))
            score = (2.0 * overlap) / (len(q_tgs) + c_len)
            if score > best_score:
                best_score, best_cand = score, cand

        if best_score >= cutoff: return best_cand

        if scores:
            ranked = sorted(scores, key=lambda c: scores[c], reverse=True)
            lev_best_cand, lev_best_dist = None, self._MAX_EDIT + 1
            for cand in ranked:
                dist = self._levenshtein(query, cand)
                if dist < lev_best_dist:
                    lev_best_dist, lev_best_cand = dist, cand
                    if dist == 1: break
            if lev_best_dist <= self._MAX_EDIT:
                return lev_best_cand

        return None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Image Processing & Vision Engine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ScannerVision:
    def __init__(self, min_saturation=40, green_bias=25, red_bias=25, max_workers=8):
        self.min_saturation = min_saturation
        self.green_bias     = green_bias
        self.red_bias       = red_bias
        self.pool           = ThreadPoolExecutor(max_workers=max_workers)

    def find_signal_col_x(self, arr: np.ndarray) -> int:
        h, w = arr.shape[:2]
        y0, x0, x1 = int(h * 0.05), int(w * 0.10), int(w * 0.60)

        region = arr[y0::15, x0:x1:5].astype(np.int32) 
        if region.size == 0: return -1

        r, g, b = region[..., 0], region[..., 1], region[..., 2]
        sat   = region.max(axis=2) - region.min(axis=2)
        green = (sat >= self.min_saturation) & (g - r >= self.green_bias) & (g > 80)
        red   = (sat >= self.min_saturation) & (r - g >= self.red_bias)   & (r > 80)
        mask  = green | red                                       

        hits = np.argwhere(mask) 
        if hits.size == 0: return -1

        return x0 + hits[0][1] * 5

    def detect_rows(self, arr: np.ndarray, sig_x: int) -> list:
        brightness = arr[:, sig_x, :].max(axis=1) > 80 
        padded     = np.concatenate([[False], brightness, [False]])
        starts     = np.where(~padded[:-1] &  padded[1:])[0]
        ends       = np.where( padded[:-1] & ~padded[1:])[0]
        return [(int(s), int(e)) for s, e in zip(starts, ends) if e - s >= 10]

    def classify_signal(self, arr: np.ndarray, row: tuple, sig_x: int) -> str:
        y0, y1 = row
        w = arr.shape[1]
        x_lo, x_hi = max(0, sig_x - 12), min(w - 1, sig_x + 12)

        region = arr[y0 + 2 : y1 - 2 : max(1, (y1 - y0) // 6), x_lo : x_hi : max(1, (x_hi - x_lo) // 6)].astype(np.float32)
        if region.size == 0: return ""

        avg = region.mean(axis=(0, 1))
        r, g, b = float(avg[0]), float(avg[1]), float(avg[2])

        if max(r, g, b) - min(r, g, b) < self.min_saturation: return ""
        if g - r >= self.green_bias and g > 80: return "BUY"
        if r - g >= self.red_bias and r > 80: return "SELL"
        return ""

    def _ocr_strip(self, img: Image.Image, y0: int, y1: int, x0: int, x1: int, whitelist: str = "") -> str:
        if x1 <= x0 or y1 <= y0: return ""
        cropped = img.crop((x0, y0, x1, y1))
        return _engine_ocr_pil(cropped, whitelist=whitelist).strip()

    def extract_row_data(self, img: Image.Image, arr: np.ndarray, row: tuple, sig_x: int) -> dict:
        y0, y1 = row
        w = img.size[0]
        y_mid = y0 + (y1 - y0) // 2

        left_bright = arr[y_mid, : sig_x + 1, :].max(axis=1)
        col_sig_start = int(np.where(left_bright < 50)[0][-1]) if np.where(left_bright < 50)[0].size else 0

        right_bright = arr[y_mid, sig_x :, :].max(axis=1)
        col_sig_end  = sig_x + int(np.where(right_bright < 50)[0][0]) if np.where(right_bright < 50)[0].size else w - 1

        sym_raw = self._ocr_strip(img, y0, y1, 0, max(2, col_sig_start - 2), whitelist="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!&")
        symbol  = clean_symbol(sym_raw.upper().replace("T1!", "1!").replace("OH", "O1!").replace("EO", "CO"))

        e_start = col_sig_end + 2
        search_limit = min(w - 1, e_start + int(w * 0.35))
        col_max = arr[y0 + 2 : y1 - 2, e_start : search_limit].max(axis=(0, 2)) if e_start < search_limit else np.array([])

        in_text, gap_count, e_end = False, 0, search_limit
        for i, mx in enumerate(col_max):
            if mx > 120:
                in_text, gap_count = True, 0
            elif in_text:
                gap_count += 1
                if gap_count >= 8:
                    e_end = e_start + i - gap_count + 4
                    break

        ent_raw = self._ocr_strip(img, y0, y1, e_start, e_end, whitelist="0123456789.")
        entry_match = re.search(r"\d+(\.\d+)?", ent_raw)
        
        return {"symbol": symbol, "entry": entry_match.group(0) if entry_match else ""}

    def process_row(self, img: Image.Image, arr: np.ndarray, row: tuple, sig_x: int) -> Optional[dict]:
        signal = self.classify_signal(arr, row, sig_x)
        if not signal: return None
        
        data = self.extract_row_data(img, arr, row, sig_x)
        if not data["symbol"]: return None
        
        data["signal"] = signal
        return data

    def process_image(self, img_path: str) -> list[dict]:
        """Main pipeline to extract all signals from an image."""
        img = Image.open(img_path).convert("RGB")
        arr = np.array(img, dtype=np.uint8)

        dynamic_sig_x = self.find_signal_col_x(arr)
        if dynamic_sig_x == -1:
            return []

        rows = self.detect_rows(arr, dynamic_sig_x)
        
        # Multithreaded row parsing
        futures = {self.pool.submit(self.process_row, img, arr, row, dynamic_sig_x): row for row in rows}
        
        results_by_row = {}
        for fut in as_completed(futures):
            row = futures[fut]
            result = fut.result()
            if result is not None:
                results_by_row[row] = result

        # Return parsed rows in original order
        return [results_by_row[row] for row in rows if row in results_by_row]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Trade Execution Wrapper
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TradeExecutor:
    """Orchestrates API resolution and Dhan orders."""
    def __init__(self, exchange_map: dict, no_trade: bool, rebuild_master: bool):
        self.exchange_map = exchange_map
        self.no_trade     = no_trade
        self.broker       = DhanTrader(refresh_master_scrip=rebuild_master) if not self.no_trade else None

    def get_exchange(self, symb: str) -> str:
        if symb in self.exchange_map['NSE']: return 'NSE'
        if symb in self.exchange_map['COMM']: return 'MCX'
        return ''

    def place_orders(self, orders_list: list):
        if not orders_list or self.no_trade or not self.broker:
            return

        self.broker.begin_session()
        for order in orders_list:
            symb = order['symbol']
            exch = self.get_exchange(symb)
            if not exch:
                continue
                
            self.broker.fire_trade(
                symb      = symb,
                exch      = exch,
                signal    = order['signal'],
                entry_val = float(order.get('entry', 0)),
            )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main Application Controller
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TVScannerApp:
    def __init__(self, args):
        self.debug            = args.debug
        self.ignore_lastseen  = args.ignore_lastseen
        self.no_trade         = args.no_trade
        self.rebuild_master   = args.rebuild_master_scrip
        self.reload_interval  = args.reload_interval
        self.new_setup        = args.new_setup
        self.buffer_seconds   = 7
        
        # Paths
        self.config_file = Path(__file__).parent / ".tv_scanner_config.json"
        self.seen_file   = Path(__file__).parent / ".tv_scanner_seen.json"
        self.cand_paths  = [
            os.path.join(OUT_DIR, "candidates.txt"),
            os.path.join(OUT_DIR, "candidates_comm.txt"),
            os.path.join(OUT_DIR, "candidates_cryp.txt")
        ]

        # Core Components
        self.config   = ScannerConfig(self.config_file, self.seen_file)
        self.vision   = ScannerVision()
        self.matcher  = None
        self.executor = None

    def dprint(self, *args, **kwargs):
        if self.debug:
            print(*args, **kwargs)

    def load_candidates(self) -> tuple[set, list]:
        candidates: set = set()
        cands_list = []
        for filepath in self.cand_paths:
            cur_candset = set()
            try:
                with open(filepath) as f:
                    for line in f:
                        sym = line.strip()
                        if sym:
                            candidates.add(sym)
                            cur_candset.add(sym)
            except FileNotFoundError:
                pass
            cands_list.append(cur_candset)
        return candidates, cands_list

    def capture_screen(self) -> str:
        """Takes screencapture based on saved coordinates."""
        cfg = self.config.data
        tmp = tempfile.mkstemp(suffix=".png")[1]
        subprocess.run(
            ["screencapture", "-x", "-R",
             f"{cfg['x1']},{cfg['y1']},{cfg['x2']-cfg['x1']},{cfg['y2']-cfg['y1']}", tmp],
            check=True,
        )
        return tmp

    def focus_tradingview(self):
        """Ensures TradingView is active and at the forefront."""
        if not os.popen("pgrep -x 'TradingView'").read():
            print("TradingView app closed. Exiting script.")
            sys.exit()

        focus_script = """
            tell application "System Events"
                tell process "TradingView"
                    set targetWindow to window 1
                    perform action "AXRaise" of targetWindow
                    set value of attribute "AXMain" of targetWindow to true
                end tell
            end tell
        """
        try:
            subprocess.run(["osascript", "-e", focus_script], check=True, capture_output=True)
        except Exception as e:
            self.dprint(f"Focus Error: {e}")
            os.system("osascript -e 'tell application \"TradingView\" to activate'")

    def process_scan(self):
        t0 = time.time()
        self.dprint(f"\n── Scanning ──────────────────────────────────────")

        self.focus_tradingview()

        try:
            img_path = self.capture_screen()
        except RuntimeError as e:
            self.dprint(f"  screencapture failed: {e}")
            return

        try:
            parsed = self.vision.process_image(img_path)
            if not parsed:
                self.dprint("No Alerts")
                return

            self.dprint(f"  Rows detected : {len(parsed)}")
            self.dprint(f"\n  ── Candidate Matching ─────────────────────────────")
            
            valid_parsed: list[dict] = []
            for row in parsed:
                sym = row["symbol"]
                best_match = self.matcher.match(sym)
                icon = "🟢" if row["signal"] == "BUY" else "🔴"

                if best_match == sym:
                    valid_parsed.append(row)
                    self.dprint(f"  {icon} {sym:<14}  {row['signal']:<4}  entry={row['entry']:<10} [Exact]")
                elif best_match:
                    row["symbol"] = best_match
                    valid_parsed.append(row)
                    self.dprint(f"  {icon} {sym:<14}  {row['signal']:<4}  entry={row['entry']:<10} [Fixed: {best_match}]")
                else:
                    self.dprint(f"  ❌ {sym:<14}  {row['signal']:<4}  entry={row['entry']:<10} [No Match]")

            if not valid_parsed:
                self.dprint("  No BUY/SELL rows matched candidates.")
                return

            t1 = time.time()
            self.dprint(f"Scan completed in {t1 - t0:.2f} seconds")

            # ── Deduplication Logic ──
            seen = self.config.load_seen()
            new_alerts = []
            
            for row in valid_parsed:
                sym = row["symbol"]
                sig = row["signal"]
                
                if not self.ignore_lastseen:
                    if seen.get(sym) != sig:
                        new_alerts.append(row)
                        seen[sym] = sig 
                else:
                    new_alerts.append(row)

            self.config.save_seen(seen)

            if not new_alerts:
                self.dprint("  No new alerts.")
                return

            # Execute & Notify
            self.executor.place_orders(new_alerts)

            buys  = [r for r in new_alerts if r["signal"] == "BUY"]
            sells = [r for r in new_alerts if r["signal"] == "SELL"]
            parts = []
            if buys:  parts.append(f"{len(buys)}x BUY")
            if sells: parts.append(f"{len(sells)}x SELL")

            title = f"TV Alert {datetime.now().strftime('%H:%M')}  •  {', '.join(parts)}"
            lines = [f"{r['symbol']:<10} {r['signal']:<4}  {r['entry']}" for r in new_alerts]

            AlertNotifier.notify(title, "\n".join(lines))
            self.dprint(f"\n  Alert fired! Title: {title}")

        finally:
            try: os.remove(img_path)
            except OSError: pass

    def run(self):
        # Initial Setup/Config checks
        if self.new_setup:
            if not self.config.setup_interactive():
                sys.exit(1)
        elif not self.config.load():
            print("No config — launching setup…\n")
            if not self.config.setup_interactive():
                sys.exit(1)

        # Build Lookups
        candidates, cands_list = self.load_candidates()
        
        exchange_map = {
            'NSE':    cands_list[0] if len(cands_list) > 0 else set(),
            'COMM':   cands_list[1] if len(cands_list) > 1 else set(),
            'CRYPTO': cands_list[2] if len(cands_list) > 2 else set(),
        }

        self.matcher  = TrigramIndexMatcher(candidates)
        self.executor = TradeExecutor(exchange_map, self.no_trade, self.rebuild_master)
        self.config.clear_seen()

        if self.debug:
            self.dprint(f"\n  TradingView Scanner v4 started (Production)")
            self.dprint(f"  Interval  : {self.reload_interval} min (+{self.buffer_seconds}s buffer)")
            self.dprint(f"  Index     : Built {len(candidates)} candidates")
            self.dprint(f"  Ctrl+C to stop\n")
        elif not self.new_setup:
            print("TradingView Scanner running in background. (Use --debug to view scan logs)")

        print(f"DEBUG_MODE: {self.debug}, IGNORE_LASTSEEN: {self.ignore_lastseen}, NO_TRADE: {self.no_trade}")

        # Scan Loop
        self.process_scan()
        while True:
            wait_next_wall_clock(self.reload_interval, self.buffer_seconds)
            self.process_scan()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Execution entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    p = argparse.ArgumentParser()
    p.add_argument("-ri", "--reload-interval", type=int, default=1)
    p.add_argument("-ns", "--new-setup", action="store_true")
    p.add_argument("-d", "--debug", action="store_true", help="Print scan logs to terminal")
    p.add_argument("-il", "--ignore-lastseen", action="store_true", help="Ignore Last Seen")
    p.add_argument("-nt", "--no-trade", action="store_true", help="Don't Place Orders")
    p.add_argument("-rm", "--rebuild-master-scrip", action="store_true", help="Rebuild Master Scrip")

    args = p.parse_args()
    
    app = TVScannerApp(args)
    app.run()

if __name__ == "__main__":
    try: 
        main()
    except KeyboardInterrupt: 
        sys.exit(0)