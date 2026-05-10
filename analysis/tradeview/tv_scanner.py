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

import numpy as np
from PIL import Image

# ─── Custom Imports ───────────────────────────────────────────────────────────
from utils.utility import wait_next_wall_clock
from utils.data.paths import OUT_DIR
# IMPORT THE NEW CLASS INSTEAD OF THE FREE FUNCTIONS
from tradeapi.dhan_trade import DhanTrader
from utils.ocr.ocr_engine import ocr_pil as _engine_ocr_pil

cand_nse_file_path  = os.path.join(OUT_DIR, "candidates.txt")
cand_comm_file_path = os.path.join(OUT_DIR, "candidates_comm.txt")
cand_cryp_file_path = os.path.join(OUT_DIR, "candidates_cryp.txt")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Trade Execution
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TradeExecutor:
    """
    Owns everything needed to place orders for one scan session:
      - which symbols belong to which exchange
      - the DhanTrader API instance
      - per-trade amount / dry-run flag
    """
    def __init__(
        self,
        exchange_map: dict,
        no_trade: bool = False,
    ):
        self.exchange_map = exchange_map   # {'NSE': set, 'COMM': set, 'CRYPTO': set}
        self.no_trade     = no_trade
        
        # Instantiate the Object-Oriented Broker
        self.broker = DhanTrader() if not self.no_trade else None

    # ── Exchange resolution ──────────────────────────────────────────────────
    def get_exchange(self, symb: str) -> str:
        """Return 'NSE', 'MCX', or '' based on which candidate set owns the symbol."""
        if symb in self.exchange_map['NSE']:
            return 'NSE'
        if symb in self.exchange_map['COMM']:
            return 'MCX'
        return ''   # CRYPTO or unknown — not tradeable via Dhan

    # ── Order placement ──────────────────────────────────────────────────────
    def place_orders(self, orders_list: list):
        """Fire Dhan orders for all signals in orders_list."""
        if not orders_list or self.no_trade or not self.broker:
            return

        # Tell the broker instance a new minute/cycle has started
        self.broker.begin_session()
        
        for order in orders_list:
            symb = order['symbol']
            exch = self.get_exchange(symb)
            if not exch:
                continue
                
            # Fire the trade on the instance rather than using global functions
            self.broker.fire_trade(
                symb      = symb,
                exch      = exch,
                signal    = order['signal'],
                entry_val = float(order.get('entry', 0)),
            )

# ─── Config & Constants ───────────────────────────────────────────────────────
CONFIG_FILE      = Path(__file__).parent /  ".tv_scanner_config.json"
SEEN_ALERTS_FILE =  Path(__file__).parent / ".tv_scanner_seen.json"
BUFFER_SECONDS   = 7

MIN_SATURATION = 40
GREEN_BIAS     = 25
RED_BIAS       = 25

DEBUG_MODE = False
IGNORE_LASTSEEN = False


def dprint(*args, **kwargs):
    if DEBUG_MODE:
        print(*args, **kwargs)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Auto-Detect Signal Column
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def find_signal_col_x(img, arr: np.ndarray) -> int:
    h, w = arr.shape[:2]
    y0, x0, x1 = int(h * 0.05), int(w * 0.10), int(w * 0.60)

    region = arr[y0::15, x0:x1:5].astype(np.int32) 
    if region.size == 0: return -1

    r, g, b = region[..., 0], region[..., 1], region[..., 2]
    sat   = region.max(axis=2) - region.min(axis=2)
    green = (sat >= MIN_SATURATION) & (g - r >= GREEN_BIAS) & (g > 80)
    red   = (sat >= MIN_SATURATION) & (r - g >= RED_BIAS)   & (r > 80)
    mask  = green | red                                       

    hits = np.argwhere(mask) 
    if hits.size == 0: return -1

    return x0 + hits[0][1] * 5


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cleaning & Search Logic
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def clean_symbol(sym: str) -> str:
    sym = sym.upper()
    sym = re.sub(r"1!$", "", sym)
    sym = re.sub(r"!$",  "", sym)
    return re.sub(r"[^A-Z0-9&]", "", sym)

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

    def match(self, query, cutoff=_DICE_CUTOFF):
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

def load_candidates(filepath_list: list) -> tuple:
    candidates: set = set()
    cands_list = []
    
    for filepath in filepath_list:
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core Utils
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def notify(title: str, body: str):
    script = f'display notification "{body}" with title "{title}"'
    try:
        subprocess.run(["osascript", "-e", script], check=True, capture_output=True)
        subprocess.run(["afplay", "/System/Library/Sounds/Glass.aiff"], capture_output=True)
    except Exception:
        pass

def save_config(cfg: dict):
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f: return json.load(f)
    return None

def run_setup() -> dict:
    import pyautogui
    print("\n╔══════════════════════════════════════════════════════╗")
    print("║   TradingView Scanner v3 — Setup (PyAutoGUI)         ║")
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

    cfg = {"x1": x1, "y1": y1, "x2": x2, "y2": y2, "signal_col_x": max(0, sx - x1)}
    save_config(cfg)
    return cfg

def capture_region(cfg: dict) -> str:
    tmp = tempfile.mkstemp(suffix=".png")[1]
    subprocess.run(
        ["screencapture", "-x", "-R",
         f"{cfg['x1']},{cfg['y1']},{cfg['x2']-cfg['x1']},{cfg['y2']-cfg['y1']}", tmp],
        check=True,
    )
    return tmp

def load_seen() -> dict:
    if SEEN_ALERTS_FILE.exists():
        with open(SEEN_ALERTS_FILE) as f: return json.load(f)
    return {}

def save_seen(d: dict):
    with open(SEEN_ALERTS_FILE, "w") as f: json.dump(d, f, indent=2)




# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Dynamic Detection & OCR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def detect_rows(arr: np.ndarray, sig_x: int) -> list:
    brightness = arr[:, sig_x, :].max(axis=1) > 80 
    padded     = np.concatenate([[False], brightness, [False]])
    starts     = np.where(~padded[:-1] &  padded[1:])[0]
    ends       = np.where( padded[:-1] & ~padded[1:])[0]
    return [(int(s), int(e)) for s, e in zip(starts, ends) if e - s >= 10]

def classify_signal(arr: np.ndarray, row: tuple, sig_x: int) -> str:
    y0, y1 = row
    w = arr.shape[1]
    x_lo, x_hi = max(0, sig_x - 12), min(w - 1, sig_x + 12)

    region = arr[y0 + 2 : y1 - 2 : max(1, (y1 - y0) // 6), x_lo : x_hi : max(1, (x_hi - x_lo) // 6)].astype(np.float32)
    if region.size == 0: return ""

    avg = region.mean(axis=(0, 1))
    r, g, b = float(avg[0]), float(avg[1]), float(avg[2])

    if max(r, g, b) - min(r, g, b) < MIN_SATURATION: return ""
    if g - r >= GREEN_BIAS and g > 80: return "BUY"
    if r - g >= RED_BIAS and r > 80: return "SELL"
    return ""

def _ocr_strip(img: Image.Image, y0: int, y1: int, x0: int, x1: int, whitelist: str = "") -> str:
    if x1 <= x0 or y1 <= y0: return ""
    cropped = img.crop((x0, y0, x1, y1))

    return _engine_ocr_pil(cropped, whitelist=whitelist).strip()

def extract_row_data(img: Image.Image, arr: np.ndarray, row: tuple, cfg: dict) -> dict:
    y0, y1 = row
    w = img.size[0]

    y_mid = y0 + (y1 - y0) // 2
    sig_x = cfg.get("temp_sig_x", int(w * 0.28))

    left_bright = arr[y_mid, : sig_x + 1, :].max(axis=1)
    col_sig_start = int(np.where(left_bright < 50)[0][-1]) if np.where(left_bright < 50)[0].size else 0

    right_bright = arr[y_mid, sig_x :, :].max(axis=1)
    col_sig_end  = sig_x + int(np.where(right_bright < 50)[0][0]) if np.where(right_bright < 50)[0].size else w - 1

    sym_raw = _ocr_strip(img, y0, y1, 0, max(2, col_sig_start - 2), whitelist="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!&")
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

    ent_raw = _ocr_strip(img, y0, y1, e_start, e_end, whitelist="0123456789.")
    entry_match = re.search(r"\d+(\.\d+)?", ent_raw)
    
    return {"symbol": symbol, "entry": entry_match.group(0) if entry_match else ""}

def _process_row(img: Image.Image, arr: np.ndarray, row: tuple, cfg: dict) -> dict | None:
    signal = classify_signal(arr, row, cfg["temp_sig_x"])
    if not signal: return None
    data = extract_row_data(img, arr, row, cfg)
    if not data["symbol"]: return None
    data["signal"] = signal
    return data


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core scan
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_MAX_WORKERS = 8
_ROW_POOL    = ThreadPoolExecutor(max_workers=_MAX_WORKERS)

def scan(cfg: dict, matcher: TrigramIndexMatcher, executor: TradeExecutor):
    # 1. TradingView Application Checks & Focus
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
        subprocess.run(["osascript", "-e", focus_script], check=True)
    except Exception as e:
        dprint(f"Focus Error: {e}")
        os.system("osascript -e 'tell application \"TradingView\" to activate'")

    _scan(cfg, matcher, executor)


def _scan(cfg: dict, matcher: TrigramIndexMatcher, executor: TradeExecutor):
    t0 = time.time()
    dprint(f"\n── Scanning ──────────────────────────────────────")

    try: img_path = capture_region(cfg)
    except RuntimeError as e:
        dprint(f"  screencapture failed: {e}")
        return

    img = Image.open(img_path).convert("RGB")
    arr = np.array(img, dtype=np.uint8)

    dynamic_sig_x = find_signal_col_x(img, arr)
    if dynamic_sig_x == -1:
        dprint("No Alerts")
        try: os.remove(img_path)
        except OSError: pass
        return

    cfg["temp_sig_x"] = dynamic_sig_x
    rows = detect_rows(arr, dynamic_sig_x)
    dprint(f"  Rows detected : {len(rows)}")

    parsed: list[dict] = []
    futures = {_ROW_POOL.submit(_process_row, img, arr, row, cfg): row for row in rows}

    results_by_row = {}
    for fut in as_completed(futures):
        row = futures[fut]
        result = fut.result()
        if result is not None:
            results_by_row[row] = result

    for row in rows:
        if row in results_by_row:
            parsed.append(results_by_row[row])

    try: os.remove(img_path)
    except OSError: pass

    dprint(f"\n  ── Candidate Matching ─────────────────────────────")
    valid_parsed: list[dict] = []
    for row in parsed:
        sym = row["symbol"]
        best_match = matcher.match(sym)
        icon = "🟢" if row["signal"] == "BUY" else "🔴"

        if best_match == sym:
            valid_parsed.append(row)
            dprint(f"  {icon} {sym:<14}  {row['signal']:<4}  entry={row['entry']:<10} [Exact]")
        elif best_match:
            row["symbol"] = best_match
            valid_parsed.append(row)
            dprint(f"  {icon} {sym:<14}  {row['signal']:<4}  entry={row['entry']:<10} [Fixed: {best_match}]")
        else:
            dprint(f"  ❌ {sym:<14}  {row['signal']:<4}  entry={row['entry']:<10} [No Match]")

    parsed = valid_parsed
    if not parsed:
        dprint("  No BUY/SELL rows matched candidates.")
        return

    t1 = time.time()
    dprint(f"Scan completed in {t1 - t0:.2f} seconds")

    # ── Deduplication Logic (Fixed) ──
    seen = load_seen()
    new_alerts = []
    
    for row in parsed:
        sym = row["symbol"]
        sig = row["signal"]
        
        if not IGNORE_LASTSEEN:
            if seen.get(sym) != sig:
                new_alerts.append(row)
                seen[sym] = sig # Update existing seen dictionary instead of wiping it
        else:
            new_alerts.append(row)

    save_seen(seen)

    if not new_alerts:
        dprint("  No new alerts.")
        return

    # Execute trades via API (no_trade guard is inside executor)
    executor.place_orders(new_alerts)

    buys  = [r for r in new_alerts if r["signal"] == "BUY"]
    sells = [r for r in new_alerts if r["signal"] == "SELL"]
    parts = []
    if buys:  parts.append(f"{len(buys)}x BUY")
    if sells: parts.append(f"{len(sells)}x SELL")

    title = f"TV Alert {datetime.now().strftime('%H:%M')}  •  {', '.join(parts)}"
    lines = [f"{r['symbol']:<10} {r['signal']:<4}  {r['entry']}" for r in new_alerts]

    notify(title, "\n".join(lines))
    dprint(f"\n  Alert fired! Title: {title}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("-ri", "--reload-interval", type=int, default=1)
    p.add_argument("-ns", "--new-setup", action="store_true")
    p.add_argument("-d", "--debug", action="store_true", help="Print scan logs to terminal")
    p.add_argument("-il", "--ignore-lastseen", action="store_true",help="Ignore Last Seen")
    p.add_argument("-nt", "--no-trade", action="store_true",help="Don't Place Orders")

    return p.parse_args()


def main():
    global DEBUG_MODE, IGNORE_LASTSEEN 

    args = parse_args()

    DEBUG_MODE      = args.debug
    IGNORE_LASTSEEN = args.ignore_lastseen

    if args.new_setup:
        cfg = run_setup()
        if not cfg: sys.exit(1)
    else:
        cfg = load_config()
        if not cfg:
            print("No config — launching setup…\n")
            cfg = run_setup()
            if not cfg: sys.exit(1)

    # Load all candidate files
    candidates, cands_list = load_candidates([cand_nse_file_path, cand_comm_file_path, cand_cryp_file_path])

    exchange_map = {
        'NSE':    cands_list[0] if len(cands_list) > 0 else set(),
        'COMM':   cands_list[1] if len(cands_list) > 1 else set(),
        'CRYPTO': cands_list[2] if len(cands_list) > 2 else set(),
    }

    executor = TradeExecutor(
        exchange_map  = exchange_map,
        no_trade      = args.no_trade,
    )

    matcher  = TrigramIndexMatcher(candidates)
    interval = args.reload_interval

    if DEBUG_MODE:
        dprint(f"\n  TradingView Scanner v4 started (Production)")
        dprint(f"  Interval  : {interval} min (+{BUFFER_SECONDS}s buffer)")
        dprint(f"  Index     : Built {len(candidates)} candidates")
        dprint(f"  Ctrl+C to stop\n")
    else:
        if not args.new_setup:
            print("TradingView Scanner running in background. (Use --debug to view scan logs)")

    SEEN_ALERTS_FILE.unlink(missing_ok=True)

    print(f"DEBUG_MODE: {DEBUG_MODE},"
          f"IGNORE_LASTSEEN: {IGNORE_LASTSEEN}, NO_TRADE: {executor.no_trade}")

    scan(cfg, matcher, executor)

    while True:
        wait_next_wall_clock(interval, BUFFER_SECONDS)
        scan(cfg, matcher, executor)

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: sys.exit(0)