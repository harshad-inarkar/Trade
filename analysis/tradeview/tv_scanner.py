"""
TradingView Table Scanner v4 — macOS (Production)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Silent background execution. Auto-executes trades via Dhan API.
Driven by tv_scanner_config.toml.
"""

import argparse
import contextlib
import json
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pyautogui
import tomllib
from PIL import Image

# ─── Custom Imports ───────────────────────────────────────────────────────────
from tradeapi.dhan_trade import DhanTrader
from utils.data.paths import OUT_DIR
from utils.ocr.ocr_engine import ocr_pil as _engine_ocr_pil
from utils.utility import wait_next_wall_clock

# ─────────────────────────────────────────────────────────────────────────────
# macOS Background App Registration
# ─────────────────────────────────────────────────────────────────────────────
if sys.platform == "darwin":
    with contextlib.suppress(ImportError):
        import AppKit

        # Initialize the shared application instance
        app = AppKit.NSApplication.sharedApplication()

        # Set activation policy to 'Accessory' (1)
        # This acts exactly like LSUIElement=1 (Hides Dock icon, runs in background)
        app.setActivationPolicy_(AppKit.NSApplicationActivationPolicyAccessory)

try:
    import Quartz

    QUARTZ_AVAILABLE = True
except ImportError:
    QUARTZ_AVAILABLE = False


import pytz

india_tz = pytz.timezone("Asia/Kolkata")


# Safety mechanism
pyautogui.FAILSAFE = True

# ─── Constants ────────────────────────────────────────────────────────────────
_SAT_THRESHOLD = 80
_MIN_ROW_GAP = 10
_BRIGHTNESS_THRESH = 50
_COLOR_MAX_THRESH = 120
_MAX_GAP_COUNT = 12

_NSE_IDX = 0
_COMM_IDX = 1
_CRYPTO_IDX = 2


def out(msg: str = "", end: str = "\n", *, flush: bool = False) -> None:
    """Helper to output messages to stdout without triggering T201."""
    sys.stdout.write(f"{msg}{end}")
    if flush:
        sys.stdout.flush()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pure Utility & Notification
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def clean_symbol(sym: str) -> str:
    sym = sym.upper()
    sym = re.sub(r"1!$", "", sym)
    sym = re.sub(r"!$", "", sym)
    return re.sub(r"[^A-Z0-9&]", "", sym)


class AlertNotifier:
    @staticmethod
    def notify(title: str, body: str) -> None:
        script = f'display notification "{body}" with title "{title}"'
        with contextlib.suppress(subprocess.SubprocessError, OSError):
            subprocess.run(
                ["osascript", "-e", script], check=False, capture_output=True
            )
            subprocess.run(
                ["afplay", "/System/Library/Sounds/Glass.aiff"],
                check=False,
                capture_output=True,
            )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ScannerConfig:
    """Manages TOML settings, coordinate saving, and local 'seen' alerts state."""

    def __init__(self, config_file: Path, seen_file: Path) -> None:
        self.config_file = config_file
        self.seen_file = seen_file
        self.data = self._load_toml()

    def _load_toml(self) -> dict:
        if self.config_file.exists():
            try:
                with self.config_file.open("rb") as f:
                    return tomllib.load(f)
            except (OSError, tomllib.TOMLDecodeError) as e:
                out(f"Error reading {self.config_file}: {e}")
        return {}

    def _write_toml(self) -> None:
        """Minimal manual TOML writer to preserve structure."""
        lines = []
        for section, content in self.data.items():
            lines.append(f"[{section}]")
            for k, v in content.items():
                if isinstance(v, bool):
                    lines.append(f"{k} = {'true' if v else 'false'}")
                elif isinstance(v, str):
                    lines.append(f'{k} = "{v}"')
                else:
                    lines.append(f"{k} = {v}")
            lines.append("")

        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        with self.config_file.open("w") as f:
            f.write("\n".join(lines))

    def save_coordinates(self, x1: int, y1: int, x2: int, y2: int, sig_x: int) -> None:
        self.data.setdefault("settings", {})
        self.data.setdefault("coordinates", {})

        self.data["coordinates"]["x1"] = x1
        self.data["coordinates"]["y1"] = y1
        self.data["coordinates"]["x2"] = x2
        self.data["coordinates"]["y2"] = y2
        self.data["coordinates"]["signal_col_x"] = sig_x

        self._write_toml()

    def has_coordinates(self) -> bool:
        coords = self.data.get("coordinates", {})
        return all(k in coords for k in ("x1", "y1", "x2", "y2", "signal_col_x"))

    def load_seen(self) -> dict:
        if self.seen_file.exists():
            with self.seen_file.open() as f:
                return json.load(f)
        return {}

    def save_seen(self, seen_dict: dict) -> None:
        with self.seen_file.open("w") as f:
            json.dump(seen_dict, f, indent=2)

    def clear_seen(self) -> None:
        self.seen_file.unlink(missing_ok=True)

    def setup_interactive(self) -> bool:
        """Runs the PyAutoGUI setup to capture bounding boxes."""
        out("\n╔══════════════════════════════════════════════════════╗")
        out("║   TradingView Scanner v4 — Setup (PyAutoGUI)         ║")
        out("╠══════════════════════════════════════════════════════╣")
        out("║  1. Hover TOP-LEFT and press ENTER                   ║")
        out("║  2. Hover BOTTOM-RIGHT and press ENTER               ║")
        out("║  3. Hover SIGNAL COLUMN and press ENTER              ║")
        out("╚══════════════════════════════════════════════════════╝\n")

        def get_coord(label: str) -> tuple[int, int]:
            out(f"--> Hover over {label} and press ENTER...", end="", flush=True)
            input()
            x, y = pyautogui.position()
            out(f" Captured: ({x}, {y})")
            return x, y

        x1, y1 = get_coord("TOP-LEFT corner")
        x2, y2 = get_coord("BOTTOM-RIGHT corner")
        sx, _ = get_coord("SIGNAL column")

        sig_x = max(0, sx - x1)
        self.save_coordinates(x1, y1, x2, y2, sig_x)
        out(f"\nCoordinates saved successfully to {self.config_file.name}")
        return True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Trigram Index Matcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TrigramIndexMatcher:
    _DICE_CUTOFF = 0.7
    _MAX_EDIT = 2

    def __init__(self, candidates: list[str] | set[str]) -> None:
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
        if len(a) < len(b):
            a, b = b, a
        prev = list(range(len(b) + 1))
        for ca in a:
            curr = [prev[0] + 1]
            for j, cb in enumerate(b):
                curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
            prev = curr
        return prev[-1]

    def match(self, query: str, cutoff: float = _DICE_CUTOFF) -> str | None:
        query = clean_symbol(query)
        if not query:
            return None
        if query in self.candidates:
            return query

        q_tgs = self._get_trigrams(query)
        if not q_tgs:
            return None

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

        if best_score >= cutoff:
            return best_cand

        if scores:
            ranked = sorted(scores, key=lambda c: scores[c], reverse=True)
            lev_best_cand, lev_best_dist = None, self._MAX_EDIT + 1
            for cand in ranked:
                dist = self._levenshtein(query, cand)
                if dist < lev_best_dist:
                    lev_best_dist, lev_best_cand = dist, cand
                    if dist == 1:
                        break
            if lev_best_dist <= self._MAX_EDIT:
                return lev_best_cand

        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Image Processing & Vision Engine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ScannerVision:
    def __init__(
        self,
        min_saturation: int = 40,
        green_bias: int = 25,
        red_bias: int = 25,
        max_workers: int = 8,
    ) -> None:
        self.min_saturation = min_saturation
        self.green_bias = green_bias
        self.red_bias = red_bias
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def find_signal_col_x(self, arr: np.ndarray) -> int:
        h, w = arr.shape[:2]
        y0, x0, x1 = int(h * 0.05), int(w * 0.10), int(w * 0.60)

        region = arr[y0::15, x0:x1:5].astype(np.int32)
        if region.size == 0:
            return -1

        r, g, _ = region[..., 0], region[..., 1], region[..., 2]
        sat = region.max(axis=2) - region.min(axis=2)
        green = (
            (sat >= self.min_saturation)
            & (g - r >= self.green_bias)
            & (g > _SAT_THRESHOLD)
        )
        red = (
            (sat >= self.min_saturation)
            & (r - g >= self.red_bias)
            & (r > _SAT_THRESHOLD)
        )
        mask = green | red

        hits = np.argwhere(mask)
        if hits.size == 0:
            return -1

        return x0 + hits[0][1] * 5

    def detect_rows(self, arr: np.ndarray, sig_x: int) -> list[tuple[int, int]]:
        brightness = arr[:, sig_x, :].max(axis=1) > _SAT_THRESHOLD
        padded = np.concatenate([[False], brightness, [False]])
        starts = np.where(~padded[:-1] & padded[1:])[0]
        ends = np.where(padded[:-1] & ~padded[1:])[0]
        return [
            (int(s), int(e))
            for s, e in zip(starts, ends, strict=False)
            if e - s >= _MIN_ROW_GAP
        ]

    def classify_signal(self, arr: np.ndarray, row: tuple, sig_x: int) -> str:
        y0, y1 = row
        w = arr.shape[1]
        x_lo, x_hi = max(0, sig_x - 12), min(w - 1, sig_x + 12)

        region = arr[
            y0 + 2 : y1 - 2 : max(1, (y1 - y0) // 6),
            x_lo : x_hi : max(1, (x_hi - x_lo) // 6),
        ].astype(np.float32)
        if region.size == 0:
            return ""

        avg = region.mean(axis=(0, 1))
        r, g, b = float(avg[0]), float(avg[1]), float(avg[2])

        if max(r, g, b) - min(r, g, b) < self.min_saturation:
            return ""
        if g - r >= self.green_bias and g > _SAT_THRESHOLD:
            return "BUY"
        if r - g >= self.red_bias and r > _SAT_THRESHOLD:
            return "SELL"
        return ""

    def _ocr_strip(
        self,
        img: Image.Image,
        y0: int,
        y1: int,
        x0: int,
        x1: int,
        whitelist: str = "",
    ) -> str:
        if x1 <= x0 or y1 <= y0:
            return ""
        cropped = img.crop((x0, y0, x1, y1))
        return _engine_ocr_pil(cropped, whitelist=whitelist).strip()

    def extract_row_data(
        self,
        img: Image.Image,
        arr: np.ndarray,
        row: tuple,
        sig_x: int,
    ) -> dict:
        y0, y1 = row
        w = img.size[0]
        y_mid = y0 + (y1 - y0) // 2

        left_bright = arr[y_mid, : sig_x + 1, :].max(axis=1)
        col_sig_start = (
            int(np.where(left_bright < _BRIGHTNESS_THRESH)[0][-1])
            if np.where(left_bright < _BRIGHTNESS_THRESH)[0].size
            else 0
        )

        right_bright = arr[y_mid, sig_x:, :].max(axis=1)
        col_sig_end = (
            sig_x + int(np.where(right_bright < _BRIGHTNESS_THRESH)[0][0])
            if np.where(right_bright < _BRIGHTNESS_THRESH)[0].size
            else w - 1
        )

        sym_raw = self._ocr_strip(
            img,
            y0,
            y1,
            0,
            max(2, col_sig_start - 2),
            whitelist="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!&",
        )
        symbol = clean_symbol(
            sym_raw.upper()
            .replace("T1!", "1!")
            .replace("OH", "O1!")
            .replace("EO", "CO"),
        )

        e_start = col_sig_end + 2
        search_limit = min(w - 1, e_start + int(w * 0.35))
        col_max = (
            arr[y0 + 2 : y1 - 2, e_start:search_limit].max(axis=(0, 2))
            if e_start < search_limit
            else np.array([])
        )

        in_text, gap_count, e_end = False, 0, search_limit

        for i, mx in enumerate(col_max):
            if mx > _COLOR_MAX_THRESH:
                in_text, gap_count = True, 0
            elif in_text:
                gap_count += 1
                if gap_count >= _MAX_GAP_COUNT:
                    e_end = e_start + i - gap_count + 4
                    break

        ent_raw = self._ocr_strip(img, y0, y1, e_start, e_end, whitelist="0123456789.")
        entry_match = re.search(r"\d+(\.\d+)?", ent_raw)

        return {"symbol": symbol, "entry": entry_match.group(0) if entry_match else ""}

    def process_row(
        self,
        img: Image.Image,
        arr: np.ndarray,
        row: tuple,
        sig_x: int,
    ) -> dict | None:
        signal = self.classify_signal(arr, row, sig_x)
        if not signal:
            return None

        data = self.extract_row_data(img, arr, row, sig_x)
        if not data["symbol"]:
            return None

        data["signal"] = signal
        return data

    def process_image(self, img_path: str) -> list[dict]:
        img = Image.open(img_path).convert("RGB")
        arr = np.array(img, dtype=np.uint8)

        dynamic_sig_x = self.find_signal_col_x(arr)
        if dynamic_sig_x == -1:
            return []

        rows = self.detect_rows(arr, dynamic_sig_x)

        futures = {
            self.pool.submit(self.process_row, img, arr, row, dynamic_sig_x): row
            for row in rows
        }

        results_by_row = {}
        for fut in as_completed(futures):
            row = futures[fut]
            result = fut.result()
            if result is not None:
                results_by_row[row] = result

        return [results_by_row[row] for row in rows if row in results_by_row]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Trade Execution Wrapper
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TradeExecutor:
    """Orchestrates API resolution and Dhan orders."""

    def __init__(
        self, exchange_map: dict, *, no_trade: bool, rebuild_master: bool
    ) -> None:
        self.exchange_map = exchange_map
        self.no_trade = no_trade
        self.broker = (
            DhanTrader(refresh_master_scrip=rebuild_master)
            if not self.no_trade
            else None
        )

    def get_exchange(self, symb: str) -> str:
        if symb in self.exchange_map["NSE"]:
            return "NSE"
        if symb in self.exchange_map["COMM"]:
            return "MCX"
        return ""

    def place_orders(self, orders_list: list) -> None:
        if not orders_list or self.no_trade or not self.broker:
            return

        self.broker.begin_session()
        for order in orders_list:
            symb = order["symbol"]
            exch = self.get_exchange(symb)
            if not exch:
                continue

            entry_val = 0.0
            with contextlib.suppress(ValueError, TypeError):
                entry_val = float(order.get("entry", 0))

            self.broker.fire_trade(
                symb=symb,
                exch=exch,
                signal=order["signal"],
                entry_val=entry_val,
            )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main Application Controller
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TVScannerApp:
    def __init__(self, *, new_setup: bool = False) -> None:
        self.new_setup = new_setup

        # Paths
        self.config_file = Path(__file__).parent / "tv_scanner_config.toml"
        self.seen_file = Path(__file__).parent / ".tv_scanner_seen.json"

        # Core Components
        self.config = ScannerConfig(self.config_file, self.seen_file)

        # Map TOML Settings
        settings = self.config.data.get("settings", {})
        self.reload_interval = settings.get("reload_interval", 1)
        self.buffer_seconds = settings.get("buffer_seconds", 5)
        self.debug = settings.get("debug", True)
        self.ignore_lastseen = settings.get("ignore_lastseen", True)
        self.no_trade = settings.get("no_trade", False)
        self.rebuild_master = settings.get("rebuild_master_scrip", False)
        self.tv_focus_flag = settings.get("tv_focus_flag", False)

        candidate_files = settings.get("candidate_files", [])
        self.cand_paths = [Path(OUT_DIR) / fname for fname in candidate_files]

        self.tv_symbols_map = self.config.data.get("tv_symbol_map", {})

        self.vision = ScannerVision()
        self.matcher = None
        self.executor = None

    def dprint(self, *args: Any, **kwargs: Any) -> None:
        if self.debug:
            out(" ".join(map(str, args)), **kwargs)

    def load_candidates(self) -> tuple[set, list]:
        candidates: set = set()
        cands_list = []
        for filepath in self.cand_paths:
            cur_candset = set()
            try:
                with filepath.open() as f:
                    for line in f:
                        sym = line.strip()
                        if sym:
                            candidates.add(sym)
                            cur_candset.add(sym)
            except FileNotFoundError:
                pass
            cands_list.append(cur_candset)
        return candidates, cands_list

    def capture_screen(self) -> Path:
        """Takes screencapture based on TOML coordinates."""
        coords = self.config.data.get("coordinates", {})
        tmp_path = Path(tempfile.mkstemp(suffix=".png")[1])

        w = coords["x2"] - coords["x1"]
        h = coords["y2"] - coords["y1"]

        subprocess.run(
            [
                "screencapture",
                "-x",
                "-R",
                f"{coords['x1']},{coords['y1']},{w},{h}",
                str(tmp_path),
            ],
            check=True,
        )
        return tmp_path

    def focus_tradingview(self) -> None:
        """Ensures TradingView is active and at the forefront."""
        pgrep = subprocess.run(
            ["pgrep", "-x", "TradingView"], capture_output=True, text=True, check=False
        )
        if not pgrep.stdout.strip():
            out("TradingView app closed. Exiting script.")
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
            subprocess.run(
                ["osascript", "-e", focus_script],
                check=True,
                capture_output=True,
            )
        except subprocess.SubprocessError as e:
            self.dprint(f"Focus Error: {e}")
            subprocess.run(
                ["osascript", "-e", 'tell application "TradingView" to activate'],
                check=False,
            )

    def _check_desk_1(self) -> bool:
        """Checks if TradingView is running on the current active desktop."""
        if QUARTZ_AVAILABLE:
            window_list = Quartz.CGWindowListCopyWindowInfo(
                Quartz.kCGWindowListOptionOnScreenOnly,
                Quartz.kCGNullWindowID,
            )
            return any(
                w.get(Quartz.kCGWindowOwnerName) == "TradingView" for w in window_list
            )

        script = (
            'tell application "System Events" to get name of first '
            "application process whose frontmost is true"
        )
        result = subprocess.run(
            ["osascript", "-e", script], capture_output=True, text=True, check=False
        )
        return result.stdout.strip() == "TradingView"

    def _match_candidates(self, parsed: list[dict]) -> list[dict]:
        """Matches visual parsed texts to canonical symbol candidates."""
        valid_parsed: list[dict] = []
        for row in parsed:
            sym = row["symbol"]
            best_match = self.matcher.match(sym)
            icon = "🟢" if row["signal"] == "BUY" else "🔴"

            if best_match == sym:
                valid_parsed.append(row)
                self.dprint(
                    f"  {icon} {sym:<14}  {row['signal']:<4}  "
                    f"entry={row['entry']:<10} [Exact]"
                )
            elif best_match:
                row["symbol"] = best_match
                valid_parsed.append(row)
                self.dprint(
                    f"  {icon} {sym:<14}  {row['signal']:<4}  "
                    f"entry={row['entry']:<10} [Fixed: {best_match}]"
                )
            else:
                self.dprint(
                    f"  ❌ {sym:<14}  {row['signal']:<4}  "
                    f"entry={row['entry']:<10} [No Match]"
                )
        return valid_parsed

    def _deduplicate_alerts(self, valid_parsed: list[dict]) -> list[dict]:
        """Filters out previously seen alerts if duplication logic dictates."""
        seen = self.config.load_seen()
        new_alerts = []
        cur_seen = {}

        for row in valid_parsed:
            sym = row["symbol"]
            sym = self.tv_symbols_map.get(sym, sym)
            row["symbol"] = sym
            sig = row["signal"]

            if not self.ignore_lastseen:
                if seen.get(sym) != sig:
                    new_alerts.append(row)
                    cur_seen[sym] = sig
            else:
                new_alerts.append(row)

        self.config.save_seen(cur_seen)
        return new_alerts

    def _execute_and_notify(self, new_alerts: list[dict]) -> None:
        """Passes valid alerts to executor and pops desktop notification."""
        if not new_alerts:
            self.dprint("  No new alerts.")
            return

        self.executor.place_orders(new_alerts)

        buys = [r for r in new_alerts if r["signal"] == "BUY"]
        sells = [r for r in new_alerts if r["signal"] == "SELL"]
        parts = []
        if buys:
            parts.append(f"{len(buys)}x BUY")
        if sells:
            parts.append(f"{len(sells)}x SELL")

        now_str = datetime.now(india_tz).astimezone().strftime("%H:%M")
        title = f"TV Alert {now_str}  •  {', '.join(parts)}"
        lines = [
            f"{r['symbol']:<10} {r['signal']:<4}  {r['entry']}" for r in new_alerts
        ]

        AlertNotifier.notify(title, "\n".join(lines))
        self.dprint(f"\n  Alert fired! Title: {title}")

    def process_scan(self) -> None:
        t0 = time.time()
        self.dprint("\n── Scanning ──────────────────────────────────────")

        is_on_desk_1 = self._check_desk_1()

        if self.tv_focus_flag:
            self.focus_tradingview()

        try:
            img_path = self.capture_screen()
        except RuntimeError as e:
            self.dprint(f"  screencapture failed: {e}")
            return

        if not is_on_desk_1:
            pyautogui.hotkey("ctrl", "2")

        try:
            parsed = self.vision.process_image(str(img_path))
            if not parsed:
                self.dprint("No Alerts")
                return

            self.dprint(f"  Rows detected : {len(parsed)}")
            self.dprint("\n  ── Candidate Matching ─────────────────────────────")

            valid_parsed = self._match_candidates(parsed)
            if not valid_parsed:
                self.dprint("  No BUY/SELL rows matched candidates.")
                return

            t1 = time.time()
            self.dprint(f"Scan completed in {t1 - t0:.2f} seconds")

            new_alerts = self._deduplicate_alerts(valid_parsed)
            self._execute_and_notify(new_alerts)

        finally:
            with contextlib.suppress(OSError):
                img_path.unlink()

    def run(self) -> None:
        # Initial Setup/Config checks
        if self.new_setup:
            if not self.config.setup_interactive():
                sys.exit(1)
        elif not self.config.has_coordinates():
            out("Missing coordinates in config — launching setup…\n")
            if not self.config.setup_interactive():
                sys.exit(1)

        # Build Lookups
        candidates, cands_list = self.load_candidates()

        exchange_map = {
            "NSE": cands_list[_NSE_IDX] if len(cands_list) > _NSE_IDX else set(),
            "COMM": cands_list[_COMM_IDX] if len(cands_list) > _COMM_IDX else set(),
            "CRYPTO": (
                cands_list[_CRYPTO_IDX] if len(cands_list) > _CRYPTO_IDX else set()
            ),
        }

        self.matcher = TrigramIndexMatcher(candidates)
        self.executor = TradeExecutor(
            exchange_map, no_trade=self.no_trade, rebuild_master=self.rebuild_master
        )
        self.config.clear_seen()

        if self.debug:
            self.dprint("\n  TradingView Scanner v4 started (Production)")
            self.dprint(
                f"  Interval  : {self.reload_interval} min "
                f"(+{self.buffer_seconds}s buffer)",
            )
            self.dprint(f"  Index     : Built {len(candidates)} candidates")
            self.dprint("  Ctrl+C to stop\n")
        elif not self.new_setup:
            out(
                "TradingView Scanner running in background. "
                "(Use debug=true in TOML to view scan logs)"
            )

        out(
            f"DEBUG_MODE: {self.debug}, IGNORE_LASTSEEN: {self.ignore_lastseen}, "
            f"NO_TRADE: {self.no_trade}"
        )

        # Scan Loop
        self.process_scan()
        while True:
            wait_next_wall_clock(self.reload_interval, self.buffer_seconds)
            self.process_scan()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Execution entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "-ns",
        "--new-setup",
        action="store_true",
        help="Interactively set TV coordinates.",
    )
    args = p.parse_args()

    app = TVScannerApp(new_setup=args.new_setup)
    app.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
