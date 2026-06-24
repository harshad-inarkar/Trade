import argparse
import contextlib
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pyautogui
import tomllib

from utils.data.paths import OUT_DIR
from utils.time.time_utils import INDIA_TZ, out, wait_next_wall_clock

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


# Safety mechanism
pyautogui.FAILSAFE = True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TVUpdateConfig:
    """Manages reading and writing settings and coordinates to a TOML file."""

    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.data = self._load()

    def _load(self) -> dict:
        if self.config_path.exists():
            try:
                with self.config_path.open("rb") as f:
                    return tomllib.load(f)
            except (OSError, tomllib.TOMLDecodeError) as e:
                out(f"Error reading {self.config_path}: {e}")
        return {}

    def save_coordinates(
        self, indicator: tuple[int, int], textbox: tuple[int, int], ok: tuple[int, int]
    ) -> None:
        """Saves coordinates to the TOML file while preserving other settings."""
        self.data.setdefault("coordinates", {})
        self.data["coordinates"]["indicator"] = list(indicator)
        self.data["coordinates"]["textbox"] = list(textbox)
        self.data["coordinates"]["ok"] = list(ok)
        self._write_toml()

    def _write_toml(self) -> None:
        """Minimal manual TOML writer for simple dicts."""
        lines = []
        for section, content in self.data.items():
            lines.append(f"[{section}]")
            for k, v in content.items():
                if isinstance(v, list):
                    lines.append(f"{k} = {v}")
                elif isinstance(v, str):
                    lines.append(f'{k} = "{v}"')
                else:
                    lines.append(f"{k} = {v}")
            lines.append("")

        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config_path.open("w") as f:
            f.write("\n".join(lines))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Application Logic
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TVUpdaterApp:
    def __init__(self, args: argparse.Namespace) -> None:
        self.config_file = Path(__file__).parent / "tv_update_config.toml"
        self.config = TVUpdateConfig(self.config_file)

        # Load Settings (Args -> Config -> Defaults)
        settings = self.config.data.get("settings", {})

        self.reload_interval = settings.get("reload_interval", 15)
        self.buffer_seconds = settings.get("buffer_seconds", 15)
        self.tv_focus_flag = settings.get("tv_focus_flag", False)
        self.new_setup = args.new_setup

        # Candidates path resolution
        default_cand_path = Path(OUT_DIR) / "candidates_merge.txt"
        self.candidates_path = Path(settings.get("candidates_path", default_cand_path))

        self.coords = None

    def _is_user_active(self, seconds: float = 1) -> bool:
        """Checks if the mouse moves during a wait period to detect user activity."""
        pos1 = pyautogui.position()
        time.sleep(seconds)
        pos2 = pyautogui.position()
        return pos1 != pos2

    def _setup_coordinates(
        self,
    ) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
        """Interactive prompt to capture coordinates."""
        out("\n─── COORDINATE SETUP ───")
        out(
            "Position your mouse and press ENTER in the terminal to capture each point."
        )

        input("1. Hover over the INDICATOR LABEL (for double-click) and press Enter...")
        indicator_xy = pyautogui.position()
        out(f"Captured: {indicator_xy}")

        input("2. Hover over the SYMBOLS TEXTBOX center and press Enter...")
        textbox_xy = pyautogui.position()
        out(f"Captured: {textbox_xy}")

        input("3. Hover over the OK BUTTON and press Enter...")
        ok_xy = pyautogui.position()
        out(f"Captured: {ok_xy}\n")

        self.config.save_coordinates(
            (indicator_xy.x, indicator_xy.y),
            (textbox_xy.x, textbox_xy.y),
            (ok_xy.x, ok_xy.y),
        )
        out(f"Coordinates saved to {self.config_file}")

        return (
            (indicator_xy.x, indicator_xy.y),
            (textbox_xy.x, textbox_xy.y),
            (ok_xy.x, ok_xy.y),
        )

    def _load_coordinates(
        self,
    ) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
        """Loads coordinates from config or forces setup if missing."""
        coord_data = self.config.data.get("coordinates", {})

        if self.new_setup or not coord_data.get("indicator"):
            return self._setup_coordinates()

        try:
            indicator = (
                int(coord_data["indicator"][0]),
                int(coord_data["indicator"][1]),
            )
            textbox = (
                int(coord_data["textbox"][0]),
                int(coord_data["textbox"][1]),
            )
            ok = (
                int(coord_data["ok"][0]),
                int(coord_data["ok"][1]),
            )
        except (KeyError, ValueError, IndexError):
            out("Incomplete coordinates in config. Falling back to setup.")
            return self._setup_coordinates()
        else:
            out(f"Loaded coordinates from {self.config_file.name}:")
            out(f"  Indicator: {indicator}")
            out(f"  Textbox:   {textbox}")
            out(f"  OK Button: {ok}")
            return indicator, textbox, ok

    def _focus_tradingview(self) -> None:
        """Forces macOS to bring TradingView to the absolute front."""
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
            time.sleep(1.0)  # Give macOS time to swap window layers
        except subprocess.SubprocessError as e:
            out(f"Focus Error: {e}")
            subprocess.run(
                ["osascript", "-e", 'tell application "TradingView" to activate'],
                check=False,
            )
            time.sleep(1.5)

    def _perform_update(self) -> None:
        """Core automation sequence."""
        # App Status Check
        pgrep = subprocess.run(
            ["pgrep", "-x", "TradingView"], capture_output=True, text=True, check=False
        )
        if not pgrep.stdout.strip():
            out("TradingView app closed. Exiting script.")
            sys.exit()

        # 1. User Active Check (Initial Warning)
        subprocess.run(["afplay", "/System/Library/Sounds/Tink.aiff"], check=False)
        time.sleep(2)

        if self._is_user_active(1):
            now_str = datetime.now(INDIA_TZ).astimezone().strftime("%H:%M:%S")
            out(f"[{now_str}] User active. Skipping update...")
            return

        # Read latest candidates
        if not self.candidates_path.exists():
            out(f"Error: {self.candidates_path} not found. Skipping update.")
            return

        with self.candidates_path.open() as f:
            symbols = "\n".join([line.strip() for line in f if line.strip()])

        original_pos = pyautogui.position()

        # Boomerang logic
        is_on_desk_1 = False
        if QUARTZ_AVAILABLE:
            window_list = Quartz.CGWindowListCopyWindowInfo(
                Quartz.kCGWindowListOptionOnScreenOnly,
                Quartz.kCGNullWindowID,
            )
            is_on_desk_1 = any(
                w.get(Quartz.kCGWindowOwnerName) == "TradingView" for w in window_list
            )
        else:
            script = (
                'tell application "System Events" to get name of first '
                "application process whose frontmost is true"
            )
            result = subprocess.run(
                ["osascript", "-e", script], capture_output=True, text=True, check=False
            )
            is_on_desk_1 = result.stdout.strip() == "TradingView"

        if not is_on_desk_1:
            pass

        # 2. Focus Window
        if self.tv_focus_flag:
            self._focus_tradingview()

        # 3. Execution
        if not self.coords:
            out("Error: Coordinates are not set. Please run setup first.")
            return
        indicator_xy, textbox_xy, ok_xy = self.coords

        pyautogui.doubleClick(indicator_xy, interval=0.1)
        time.sleep(0.3)  # Wait for modal

        pyautogui.click(textbox_xy)
        pyautogui.hotkey("command", "a")
        pyautogui.press("backspace")

        # Use clipboard to avoid typing interference
        process = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE)
        process.communicate(symbols.encode("utf-8"))
        pyautogui.hotkey("command", "v")

        time.sleep(0.3)
        pyautogui.press("enter")
        pyautogui.click(ok_xy)

        if not is_on_desk_1:
            pyautogui.hotkey("ctrl", "2")

        # 4. Snap mouse back
        pyautogui.moveTo(original_pos)
        now_str = datetime.now(INDIA_TZ).astimezone().strftime("%H:%M:%S")
        out(f"[{now_str}] TV Updated successfully.")

    def run(self) -> None:
        self.coords = self._load_coordinates()

        out(f"\nAutomation active. Monitoring for {self.reload_interval}m intervals...")
        self._perform_update()

        while True:
            wait_next_wall_clock(self.reload_interval, self.buffer_seconds)
            self._perform_update()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Execution Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TradingView Indicator Auto-Updater")
    parser.add_argument(
        "-ns",
        "--new-setup",
        action="store_true",
        help="Force new coordinate setup",
    )

    args, _ = parser.parse_known_args()
    app = TVUpdaterApp(args)

    try:
        app.run()
    except KeyboardInterrupt:
        out("\nExiting.")
        sys.exit(0)
