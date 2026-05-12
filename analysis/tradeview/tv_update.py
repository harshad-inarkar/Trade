#!/usr/bin/env python3
import os
import sys
import time
import argparse
import subprocess
import datetime
from pathlib import Path
import tomllib

import pyautogui

from utils.data.paths import OUT_DIR
from utils.utility import wait_next_wall_clock

# ─────────────────────────────────────────────────────────────────────────────
# macOS Background App Registration
# ─────────────────────────────────────────────────────────────────────────────
if sys.platform == "darwin":
    try:
        import AppKit
        info = AppKit.NSBundle.mainBundle().infoDictionary()
        info["LSUIElement"] = "1"
    except ImportError:
        pass

# Safety mechanism
pyautogui.FAILSAFE = True 


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TVUpdateConfig:
    """Manages reading and writing settings and coordinates to a TOML file."""
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.data = self._load()

    def _load(self) -> dict:
        if self.config_path.exists():
            try:
                with open(self.config_path, "rb") as f:
                    return tomllib.load(f)
            except Exception as e:
                print(f"Error reading {self.config_path}: {e}")
        return {}

    def save_coordinates(self, indicator: tuple, textbox: tuple, ok: tuple):
        """Saves coordinates to the TOML file while preserving other settings."""
        self.data.setdefault('coordinates', {})
        self.data['coordinates']['indicator'] = list(indicator)
        self.data['coordinates']['textbox']   = list(textbox)
        self.data['coordinates']['ok']        = list(ok)
        self._write_toml()

    def _write_toml(self):
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
        with open(self.config_path, "w") as f:
            f.write("\n".join(lines))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Application Logic
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TVUpdaterApp:
    def __init__(self, args):
        self.config_file = Path(__file__).parent / "tv_update_config.toml"
        self.config = TVUpdateConfig(self.config_file)
        
        # Load Settings (Args -> Config -> Defaults)
        settings = self.config.data.get('settings', {})
        
        self.reload_interval = args.reload_interval or settings.get('reload_interval', 15)
        self.buffer_seconds  = settings.get('buffer_seconds', 15)
        self.new_setup       = args.new_setup
        
        # Candidates path resolution
        default_cand_path = os.path.join(OUT_DIR, 'candidates_merge.txt')
        self.candidates_path = Path(settings.get('candidates_path', default_cand_path))
        
        self.coords = None

    def _is_user_active(self, seconds=1) -> bool:
        """Checks if the mouse moves during a wait period to detect user activity."""
        pos1 = pyautogui.position()
        time.sleep(seconds)
        pos2 = pyautogui.position()
        return pos1 != pos2

    def _setup_coordinates(self) -> tuple:
        """Interactive prompt to capture coordinates."""
        print("\n─── COORDINATE SETUP ───")
        print("Position your mouse and press ENTER in the terminal to capture each point.")
        
        input("1. Hover over the INDICATOR LABEL (for double-click) and press Enter...")
        indicator_xy = pyautogui.position()
        print(f"Captured: {indicator_xy}")

        input("2. Hover over the SYMBOLS TEXTBOX center and press Enter...")
        textbox_xy = pyautogui.position()
        print(f"Captured: {textbox_xy}")

        input("3. Hover over the OK BUTTON and press Enter...")
        ok_xy = pyautogui.position()
        print(f"Captured: {ok_xy}\n")

        self.config.save_coordinates(
            (indicator_xy.x, indicator_xy.y),
            (textbox_xy.x, textbox_xy.y),
            (ok_xy.x, ok_xy.y)
        )
        print(f"Coordinates saved to {self.config_file}")
        
        return (indicator_xy.x, indicator_xy.y), (textbox_xy.x, textbox_xy.y), (ok_xy.x, ok_xy.y)

    def _load_coordinates(self) -> tuple:
        """Loads coordinates from config or forces setup if missing."""
        coord_data = self.config.data.get('coordinates', {})
        
        if self.new_setup or not coord_data.get('indicator'):
            return self._setup_coordinates()
            
        try:
            indicator = tuple(coord_data['indicator'])
            textbox   = tuple(coord_data['textbox'])
            ok        = tuple(coord_data['ok'])
            
            print(f"Loaded coordinates from {self.config_file.name}:")
            print(f"  Indicator: {indicator}")
            print(f"  Textbox:   {textbox}")
            print(f"  OK Button: {ok}")
            return indicator, textbox, ok
        except KeyError:
            print("Incomplete coordinates in config. Falling back to setup.")
            return self._setup_coordinates()

    def _focus_tradingview(self):
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
            subprocess.run(["osascript", "-e", focus_script], check=True, capture_output=True)
            time.sleep(1.0) # Give macOS time to swap window layers
        except Exception as e:
            print(f"Focus Error: {e}")
            os.system("osascript -e 'tell application \"TradingView\" to activate'")
            time.sleep(1.5)

    def _perform_update(self):
        """Core automation sequence."""
        # App Status Check
        if not os.popen("pgrep -x 'TradingView'").read():
            print("TradingView app closed. Exiting script.")
            sys.exit()

        # 1. User Active Check (Initial Warning)
        os.system("afplay /System/Library/Sounds/Tink.aiff")
        time.sleep(3)

        if self._is_user_active(1):
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] User active. Skipping update...")
            return

        # Read latest candidates
        if not self.candidates_path.exists():
            print(f"Error: {self.candidates_path} not found. Skipping update.")
            return
            
        with open(self.candidates_path, "r") as f:
            symbols = "\n".join([line.strip() for line in f if line.strip()])

        original_pos = pyautogui.position()
        
        # 2. Focus Window
        self._focus_tradingview()

        # 3. Execution
        indicator_xy, textbox_xy, ok_xy = self.coords

        pyautogui.doubleClick(indicator_xy, interval=0.1)
        time.sleep(0.8) # Wait for modal

        pyautogui.click(textbox_xy)
        pyautogui.hotkey('command', 'a')
        pyautogui.press('backspace')
        
        # Use clipboard to avoid typing interference
        process = subprocess.Popen(['pbcopy'], stdin=subprocess.PIPE)
        process.communicate(symbols.encode('utf-8'))
        pyautogui.hotkey('command', 'v')
        
        time.sleep(0.3)
        pyautogui.press('enter')
        pyautogui.click(ok_xy)
        
        # 4. Snap mouse back
        pyautogui.moveTo(original_pos)
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] TV Updated successfully.")

    def run(self):
        self.coords = self._load_coordinates()
        
        print(f"\nAutomation active. Monitoring for {self.reload_interval}m intervals...")
        self._perform_update()

        while True:
            wait_next_wall_clock(self.reload_interval, self.buffer_seconds)
            self._perform_update()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Execution Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TradingView Indicator Auto-Updater')
    parser.add_argument('-ri', '--reload-interval', type=int, help='Reload interval in minutes')
    parser.add_argument('-ns', '--new-setup', action='store_true', help='Force new coordinate setup')

    args, _ = parser.parse_known_args()
    
    app = TVUpdaterApp(args)
    
    try:
        app.run()
    except KeyboardInterrupt:
        print("\nExiting.")
        sys.exit(0)