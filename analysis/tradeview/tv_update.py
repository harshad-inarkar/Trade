import pyautogui
import time
import argparse
import os
import sys
import subprocess, datetime
from pathlib import Path
from utils.data.paths import OUT_DIR
from utils.utility import wait_next_wall_clock


if sys.platform == "darwin":
    try:
        import AppKit
        # This tells macOS that this app is a background 'Agent'
        info = AppKit.NSBundle.mainBundle().infoDictionary()
        info["LSUIElement"] = "1"
    except ImportError:
        # If AppKit isn't installed, you can install it via: pip install pyobjc-framework-AppKit
        pass



coord_path =  str(Path(__file__).parent / ".tv_indicator_coord" )
new_setup = False

CANDIDATES_PATH = os.path.join(OUT_DIR, 'candidates_merge.txt')
RELOAD_INTERVAL = 15 # min
BUFFER_SECONDS = 15 # secs
# Safety: Move mouse to any corner of the screen to abort the script
pyautogui.FAILSAFE = True 

def is_user_active(seconds=1):
    """Checks if the mouse moves during a wait period to detect user activity."""
    pos1 = pyautogui.position()
    time.sleep(seconds)
    pos2 = pyautogui.position()
    return pos1 != pos2

def get_coordinates():
    """Reads coordinates from file if exists, otherwise prompts user & saves them."""
    coords = None
    if not new_setup and os.path.exists(coord_path):
        try:
            with open(coord_path, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
                if len(lines) == 3:
                    indicator_xy = tuple(map(int, lines[0].split(',')))
                    textbox_xy = tuple(map(int, lines[1].split(',')))
                    ok_xy = tuple(map(int, lines[2].split(',')))
                    coords = (indicator_xy, textbox_xy, ok_xy)
        except Exception as e:
            print(f"Error reading {coord_path}: {e}")

    if coords is not None:
        print(f"Read TradingView indicator coordinates from {coord_path}:")
        print(f"  Indicator Label: {coords[0]}")
        print(f"  Symbols Textbox: {coords[1]}")
        print(f"  OK Button:      {coords[2]}")
        return coords

    # Capture via prompts
    print("\n--- COORDINATE SETUP ---")
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

    # Save to file
    try:
        os.makedirs(os.path.dirname(coord_path), exist_ok=True)
        with open(coord_path, "w") as f:
            f.write(f"{indicator_xy.x},{indicator_xy.y}\n")
            f.write(f"{textbox_xy.x},{textbox_xy.y}\n")
            f.write(f"{ok_xy.x},{ok_xy.y}\n")
        print(f"Coordinates saved to {coord_path}")
    except Exception as e:
        print(f"Failed to save coordinates to {coord_path}: {e}")

    return ( (indicator_xy.x, indicator_xy.y),
             (textbox_xy.x, textbox_xy.y),
             (ok_xy.x, ok_xy.y)
           )


def update_tv_app(coords):
    # App Status Check
    check_app = os.popen("pgrep -x 'TradingView'").read()
    if not check_app:
        print("TradingView app closed. Exiting script.")
        sys.exit()

    # 1. User Active Check (Initial Warning)
    os.system("afplay /System/Library/Sounds/Tink.aiff")
    time.sleep(3)

    if is_user_active(1):
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] User active. Skipping update...")
        return

    # Store original position and current frontmost app
    original_pos = pyautogui.position()
    
    # Capture symbols first so we spend less time focused on TV
    if not os.path.exists(CANDIDATES_PATH):
        print(f"Error: {CANDIDATES_PATH} not found.")
        return
    with open(CANDIDATES_PATH, "r") as f:
        symbols = "\n".join([line.strip() for line in f if line.strip()])

   # 2.  AGGRESSIVE FOCUS BLOCK
    # This script tells System Events to force the TradingView process to the absolute front
    # focus_script1 = """
    # tell application "System Events"
    #     tell process "TradingView"
    #         set frontmost to true
    #         perform action "AXRaise" of window 1
    #     end tell
    # end tell
    # """

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
        time.sleep(1.0) # Critical: give macOS time to swap window layers
    except Exception as e:
        print(f"Focus Error: {e}")
        # Fallback to standard activate if System Events fails
        os.system("osascript -e 'tell application \"TradingView\" to activate'")
        time.sleep(1.5)

    # 3. Execution
    indicator_xy, textbox_xy, ok_xy = coords

    # Double click the indicator
    pyautogui.doubleClick(indicator_xy, interval=0.1)
    time.sleep(0.8) # Wait for settings modal

    # Click the textbox and clear it
    pyautogui.click(textbox_xy)
    pyautogui.hotkey('command', 'a')
    pyautogui.press('backspace')
    
    # Use clipboard to avoid character-by-character typing interference
    process = subprocess.Popen(['pbcopy'], stdin=subprocess.PIPE)
    process.communicate(symbols.encode('utf-8'))
    pyautogui.hotkey('command', 'v')
    
    time.sleep(0.3)
    pyautogui.press('enter')
    
    # Explicitly click OK in case 'enter' was intercepted by a different UI element
    pyautogui.click(ok_xy)
    
    # 4. Low Impact: Return mouse and snap focus back (optional)
    pyautogui.moveTo(original_pos)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='NSE Intraday Flask Web Portal')
    parser.add_argument('-ri', '--reload-interval', type=int, help='Reload interval in minutes')
    parser.add_argument('-ns', '--new-setup', action='store_true', help='New Setup')


    args, unknown = parser.parse_known_args()
    if args.reload_interval is not None: RELOAD_INTERVAL = args.reload_interval
    if args.new_setup: new_setup = True
    
    coords = get_coordinates()
    print(f"Automation active. Monitoring for {RELOAD_INTERVAL}m intervals...")
    
    update_tv_app(coords)


    while True:
        wait_next_wall_clock(RELOAD_INTERVAL, BUFFER_SECONDS)
        update_tv_app(coords)