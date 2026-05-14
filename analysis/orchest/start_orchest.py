import subprocess
import os
import sys
import tomllib  
import threading
import time
from pathlib import Path

# Attempt to load psutil for performance stats
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ─── Custom Imports ───────────────────────────────────────────────────────────
from utils.data.paths import NSE_LOGS_DIR, ROOT_SRC_DIR

# Default configurations
MAX_LOG_SIZE_KB = 100 #KB
LOG_MONITOR_INT = 15 #min
STATS_MONITOR_INT = 3 #min
log_root_dir = NSE_LOGS_DIR

class ScriptManager:
    def __init__(self, config_filename="orchest_config.toml"):
        self.config_path = Path(__file__).parent / config_filename
        self.processes = {}
        self.log_handles = {} 
        self.config = {} 
        
        # Stats tracking state
        self.latest_stats = {}
        self.latest_sys_stats = {}
        self.last_stats_update = "Initializing..."
        self._proc_monitors = {}
        
        # Ensure log directory exists
        os.makedirs(log_root_dir, exist_ok=True)
        
        self.load_config()
        self.max_bytes = self.config.get('max_log_size', MAX_LOG_SIZE_KB) * 1024
        self.log_monitor_interval = self.config.get('log_monitor_interval', LOG_MONITOR_INT) * 60 
        self.stats_monitor_interval = self.config.get('stats_monitor_interval', STATS_MONITOR_INT) * 60

        # Background Log Monitor
        self.log_thread = threading.Thread(target=self._monitor_log_sizes, daemon=True)
        self.log_thread.start()

        # Background Stats Monitor
        if PSUTIL_AVAILABLE:
            self.stats_thread = threading.Thread(target=self._monitor_stats, daemon=True)
            self.stats_thread.start()

    def load_config(self):
        if not os.path.exists(self.config_path):
            print(f"Error: Configuration file '{self.config_path}' not found.")
            sys.exit(1)
            
        with open(self.config_path, "rb") as f:
            self.config = tomllib.load(f)

    def _monitor_stats(self):
        """Background thread to continuously average CPU/RAM over the configured interval."""
        psutil.cpu_percent(interval=None) # Start system timer
        first_run = True
        
        while True:
            # Do a quick 2-second snapshot on startup so the user has immediate data, 
            # then switch to the user-configured 6-minute loop.
            if first_run:
                time.sleep(2)
                first_run = False
            else:
                time.sleep(self.stats_monitor_interval)
                
            new_stats = {}
            for name, process in list(self.processes.items()):
                if process.poll() is None:
                    try:
                        # If script is newly started or restarted (PID changed), initialize psutil
                        if name not in self._proc_monitors or self._proc_monitors[name][0] != process.pid:
                            proc_obj = psutil.Process(process.pid)
                            proc_obj.cpu_percent(interval=None) # Start timer for this specific PID
                            self._proc_monitors[name] = (process.pid, proc_obj)
                            
                            # Pause a tiny bit so we don't record a pure 0.0% instantly
                            time.sleep(0.1)
                            cpu = proc_obj.cpu_percent(interval=None)
                        else:
                            # Existing process: calculate average since the last interval
                            proc_obj = self._proc_monitors[name][1]
                            cpu = proc_obj.cpu_percent(interval=None)
                            
                        mem = proc_obj.memory_info()
                        new_stats[name] = {
                            "cpu": cpu,
                            "rss": mem.rss / (1024 * 1024),
                        }
                    except psutil.NoSuchProcess:
                        pass
                        
            # Update State globally
            self.latest_sys_stats = {
                "cpu": psutil.cpu_percent(interval=None),
                "ram": psutil.virtual_memory().percent,
                "swp": psutil.swap_memory().percent
            }
            self.latest_stats = new_stats
            self.last_stats_update = time.strftime('%H:%M:%S')

    def _monitor_log_sizes(self):
        """Background thread that truncates log files if they exceed the max size."""
        while True:
            time.sleep(self.log_monitor_interval)
            for name, handle in list(self.log_handles.items()):
                try:
                    if not handle.closed:
                        handle.flush()
                        log_path = os.path.join(log_root_dir, f"{name}.log")
                        
                        if os.path.exists(log_path):
                            size = os.path.getsize(log_path)
                            if size > self.max_bytes:
                                handle.seek(0)
                                handle.truncate(0)
                                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                                handle.write(f"[{timestamp}] Orchestrator: Log exceeded {self.max_bytes/1024}KB and was auto-cleared.\n\n")
                                handle.flush()
                except Exception as exc:
                    print(f'Error Cleaning File {name} {exc}')
                    pass

    def start(self, name):
        self.load_config()
        if name not in self.config["scripts"]:
            print(f"Error: Script '{name}' not found in config.")
            return

        if name in self.processes and self.processes[name].poll() is None:
            print(f"Warning: '{name}' is already running (PID: {self.processes[name].pid}).")
            return

        script_info = self.config["scripts"][name]
        module_name = script_info.get("module")
        
        if not module_name:
            print(f"Error: No 'module' specified for '{name}' in config.")
            return

        cmd = [sys.executable, "-u", "-m", module_name] + script_info.get("args", [])

        try:
            log_path = os.path.join(log_root_dir, f"{name}.log")
            self.log_handles[name] = open(log_path, "a")
            
            start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            self.log_handles[name].write(f"{'#' * 60}\nStart: {start_time}\n{'#' * 60}\n")
            self.log_handles[name].flush()
       
            out_target = self.log_handles[name]

            process = subprocess.Popen(
                cmd,
                cwd=ROOT_SRC_DIR,
                stdout=out_target,
                stderr=subprocess.STDOUT
            )
            
            self.processes[name] = process
            print(f"[+] Started '{name}' [{module_name}] PID: {process.pid} (Logging to {name}.log)")
            
        except Exception as e:
            print(f"[-] Failed to start '{name}': {e}")
            if name in self.log_handles:
                self.log_handles[name].close()
                del self.log_handles[name]

    def stop(self, name):
        if name in self.processes:
            process = self.processes[name]
            if process.poll() is None: 
                process.terminate()
                process.wait(timeout=5)
                print(f"[-] Stopped '{name}'")
            else:
                print(f"Warning: '{name}' is not currently running.")
        else:
            print(f"Error: '{name}' is not being managed right now.")
            
        if name in self.log_handles:
            self.log_handles[name].close()
            del self.log_handles[name]

    def restart(self, name):
        print(f"[*] Restarting '{name}'...")
        self.stop(name)
        self.start(name)

    def status(self):
        print("\n--- Process Status ---")
        for name in self.config["scripts"]:
            if name in self.processes:
                process = self.processes[name]
                if process.poll() is None:
                    print(f" 🟢 {name:<15} : RUNNING (PID: {process.pid})")
                else:
                    print(f" 🔴 {name:<15} : STOPPED (Exit code: {process.returncode})")
            else:
                print(f" ⚪ {name:<15} : NOT STARTED")
        print("----------------------\n")

    def stats(self):
        if not PSUTIL_AVAILABLE:
            print("\n[!] The 'psutil' library is missing. Please run 'pip install psutil' to view stats.\n")
            return

        interval_mins = self.stats_monitor_interval / 60
        print(f"\n--- Performance Stats (Last {interval_mins:.1f} min Average) ---")
        print(f"    Last Snapshot Taken: {self.last_stats_update}")
        
        for name in self.config["scripts"]:
            if name in self.processes and self.processes[name].poll() is None:
                if name in self.latest_stats:
                    s = self.latest_stats[name]
                    print(f" 🟢 {name:<15} : CPU: {s['cpu']:>5.1f}% | RAM: {s['rss']:>6.1f} MB")
                else:
                    print(f" 🟢 {name:<15} : (Gathering data...)")
            else:
                print(f" ⚪ {name:<15} : NOT RUNNING")

        print("-" * 65)
        sys_s = self.latest_sys_stats
        if sys_s:
            print(f" 🖥️ SYSTEM OVERVIEW : CPU: {sys_s['cpu']}% | RAM: {sys_s['ram']}% | SWAP: {sys_s['swp']}%")
        else:
            print(" 🖥️ SYSTEM OVERVIEW : (Gathering data...)")
        print("-----------------------------------------------------------------\n")

    def start_all(self):
        for name in self.config["scripts"]:
            self.start(name)

    def stop_all(self):
        for name in list(self.processes.keys()):
            self.stop(name)

def main():
    manager = ScriptManager()
    
    print(f"Project Root Detected: {ROOT_SRC_DIR}")
    print("Starting all scripts based on config...")
    manager.start_all()
    manager.status()

    print("Type 'help' for available commands.")
    
    while True:
        try:
            cmd_input = input("orchestrator> ").strip().split()
            if not cmd_input:
                continue
                
            action = cmd_input[0].lower()
            args_input = cmd_input[1:]

            if action in ["exit", "quit"]:
                print("Stopping all scripts and exiting...")
                manager.stop_all()
                break
            elif action == "status":
                manager.status()
            elif action == "stats":
                manager.stats()
            elif action == "start":
                if args_input: manager.start(args_input[0])
                else: print("Usage: start <script_name>")
            elif action == "stop":
                if args_input: manager.stop(args_input[0])
                else: print("Usage: stop <script_name>")
            elif action == "restart":
                if args_input: manager.restart(args_input[0])
                else: print("Usage: restart <script_name>")
            elif action == "help":
                print("Available commands:")
                print("  status                - View running processes")
                print("  stats                 - View Background CPU & Memory Averages")
                print("  start <name>          - Start a script (e.g., start app)")
                print("  stop <name>           - Stop a script")
                print("  restart <name>        - Restart a single script")
                print("  exit/quit             - Stop all scripts and close orchestrator")
            else:
                print(f"Unknown command: {action}. Type 'help' for options.")
                
        except KeyboardInterrupt:
            print("\nCaught interrupt. Stopping all scripts and exiting...")
            manager.stop_all()
            break

if __name__ == "__main__":
    main()