import subprocess
import os
import sys
import tomllib  
import threading
import time
from pathlib import Path

# ─── Custom Imports ───────────────────────────────────────────────────────────
from utils.data.paths import NSE_LOGS_DIR, ROOT_SRC_DIR

# Set your max log size here (100 KB = 100 * 1024 bytes)
MAX_LOG_SIZE_KB = 100
log_root_dir = NSE_LOGS_DIR

class ScriptManager:
    def __init__(self, config_filename="config.toml"):
        self.config_path = Path(__file__).parent / config_filename
        self.processes = {}
        self.log_handles = {} 
        self.config = {} 
        
        
        # Ensure log directory exists
        os.makedirs(log_root_dir, exist_ok=True)
        
        self.load_config()
        self.max_bytes = self.config.get('max_log_size',MAX_LOG_SIZE_KB) * 1024

        # Always run the background log monitor since logging is always enabled
        self.monitor_thread = threading.Thread(target=self._monitor_log_sizes, daemon=True)
        self.monitor_thread.start()

    def load_config(self):
        if not os.path.exists(self.config_path):
            print(f"Error: Configuration file '{self.config_path}' not found.")
            sys.exit(1)
            
        with open(self.config_path, "rb") as f:
            self.config = tomllib.load(f)

    def _monitor_log_sizes(self):
        """Background thread that truncates log files if they exceed the max size."""
        while True:
            time.sleep(5)  # Check file sizes every 5 seconds
            
            # Iterate over a copy of the items to avoid dictionary changed size errors
            for name, handle in list(self.log_handles.items()):
                try:
                    if not handle.closed:
                        handle.flush() # Ensure OS buffers are written to disk
                        log_path = os.path.join(log_root_dir, f"{name}.log")
                        
                        if os.path.exists(log_path):
                            size = os.path.getsize(log_path)
                            if size > self.max_bytes:
                                # Safely wipe the file contents without breaking the subprocess pipe
                                handle.seek(0)
                                handle.truncate(0)
                                
                                # Leave a breadcrumb so you know why the logs vanished
                                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                                handle.write(f"[{timestamp}] Orchestrator: Log exceeded {self.max_bytes/1024}KB and was auto-cleared.\n\n")
                                handle.flush()
                except Exception as exc:
                    # Fail silently in the background thread so we don't crash the orchestrator
                    print(f'Error CLeaning File {name} {exc}')
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
            # Setup Log Output
            log_path = os.path.join(log_root_dir, f"{name}.log")
            self.log_handles[name] = open(log_path, "a")
            
            # Add Start Separator safely
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

    def start_all(self):
        for name in self.config["scripts"]:
            self.start(name)

    def stop_all(self):
        for name in list(self.processes.keys()):
            self.stop(name)

def main():
    manager = ScriptManager("config.toml")
    
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