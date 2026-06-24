import argparse
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import tomllib

from utils.utility import INDIA_TZ

# Attempt to load psutil for performance stats
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ─── Custom Imports ───────────────────────────────────────────────────────────
from utils.data.paths import NSE_LOGS_DIR, ROOT_SRC_DIR
from utils.utility import LOGGER

# Default configurations
MAX_LOG_SIZE_KB = 100  # KB
LOG_MONITOR_INT = 15  # min
STATS_MONITOR_INT = 1  # min
log_root_dir = NSE_LOGS_DIR


class ScriptManager:
    def __init__(self, config_filename: str = "orchest_config.toml") -> None:
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
        Path(log_root_dir).mkdir(parents=True, exist_ok=True)

        self.load_config()

        self.max_bytes = self.config.get("max_log_size", MAX_LOG_SIZE_KB) * 1024
        self.log_monitor_interval = (
            self.config.get("log_monitor_interval", LOG_MONITOR_INT) * 60
        )
        self.stats_monitor_interval = (
            self.config.get("stats_monitor_interval", STATS_MONITOR_INT) * 60
        )

        # Background Log Monitor
        self.log_thread = threading.Thread(target=self._monitor_log_sizes, daemon=True)
        self.log_thread.start()

        # Background Stats Monitor
        if PSUTIL_AVAILABLE:
            self.stats_thread = threading.Thread(
                target=self._monitor_stats,
                daemon=True,
            )
            self.stats_thread.start()

    def load_config(self) -> None:
        if not self.config_path.exists():
            LOGGER.critical(
                f"Error: Configuration file '{self.config_path}' not found."
            )
            sys.exit(1)

        with self.config_path.open("rb") as f:
            self.config = tomllib.load(f)

    def _monitor_stats(self) -> None:
        """Background thread to continuously average CPU/RAM.

        It runs over the configured interval.
        """
        psutil.cpu_percent(interval=None)  # Start system timer
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
                        # If script is newly started or restarted (PID changed),
                        # initialize psutil
                        if (
                            name not in self._proc_monitors
                            or self._proc_monitors[name][0] != process.pid
                        ):
                            proc_obj = psutil.Process(process.pid)
                            proc_obj.cpu_percent(
                                interval=None,
                            )  # Start timer for this specific PID
                            self._proc_monitors[name] = (process.pid, proc_obj)

                            # Pause a tiny bit so we don't record a pure 0.0% instantly
                            time.sleep(0.1)
                            cpu = proc_obj.cpu_percent(interval=None)
                        else:
                            # Existing process: calculate average
                            # since the last interval
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
                "swp": psutil.swap_memory().percent,
            }
            self.latest_stats = new_stats
            self.last_stats_update = time.strftime("%H:%M:%S")

    def _monitor_log_sizes(self) -> None:
        """Background thread truncating log files if they exceed max size."""
        while True:
            time.sleep(self.log_monitor_interval)
            for name, handle in list(self.log_handles.items()):
                try:
                    if not handle.closed:
                        handle.flush()
                        log_path = Path(log_root_dir) / f"{name}.log"

                        if log_path.exists():
                            size = log_path.stat().st_size
                            if size > self.max_bytes:
                                handle.seek(0)
                                handle.truncate(0)
                                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                                kb_size = self.max_bytes / 1024
                                msg = (
                                    f"[{timestamp}] Orchestrator: Log exceeded "
                                    f"{kb_size}KB and was auto-cleared.\n\n"
                                )
                                handle.write(msg)
                                handle.flush()
                except OSError as exc:
                    LOGGER.critical(f"Error Cleaning File {name} {exc}")

    def start(self, name: str) -> None:
        self.load_config()
        if name not in self.config["scripts"]:
            LOGGER.critical(f"Error: Script '{name}' not found in config.")
            return

        if name in self.processes and self.processes[name].poll() is None:
            pid = self.processes[name].pid
            LOGGER.critical(f"Warning: '{name}' is already running (PID: {pid}).")
            return

        script_info = self.config["scripts"][name]
        module_name = script_info.get("module")

        if not module_name:
            LOGGER.critical(f"Error: No 'module' specified for '{name}' in config.")
            return

        cmd = [
            sys.executable,
            "-u",
            "-m",
            module_name,
            *script_info.get("args", []),
        ]

        try:
            log_path = Path(log_root_dir) / f"{name}.log"
            self.log_handles[name] = log_path.open("a")

            start_time = datetime.now(INDIA_TZ).strftime("%Y-%m-%d %H:%M:%S")

            self.log_handles[name].write(
                f"{'#' * 60}\nStart: {start_time}\n{'#' * 60}\n",
            )
            self.log_handles[name].flush()

            out_target = self.log_handles[name]

            process = subprocess.Popen(
                cmd,
                cwd=ROOT_SRC_DIR,
                stdout=out_target,
                stderr=subprocess.STDOUT,
            )

            self.processes[name] = process
            LOGGER.critical(
                f"[+] Started '{name}' [{module_name}] PID: {process.pid} "
                f"(Logging to {name}.log)"
            )

        except (OSError, ValueError) as e:
            LOGGER.critical(f"[-] Failed to start '{name}': {e}")
            if name in self.log_handles:
                self.log_handles[name].close()
                del self.log_handles[name]

    def stop(self, name: str) -> None:
        if name in self.processes:
            process = self.processes[name]
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
                LOGGER.critical(f"[-] Stopped '{name}'")
            else:
                LOGGER.critical(f"Warning: '{name}' is not currently running.")
        else:
            LOGGER.critical(f"Error: '{name}' is not being managed right now.")

        if name in self.log_handles:
            self.log_handles[name].close()
            del self.log_handles[name]

    def restart(self, name: str) -> None:
        LOGGER.critical(f"[*] Restarting '{name}'...")
        self.stop(name)
        self.start(name)

    def status(self) -> None:
        LOGGER.critical("\n--- Process Status ---")
        for name in self.config["scripts"]:
            if name in self.processes:
                process = self.processes[name]
                if process.poll() is None:
                    LOGGER.critical(f" 🟢 {name:<15} : RUNNING (PID: {process.pid})")
                else:
                    LOGGER.critical(
                        f" 🔴 {name:<15} : STOPPED (Exit code: {process.returncode})"
                    )
            else:
                LOGGER.critical(f" ⚪ {name:<15} : NOT STARTED")
        LOGGER.critical("----------------------\n")

    def stats(self) -> None:
        if not PSUTIL_AVAILABLE:
            LOGGER.critical(
                "\n[!] The 'psutil' library is missing. "
                "Please run 'pip install psutil' to view stats.\n"
            )
            return

        interval_mins = self.stats_monitor_interval / 60
        LOGGER.critical(
            f"\n--- Performance Stats (Last {interval_mins:.1f} min Average) ---"
        )
        LOGGER.critical(f"    Last Snapshot Taken: {self.last_stats_update}")

        for name in self.config["scripts"]:
            if name in self.processes and self.processes[name].poll() is None:
                if name in self.latest_stats:
                    s = self.latest_stats[name]
                    cpu = s["cpu"]
                    rss = s["rss"]
                    LOGGER.critical(
                        f" 🟢 {name:<15} : CPU: {cpu:>5.1f}% | RAM: {rss:>6.1f} MB"
                    )
                else:
                    LOGGER.critical(f" 🟢 {name:<15} : (Gathering data...)")
            else:
                LOGGER.critical(f" ⚪ {name:<15} : NOT RUNNING")

        LOGGER.critical("-" * 65)
        sys_s = self.latest_sys_stats
        if sys_s:
            cpu_s = sys_s["cpu"]
            ram_s = sys_s["ram"]
            swp_s = sys_s["swp"]
            LOGGER.critical(
                f" 🖥️ SYSTEM OVERVIEW : CPU: {cpu_s}% | RAM: {ram_s}% | SWAP: {swp_s}%"
            )
        else:
            LOGGER.critical(" 🖥️ SYSTEM OVERVIEW : (Gathering data...)")
        LOGGER.critical(
            "-----------------------------------------------------------------\n"
        )

    def start_all(self) -> None:
        for name in self.config["scripts"]:
            self.start(name)

    def stop_all(self) -> None:
        for name in list(self.processes.keys()):
            self.stop(name)


def _show_help() -> None:
    """Display available commands."""
    LOGGER.critical("Available commands:")
    LOGGER.critical("  status                - View running processes")
    LOGGER.critical("  stats                 - View Background CPU & Memory Averages")
    LOGGER.critical("  start <name>          - Start a script (e.g., start app)")
    LOGGER.critical("  stop <name>           - Stop a script")
    LOGGER.critical("  restart <name>        - Restart a single script")
    LOGGER.critical("  exit/quit             - Stop all scripts and close orchestrator")


def handle_command(manager: ScriptManager, action: str, args: list[str]) -> bool:
    """Process a single CLI command. Returns False to exit the loop."""
    if action in ("exit", "quit"):
        LOGGER.critical("Stopping all scripts and exiting...")
        manager.stop_all()
        return False
    if action == "status":
        manager.status()
    elif action == "stats":
        manager.stats()
    elif action == "start":
        if args:
            manager.start(args[0])
        else:
            LOGGER.critical("Usage: start <script_name>")
    elif action == "stop":
        if args:
            manager.stop(args[0])
        else:
            LOGGER.critical("Usage: stop <script_name>")
    elif action == "restart":
        if args:
            manager.restart(args[0])
        else:
            LOGGER.critical("Usage: restart <script_name>")
    elif action == "help":
        _show_help()
    else:
        LOGGER.critical(f"Unknown command: {action}. Type 'help' for options.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Script Orchestrator")
    parser.add_argument(
        "-ml",
        "--module-list",
        nargs="+",
        help="List of modules to start (e.g. -ml tv_update trade_app)",
        dest="module_list",
        default=None,
    )
    args = parser.parse_args()

    manager = ScriptManager()

    if args.module_list:
        LOGGER.critical(f"Starting scripts: {', '.join(args.module_list)}")
        for module_name in args.module_list:
            manager.start(module_name)
    else:
        LOGGER.critical("Starting all scripts based on config...")
        manager.start_all()

    manager.status()
    LOGGER.critical("Type 'help' for available commands.")

    while True:
        try:
            cmd_input = input("orchestrator> ").strip().split()
            if not cmd_input:
                continue

            action = cmd_input[0].lower()
            args_input = cmd_input[1:]

            if not handle_command(manager, action, args_input):
                break

        except KeyboardInterrupt:
            LOGGER.critical("\nCaught interrupt. Stopping all scripts and exiting...")
            manager.stop_all()
            break


if __name__ == "__main__":
    main()
