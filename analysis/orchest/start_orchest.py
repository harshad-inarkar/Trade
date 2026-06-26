"""
Script Orchestrator (Ruff & Mypy Compliant)
"""

import argparse
import subprocess
import sys
import threading
import time
from pathlib import Path

import tomllib

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ─── Custom Imports ───────────────────────────────────────────────────────────
from utils.data.paths import NSE_LOGS_DIR, ROOT_SRC_DIR
from utils.logging.log_utils import LogFileManager, out, set_out_log_level

# Default configurations
MAX_LOG_SIZE_KB = 100  # KB
LOG_MONITOR_INT = 15  # min
STATS_MONITOR_INT = 1  # min


class ScriptManager:
    def __init__(self, config_filename: str = "orchest_config.toml") -> None:
        self.config_path: Path = Path(__file__).parent / config_filename
        self.processes: dict[str, subprocess.Popen] = {}
        self.config: dict = {}

        # Stats tracking state
        self.latest_stats: dict = {}
        self.latest_sys_stats: dict = {}
        self.last_stats_update: str = "Initializing..."
        self._proc_monitors: dict = {}

        self.load_config()

        self.log_manager = LogFileManager(log_dir=NSE_LOGS_DIR)
        self.log_manager.start_monitor()

        self.stats_monitor_interval = (
            self.config.get("stats_monitor_interval", STATS_MONITOR_INT) * 60
        )

        # Background Stats Monitor
        if PSUTIL_AVAILABLE:
            self.stats_thread = threading.Thread(
                target=self._monitor_stats,
                daemon=True,
            )
            self.stats_thread.start()

    def load_config(self) -> None:
        if not self.config_path.exists():
            out(f"Error: Configuration file '{self.config_path}' not found.")
            sys.exit(1)

        with self.config_path.open("rb") as f:
            self.config = tomllib.load(f)

    def _monitor_stats(self) -> None:
        psutil.cpu_percent(interval=None)
        first_run = True

        while True:
            if first_run:
                time.sleep(2)
                first_run = False
            else:
                time.sleep(self.stats_monitor_interval)

            new_stats = {}
            for name, process in list(self.processes.items()):
                if process.poll() is None:
                    try:
                        if (
                            name not in self._proc_monitors
                            or self._proc_monitors[name][0] != process.pid
                        ):
                            proc_obj = psutil.Process(process.pid)
                            proc_obj.cpu_percent(interval=None)
                            self._proc_monitors[name] = (process.pid, proc_obj)
                            time.sleep(0.1)
                            cpu = proc_obj.cpu_percent(interval=None)
                        else:
                            proc_obj = self._proc_monitors[name][1]
                            cpu = proc_obj.cpu_percent(interval=None)

                        mem = proc_obj.memory_info()
                        new_stats[name] = {
                            "cpu": cpu,
                            "rss": mem.rss / (1024 * 1024),
                        }
                    except psutil.NoSuchProcess:
                        pass

            self.latest_sys_stats = {
                "cpu": psutil.cpu_percent(interval=None),
                "ram": psutil.virtual_memory().percent,
                "swp": psutil.swap_memory().percent,
            }
            self.latest_stats = new_stats
            self.last_stats_update = time.strftime("%H:%M:%S")

    def start(self, name: str) -> None:
        self.load_config()
        if name not in self.config["scripts"]:
            out(f"Error: Script '{name}' not found in config.")
            return

        if name in self.processes and self.processes[name].poll() is None:
            pid = self.processes[name].pid
            out(f"Warning: '{name}' is already running (PID: {pid}).")
            return

        script_info = self.config["scripts"][name]
        module_name = script_info.get("module")

        if not module_name:
            out(f"Error: No 'module' specified for '{name}' in config.")
            return

        cmd = [
            sys.executable,
            "-u",
            "-m",
            module_name,
            *script_info.get("args", []),
        ]

        try:
            out_target = self.log_manager.open_log(name)
            process = subprocess.Popen(
                cmd,
                cwd=ROOT_SRC_DIR,
                stdout=out_target,
                stderr=subprocess.STDOUT,
            )

            self.processes[name] = process
            out(
                f"[+] Started '{name}' [{module_name}] PID: {process.pid} "
                f"(Logging to {name}.log)"
            )

        except (OSError, ValueError) as e:
            out(f"[-] Failed to start '{name}': {e}")
            self.log_manager.close_log(name)

    def stop(self, name: str) -> None:
        if name in self.processes:
            process = self.processes[name]
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
                out(f"[-] Stopped '{name}'")
            else:
                out(f"Warning: '{name}' is not currently running.")
        else:
            out(f"Error: '{name}' is not being managed right now.")

        self.log_manager.close_log(name)

    def restart(self, name: str) -> None:
        out(f"[*] Restarting '{name}'...")
        self.stop(name)
        self.start(name)

    def status(self) -> None:
        out("\n--- Process Status ---")
        for name in self.config.get("scripts", {}):
            if name in self.processes:
                process = self.processes[name]
                if process.poll() is None:
                    out(f" 🟢 {name:<15} : RUNNING (PID: {process.pid})")
                else:
                    out(f" 🔴 {name:<15} : STOPPED (Exit code: {process.returncode})")
            else:
                out(f" ⚪ {name:<15} : NOT STARTED")
        out("----------------------\n")

    def stats(self) -> None:
        if not PSUTIL_AVAILABLE:
            out(
                "\n[!] The 'psutil' library is missing. "
                "Please run 'pip install psutil' to view stats.\n"
            )
            return

        interval_mins = self.stats_monitor_interval / 60
        out(f"\n--- Performance Stats (Last {interval_mins:.1f} min Average) ---")
        out(f"    Last Snapshot Taken: {self.last_stats_update}")

        for name in self.config.get("scripts", {}):
            if name in self.processes and self.processes[name].poll() is None:
                if name in self.latest_stats:
                    s = self.latest_stats[name]
                    out(
                        f" 🟢 {name:<15} : CPU: {s['cpu']:>5.1f}% | "
                        f"RAM: {s['rss']:>6.1f} MB"
                    )
                else:
                    out(f" 🟢 {name:<15} : (Gathering data...)")
            else:
                out(f" ⚪ {name:<15} : NOT RUNNING")

        out("-" * 65)
        sys_s = self.latest_sys_stats
        if sys_s:
            out(
                f" 🖥️ SYSTEM OVERVIEW : CPU: {sys_s['cpu']}% | "
                f"RAM: {sys_s['ram']}% | SWAP: {sys_s['swp']}%"
            )
        else:
            out(" 🖥️ SYSTEM OVERVIEW : (Gathering data...)")
        out("-----------------------------------------------------------------\n")

    def start_all(self) -> None:
        for name in self.config.get("scripts", {}):
            self.start(name)

    def stop_all(self) -> None:
        for name in list(self.processes.keys()):
            self.stop(name)
        self.log_manager.stop_monitor()
        self.log_manager.close_all()


def _show_help() -> None:
    out("Available commands:")
    out("  status                - View running processes")
    out("  stats                 - View Background CPU & Memory Averages")
    out("  start <name>          - Start a script (e.g., start app)")
    out("  stop <name>           - Stop a script")
    out("  restart <name>        - Restart a single script")
    out("  exit/quit             - Stop all scripts and close orchestrator")


def handle_command(manager: ScriptManager, action: str, args: list[str]) -> bool:
    if action in ("exit", "quit"):
        out("Stopping all scripts and exiting...")
        manager.stop_all()
        return False
    if action == "status":
        manager.status()
    elif action == "stats":
        manager.stats()
    elif action == "start":
        manager.start(args[0]) if args else out("Usage: start <script_name>")
    elif action == "stop":
        manager.stop(args[0]) if args else out("Usage: stop <script_name>")
    elif action == "restart":
        manager.restart(args[0]) if args else out("Usage: restart <script_name>")
    elif action == "help":
        _show_help()
    else:
        out(f"Unknown command: {action}. Type 'help' for options.")
    return True


def main() -> None:
    set_out_log_level("critical")
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
        out(f"Starting scripts: {', '.join(args.module_list)}")
        for module_name in args.module_list:
            manager.start(module_name)
    else:
        out("Starting all scripts based on config...")
        manager.start_all()

    manager.status()
    out("Type 'help' for available commands.")

    while True:
        try:
            cmd_input = input("orchestrator> ").strip().split()
            if not cmd_input:
                continue

            if not handle_command(manager, cmd_input[0].lower(), cmd_input[1:]):
                break

        except KeyboardInterrupt:
            out("\nCaught interrupt. Stopping all scripts and exiting...")
            manager.stop_all()
            break


if __name__ == "__main__":
    main()
