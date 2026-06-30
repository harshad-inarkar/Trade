"""
Logging Utilities (Ruff & Mypy Compliant)
Handles dynamic log levels, configuration, and automated log file rotation.
"""

import logging
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import TextIO

from utils.config.config_loader import load_config_toml
from utils.data.paths import CONFIG_DIR, ROOT_SRC_DIR_PATH_OBJ
from utils.time.time_utils import INDIA_TZ

_pyproject = load_config_toml(ROOT_SRC_DIR_PATH_OBJ / "pyproject.toml")


PROJECT_NAME: str = _pyproject.get("project", {}).get("name", __name__)
LOGGER: logging.Logger = logging.getLogger(PROJECT_NAME)


_app_config_path: Path = CONFIG_DIR / "app_config.toml"
_def_out_log_level: str = "info"
_def_project_log_lvl: str = "critical"

_app_config = load_config_toml(_app_config_path)


def bool_env_or_cfg(key: str, cfg: dict, default_val: bool = False) -> bool:
    val = os.environ.get(key)
    if val is not None:
        return val.lower() in ("1", "true", "yes", "on")
    return bool(cfg.get(key, default_val))


def _str_env_or_cfg(key: str, cfg: dict | None = None, default_val: str = "") -> str:
    return os.environ.get(key, cfg.get(key, default_val) if cfg else default_val)


def get_project_log_level() -> str:
    log_level = _str_env_or_cfg(
        "log_level", _app_config.get("logs", {}), _def_project_log_lvl
    )
    return log_level or _def_project_log_lvl


def set_logger_config(log_level: str = "", log_handle: TextIO = sys.stdout) -> None:
    if not log_level:
        log_level = get_project_log_level()

    numeric_level = logging.getLevelNamesMapping().get(
        log_level.upper(), logging.CRITICAL
    )
    logging.basicConfig(
        level=numeric_level,
        format="%(message)s",
        handlers=[logging.StreamHandler(log_handle)],
        force=True,
    )


def set_out_log_level(log_level: str = "info") -> None:
    global _def_out_log_level  # noqa: PLW0603
    _def_out_log_level = log_level
    LOGGER.critical("Set Def Out Log Level : %s", _def_out_log_level)


def out(msg: str = "", end: str = "\n", log_level: str = "") -> None:
    if end == "\n":
        end = ""

    current_log_level = log_level or _def_out_log_level

    match current_log_level.lower():
        case "debug":
            LOGGER.debug("%s%s", msg, end)
        case "info":
            LOGGER.info("%s%s", msg, end)
        case "warning":
            LOGGER.warning("%s%s", msg, end)
        case "error":
            LOGGER.error("%s%s", msg, end)
        case "critical":
            LOGGER.critical("%s%s", msg, end)
        case _:
            LOGGER.info("%s%s", msg, end)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Log File Manager (Object-Oriented Auto-Rotation)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class LogFileManager:
    """Manages text file handles and auto-truncates them if they exceed max size."""

    def __init__(
        self, log_dir: Path | str, max_kb: int = 0, monitor_interval_min: int = 0
    ) -> None:
        self.log_dir: Path = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.monitor_interval: int = monitor_interval_min * 60
        self.handles: dict[str, TextIO] = {}

        if max_kb <= 0:
            max_kb = _app_config.get("logs", {}).get("log_max_size_kb", 100)

        self.max_bytes: int = max_kb * 1024

        if self.monitor_interval <= 0:
            self.monitor_interval = (
                _app_config.get("logs", {}).get("log_monitor_interval_min", 15) * 60
            )

        self._stop_event: threading.Event = threading.Event()
        self._monitor_thread: threading.Thread | None = None

    def start_monitor(self) -> None:
        """Starts the background thread to monitor log sizes."""
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(
                target=self._monitor_log_sizes, daemon=True
            )
            self._monitor_thread.start()

    def stop_monitor(self) -> None:
        """Stops the background monitoring thread gracefully."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2.0)
            self._monitor_thread = None

    def open_log(self, name: str) -> TextIO:
        """Opens and tracks a log file handle for a given process/name."""
        log_path = self.log_dir / f"{name}.log"
        handle = log_path.open("a", encoding="utf-8")
        self.handles[name] = handle
        start_time = datetime.now(INDIA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        handle.write(f"{'#' * 60}\nStart: {start_time}\n{'#' * 60}\n")
        handle.write(f"Project Log Level : {get_project_log_level()}\n")
        handle.flush()

        return handle

    def close_log(self, name: str) -> None:
        """Closes and untracks a specified log file handle."""
        if (handle := self.handles.pop(name, None)) and not handle.closed:
            handle.close()

    def close_all(self) -> None:
        """Closes all tracked log file handles."""
        for name in list(self.handles.keys()):
            self.close_log(name)

    def _monitor_log_sizes(self) -> None:
        """Background loop to check file sizes and truncate if necessary."""
        while not self._stop_event.wait(self.monitor_interval):
            for name, handle in list(self.handles.items()):
                try:
                    if not handle.closed:
                        handle.flush()
                        log_path = self.log_dir / f"{name}.log"

                        if log_path.exists():
                            size = log_path.stat().st_size
                            if size > self.max_bytes:
                                handle.seek(0)
                                handle.truncate(0)
                                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                                kb_size = self.max_bytes / 1024
                                msg = (
                                    f"[{timestamp}] LogFileManager: Log exceeded "
                                    f"{kb_size}KB and was auto-cleared.\n\n"
                                )
                                handle.write(msg)
                                handle.flush()
                except OSError as exc:
                    LOGGER.critical("Error Cleaning File %s: %s", name, exc)


# Initialize module-level defaults
set_logger_config()
LOGGER.critical("Project Log Level : %s", get_project_log_level())
