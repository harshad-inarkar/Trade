import logging
import os
import sys
from typing import TextIO

import tomllib

from utils.data.paths import CONFIG_DIR, ROOT_SRC_DIR_PATH_OBJ

_pyproject = {}
with (ROOT_SRC_DIR_PATH_OBJ / "pyproject.toml").open("rb") as f:
    _pyproject = tomllib.load(f)

PROJECT_NAME = _pyproject.get("project", {}).get("name", __name__)
LOGGER = logging.getLogger(PROJECT_NAME)


_app_config_path = CONFIG_DIR / "app_config.toml"

_app_config = {}
with (_app_config_path).open("rb") as f:
    _app_config = tomllib.load(f)


def bool_env_or_cfg(key: str, cfg: dict, default_val: bool = False) -> bool:
    val = os.environ.get(key)
    if val is not None:
        return val.lower() in ("1", "true", "yes", "on")
    return bool(cfg.get(key, default_val))


def _str_env_or_cfg(key: str, cfg: dict | None = None, default_val: str = "") -> str:
    return os.environ.get(key, cfg.get(key, default_val) if cfg else default_val)


def get_project_log_level() -> str:
    def_log_lvl = "critical"
    log_level = _str_env_or_cfg("log_level", _app_config.get("apps", {}), def_log_lvl)
    if not log_level:
        log_level = def_log_lvl

    return log_level


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


def out(msg: str = "", end: str = "\n") -> None:
    if end == "\n":
        end = ""

    LOGGER.info("%s%s", msg, end)


set_logger_config()
LOGGER.critical("Log Level : %s", get_project_log_level())
