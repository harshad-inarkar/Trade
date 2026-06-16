import sys
from pathlib import Path

import tomllib

# ─── Internal Directory Constants (Kept as is) ──────────────────────────────
_nse_data_dir = "nse_data"
_intraday_dir = "intraday"
_indx_dir = "index"
_web_scr = "web_scripts"
_templates_dir = "templates"
_logs_dir = "logs"


def out(msg: str = "", end: str = "\n") -> None:
    sys.stdout.write(f"{msg}{end}")


class PathManager:
    """Object-oriented manager to resolve project paths and configurations."""

    def __init__(self, config_filename: str = "paths_config.toml") -> None:
        self._current_dir = Path(__file__).resolve().parent
        self.config_path = self._current_dir / config_filename
        self.config = self._load_config()

        # Initialize base paths
        self.root_src = self._resolve_src_root()
        self.root_data = self._resolve_data_root()
        self.remote = self._resolve_remote_root()

    def _load_config(self) -> dict:
        """Loads path settings from TOML config if it exists."""
        if self.config_path.exists():
            try:
                with self.config_path.open("rb") as f:
                    return tomllib.load(f).get("paths", {})
            except tomllib.TOMLDecodeError as e:
                out(f"Error loading {self.config_path.name}: {e}")
        return {}

    def _find_pyproject_root(self) -> Path:
        """Walks up the directory tree to find the pyproject.toml file."""
        for parent in [self._current_dir, *self._current_dir.parents]:
            if (parent / "pyproject.toml").exists():
                return parent

        err_msg = "Could not find project root (pyproject.toml not found)."
        raise FileNotFoundError(err_msg)

    def _resolve_src_root(self) -> Path:
        configured_path = self.config.get("root_src_dir")
        if configured_path:
            return Path(configured_path).resolve()
        return self._find_pyproject_root()

    def _resolve_data_root(self) -> Path:
        configured_path = self.config.get("root_data_dir")
        if configured_path:
            return Path(configured_path).resolve()
        # Fallback to source directory if no separate data drive/folder is configured
        return self.root_src

    def _resolve_remote_root(self) -> Path | None:
        configured_path = self.config.get("remote_dir")
        if configured_path:
            return Path(configured_path)
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Module-Level Exports
# (Maintains compatibility with all existing `from ... import` statements)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Instantiate the path manager
_manager = PathManager()

# Base Path Objects & Strings
ROOT_SRC_DIR_PATH_OBJ = _manager.root_src
ROOT_SRC_DIR = str(ROOT_SRC_DIR_PATH_OBJ)

ROOT_DATA_DIR_PATH_OBJ = _manager.root_data
ROOT_DATA_DIR = str(ROOT_DATA_DIR_PATH_OBJ)

REMOTE_DIR_PATH_OBJ = _manager.remote
REMOTE_DIR = str(REMOTE_DIR_PATH_OBJ)

# Derived Paths
OUT_DIR = str(ROOT_DATA_DIR_PATH_OBJ / "out")

NSE_LOGS_DIR = str(ROOT_DATA_DIR_PATH_OBJ / _nse_data_dir / _logs_dir)
NSE_INDX_DATA = str(ROOT_DATA_DIR_PATH_OBJ / _nse_data_dir / _indx_dir)
REMOTE_NSE_INDX_DATA = str(REMOTE_DIR_PATH_OBJ / _nse_data_dir / _indx_dir)

NSE_INTRADAY_DIR_PATH = str(ROOT_DATA_DIR_PATH_OBJ / _nse_data_dir / _intraday_dir)
REMOTE_INTRADAY_DIR_PATH = str(REMOTE_DIR_PATH_OBJ / _nse_data_dir / _intraday_dir)

TEMPLATES_ROOT_DIR = str(ROOT_SRC_DIR_PATH_OBJ / _web_scr / _templates_dir)
