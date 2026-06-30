from importlib.util import find_spec
from pathlib import Path

from utils.config.config_loader import load_config_toml

# ─── Internal Directory Constants (Kept as is) ──────────────────────────────
_nse_data_dir = "nse_data"
_intraday_dir = "intraday"
_indx_dir = "index"
_apps_dir = "apps"
out_dir_name = "out"
templates_dir = "templates"
_logs_dir = "logs"
_holidays_list = "holidays_list.csv"


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

        return load_config_toml(self.config_path)

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

    def get_module_path(
        self, module_name: str, parent_flag: bool = True
    ) -> Path | None:
        """Return the Path to the top-level module directory for a given module name."""
        spec = find_spec(module_name)
        if spec and spec.origin:
            return Path(spec.origin).parent if parent_flag else Path(spec.origin)
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Module-Level Exports
# (Maintains compatibility with all existing `from ... import` statements)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Instantiate the path manager
_manager = PathManager()


def get_module_path(module_name: str, parent_flag: bool = True) -> Path | None:
    """Return the Path to the top-level module directory for a given module name."""
    return _manager.get_module_path(module_name=module_name, parent_flag=parent_flag)


# Base Path Objects & Strings
ROOT_SRC_DIR_PATH_OBJ: Path = _manager.root_src
ROOT_SRC_DIR: str = str(ROOT_SRC_DIR_PATH_OBJ)

ROOT_DATA_DIR_PATH_OBJ: Path = _manager.root_data
ROOT_DATA_DIR: str = str(ROOT_DATA_DIR_PATH_OBJ)

REMOTE_DIR_PATH_OBJ: Path | None = _manager.remote
REMOTE_DIR: str | None = (
    str(REMOTE_DIR_PATH_OBJ) if REMOTE_DIR_PATH_OBJ is not None else None
)

# Derived Paths
OUT_DIR: str = str(ROOT_DATA_DIR_PATH_OBJ / out_dir_name)

NSE_LOGS_DIR: str = str(ROOT_DATA_DIR_PATH_OBJ / _nse_data_dir / _logs_dir)
NSE_INDX_DATA: str = str(ROOT_DATA_DIR_PATH_OBJ / _nse_data_dir / _indx_dir)
REMOTE_NSE_INDX_DATA: str | None = (
    str(REMOTE_DIR_PATH_OBJ / _nse_data_dir / _indx_dir)
    if REMOTE_DIR_PATH_OBJ is not None
    else None
)

NSE_INTRADAY_DIR_PATH: str = str(ROOT_DATA_DIR_PATH_OBJ / _nse_data_dir / _intraday_dir)
REMOTE_INTRADAY_DIR_PATH: str | None = (
    str(REMOTE_DIR_PATH_OBJ / _nse_data_dir / _intraday_dir)
    if REMOTE_DIR_PATH_OBJ is not None
    else None
)

MASTER_CONFIG_PATH = Path("~/.config").expanduser()
CONFIG_DIR: Path = ROOT_SRC_DIR_PATH_OBJ / "utils/config"
HOLIDAYS_LIST_PATH = Path(OUT_DIR) / _holidays_list
