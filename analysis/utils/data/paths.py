from pathlib import Path

def get_project_root() -> Path:
    """Walks up the directory tree to find the pyproject.toml file."""
    # Start at the directory of the current script
    current_path = Path(__file__).resolve().parent
    
    # Check the current directory and all parents
    for parent in [current_path] + list(current_path.parents):
        if (parent / "pyproject.toml").exists():
            return parent
            
    # Fallback just in case
    raise FileNotFoundError("Could not find project root (pyproject.toml not found).")

# Create a constant you can import anywhere
ROOT_DIR_PATH_OBJ= get_project_root()
ROOT_DIR = str(ROOT_DIR_PATH_OBJ)

REMOTE_DIR = 'gs:/nse-data-bucket'
REMOTE_DIR_PATH_OBJ = Path(REMOTE_DIR)


OUT_DIR = str(ROOT_DIR_PATH_OBJ / 'out')
_nse_data_dir = 'nse_data'
_intraday_dir = 'intraday'
_indx_dir = 'index'
_web_scr = 'web_scripts'
_templates_dir =  'templates'
_logs_dir = 'logs'

NSE_LOGS_DIR = str(ROOT_DIR_PATH_OBJ / _nse_data_dir / _logs_dir)
NSE_INDX_DATA = str(ROOT_DIR_PATH_OBJ / _nse_data_dir / _indx_dir)
REMOTE_NSE_INDX_DATA = str(REMOTE_DIR_PATH_OBJ / _nse_data_dir / _indx_dir)

NSE_INTRADAY_DIR_PATH = str(ROOT_DIR_PATH_OBJ / _nse_data_dir / _intraday_dir)
REMOTE_INTRADAY_DIR_PATH = str(REMOTE_DIR_PATH_OBJ / _nse_data_dir / _intraday_dir)

TEMPLATES_ROOT_DIR = str(ROOT_DIR_PATH_OBJ / _web_scr / _templates_dir)