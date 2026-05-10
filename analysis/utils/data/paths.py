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
NSE_DATA_DIR = 'nse_data'
INTRADAY_DIR = 'intraday'
INDX_DIR = 'index'
WEB_SCR = 'web_scripts'
TEMPLS =  'templates'

NSE_INDX_DATA = str(ROOT_DIR_PATH_OBJ / NSE_DATA_DIR / INDX_DIR)
REMOTE_NSE_INDX_DATA = str(REMOTE_DIR_PATH_OBJ / NSE_DATA_DIR / INDX_DIR)

NSE_INTRADAY_DIR_PATH = str(ROOT_DIR_PATH_OBJ / NSE_DATA_DIR / INTRADAY_DIR)
REMOTE_INTRADAY_DIR_PATH = str(REMOTE_DIR_PATH_OBJ / NSE_DATA_DIR / INTRADAY_DIR)

TEMPLATES_PARENT_DIR = str(ROOT_DIR_PATH_OBJ / WEB_SCR / TEMPLS)