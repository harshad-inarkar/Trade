from pathlib import Path

import tomllib


def load_config_toml(toml_file_path: str | Path) -> dict:
    toml_file_path = (
        Path(toml_file_path) if not isinstance(toml_file_path, Path) else toml_file_path
    )

    if toml_file_path.exists():
        with toml_file_path.open("rb") as f:
            return tomllib.load(f)
    else:
        err_msg = f"{toml_file_path} Not Found"
        raise FileNotFoundError(err_msg)
