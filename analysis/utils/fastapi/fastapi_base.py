"""
utils/fastapi_base.py
Shared functionality for FastAPI application instantiation and configuration.
"""

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from utils.data.paths import templates_dir
from utils.logging.log_utils import get_project_log_level, out


@dataclass(frozen=True)
class AppPaths:
    """Standardized path resolution for FastAPI apps."""

    base: Path
    config: Path
    templates: Path

    @classmethod
    def resolve(cls, caller_file: str) -> "AppPaths":
        """
        Calculates app-specific paths using the caller's __file__ attribute.
        """
        base_dir = Path(caller_file).resolve().parent
        return cls(
            base=base_dir,
            config=base_dir / f"{Path(caller_file).stem}.toml",
            templates=base_dir / templates_dir,
        )


class BaseAppConfig:
    """Shared configuration logic for all FastAPI applications."""

    def __init__(self, path: Path):
        self.path = path
        self.raw_cfg = self._load()

        srv = self.raw_cfg.get("server", {})
        self.host: str = srv.get("host", "localhost")
        self.port: int = srv.get("port", 8000)
        self.reload: bool = srv.get("reload", False)

    def _load(self) -> dict[str, Any]:
        try:
            with self.path.open("rb") as config_file:
                return tomllib.load(config_file)
        except (OSError, tomllib.TOMLDecodeError):
            return {}


class BaseFastAPIApp:
    """Shared FastAPI server initialization and layout."""

    def __init__(
        self,
        title: str,
        config: BaseAppConfig,
        template_dir: Path,
        lifespan: Callable[[FastAPI], AsyncIterator[None]] | None = None,
        root_path: str = "/",
    ):
        self.cfg = config
        self.app = FastAPI(title=title, lifespan=lifespan, root_path=root_path)
        self.templates = Jinja2Templates(directory=template_dir)

        # Automatically mount static directory
        static_dir = template_dir / "static"
        static_dir.mkdir(parents=True, exist_ok=True)

        mount_path = (
            "/static"  # f"{root_path}/static" if root_path != "/" else "/static"
        )

        self.app.mount(
            mount_path,
            StaticFiles(directory=str(static_dir.resolve())),
            name="static",
        )
        out(
            f"Static files mounted at {mount_path} -> {static_dir.resolve()}",
            log_level="critical",
        )

    def _setup_routes(self) -> None:
        """Override this method in the child class to register routes."""
        raise NotImplementedError

    def run(self, log_level: str = get_project_log_level()) -> None:
        try:
            uvicorn.run(
                self.app,
                host=self.cfg.host,
                port=self.cfg.port,
                reload=self.cfg.reload,
                log_level=log_level,
                proxy_headers=True,
                forwarded_allow_ips="*",
            )
        except (KeyboardInterrupt, SystemExit, KeyError):
            out("FastApi Run Exception", log_level="error")
