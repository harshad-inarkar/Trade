import contextlib
import logging
import os
import subprocess
import time
from pathlib import Path

import tomllib

from utils.logging.log_utils import LOGGER

script_dir = Path(__file__).parent
config_file = script_dir / "proxy_config.toml"


class SSHProxyManager:
    """Manages the lifecycle of an AutoSSH SOCKS proxy."""

    def __init__(self, config_path: str | Path = config_file):
        self.config_path = Path(config_path)
        self.config = self._load_config()

        # Extract configurations
        proxy_cfg = self.config.get("proxy", {})
        self.host = proxy_cfg.get("host", "")
        self.port = proxy_cfg.get("port", 0)
        self.proxy_host = proxy_cfg.get("proxy_host", "localhost")
        self.webhook_port = proxy_cfg.get("webhook_port", 0)
        self.restart_delay = proxy_cfg.get("restart_delay", 1)

        # AutoSSH configurations
        self.server_alive_interval = proxy_cfg.get("server_alive_interval", 30)
        self.server_alive_count_max = proxy_cfg.get("server_alive_count_max", 3)

    def _load_config(self) -> dict:
        """Loads configuration from the TOML file."""
        if not self.config_path.exists():
            LOGGER.info(
                "Warning: Config file '%s' not found. Using default values.",
                self.config_path,
            )
            return {}

        try:
            with self.config_path.open("rb") as f:
                return tomllib.load(f)
        except (OSError, tomllib.TOMLDecodeError):
            LOGGER.exception("Error reading %s", self.config_path)
            return {}

    def stop(self) -> None:
        """Kills any existing local AutoSSH/SSH proxy processes."""
        LOGGER.critical(
            "Stopping existing local proxy processes on port %s...", self.port
        )

        # 1. Kill the AutoSSH monitor process securely
        subprocess.run(
            ["pkill", "-f", f"autossh.*{self.host}"],
            check=False,
            stderr=subprocess.DEVNULL,
        )

        # 2. Kill via original SSH signature securely
        subprocess.run(
            ["pkill", "-f", "ssh"],
            check=False,
            stderr=subprocess.DEVNULL,
        )

        # 3. Kill whatever is occupying the local SOCKS port safely without a shell
        with contextlib.suppress(subprocess.CalledProcessError):
            # Step A: Fetch the Process IDs (PIDs) using lsof
            pids_raw = subprocess.check_output(
                ["lsof", "-t", f"-i:{self.port}"], stderr=subprocess.DEVNULL
            )
            pids = pids_raw.decode().strip().split()

            # Step B: If processes were found, kill them
            if pids:
                subprocess.run(
                    ["kill", "-15", *pids],
                    check=False,
                    stderr=subprocess.DEVNULL,
                )

        self.clear_remote_zombies()

    def clear_remote_zombies(self) -> None:
        """Actively logs into Oracle server to kill ghost
        processes holding Port 8000."""

        LOGGER.critical(
            "🧹 Sweeping Oracle server for zombie connections on Port %s...",
            self.webhook_port,
        )

        # The raw bash pipeline to execute on the Oracle server
        remote_bash_cmd = (
            f"sudo ss -lptn 'sport = :{self.webhook_port}' | "
            r"grep -oP 'pid=\K[0-9]+' | xargs -r sudo kill -9"
        )

        # The local SSH command formatted securely as a list
        ssh_command_list = [
            "ssh",
            f"{self.host}",
            remote_bash_cmd,
        ]

        with contextlib.suppress(Exception):
            subprocess.run(
                ssh_command_list,
                check=False,
                stderr=subprocess.DEVNULL,
            )

    def start(self) -> None:
        """Starts the AutoSSH proxy process in the background."""

        self.clear_remote_zombies()
        LOGGER.critical("🚀 Starting AutoSSH proxy to %s...", self.host)

        # Securely pass all arguments as an explicit list
        autossh_cmd = [
            "autossh",
            "-M",
            "0",
            "-f",
            "-N",
            "-o",
            f"ServerAliveInterval {self.server_alive_interval}",
            "-o",
            f"ServerAliveCountMax {self.server_alive_count_max}",
            "-o",
            "ExitOnForwardFailure=yes",
            "-D",
            str(self.port),
            "-R",
            f"{self.webhook_port}:{self.proxy_host}:{self.webhook_port}",
            f"{self.host}",
        ]

        env = os.environ.copy()
        env["AUTOSSH_GATETIME"] = "0"

        try:
            subprocess.run(
                autossh_cmd,
                check=True,
                env=env,
            )
            LOGGER.info(
                "✅ Tunnel securely established! (Oracle Port %s -> Mac Port %s)",
                self.webhook_port,
                self.webhook_port,
            )
        except subprocess.CalledProcessError:
            LOGGER.exception("❌ Failed to start AutoSSH proxy")

    def restart(self) -> None:
        """Performs a full restart of the proxy connection."""
        self.stop()
        time.sleep(self.restart_delay)
        self.start()
        time.sleep(self.restart_delay)


if __name__ == "__main__":
    # ADD THIS LINE to make your console output visible
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    proxy_manager = SSHProxyManager()
    proxy_manager.restart()
