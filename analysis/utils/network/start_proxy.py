import subprocess
import time
from pathlib import Path
import tomllib
import sys


script_dir = Path(__file__).parent
config_file = script_dir / "proxy_config.toml"


class SSHProxyManager:
    """Manages the lifecycle of an SSH SOCKS proxy."""

    def __init__(self, config_path: str | Path = config_file):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
        # Extract configurations
        proxy_cfg = self.config.get("proxy", {})
        self.host = proxy_cfg.get("host", "80.225.247.216")
        self.user = proxy_cfg.get("user", "ubuntu")
        self.port = proxy_cfg.get("port", 9090)
        self.restart_delay = proxy_cfg.get("restart_delay", 2.0)
        
        # Expand user path (e.g., ~/.ssh/...)
        raw_key_path = proxy_cfg.get("key_path", "~/.ssh/oracle_dhan.key")
        self.key_path = Path(raw_key_path).expanduser()

    def _load_config(self) -> dict:
        """Loads configuration from the TOML file."""
        if not self.config_path.exists():
            print(f"Warning: Config file '{self.config_path}' not found. Using default values.")
            return {}
        
        try:
            with open(self.config_path, "rb") as f:
                return tomllib.load(f)
        except Exception as e:
            print(f"Error reading {self.config_path}: {e}")
            sys.exit(1)

    def stop(self):
        """Kills any existing SSH proxy process tied to the configured key or port."""
        print(f"Stopping existing SSH proxy processes on port {self.port}...")
        
        # 1. Kill via SSH signature
        pkill_cmd = f"pkill -f 'ssh -i {self.key_path}'"
        subprocess.run(pkill_cmd, shell=True, stderr=subprocess.DEVNULL)
        
        # 2. Kill whatever is occupying the local SOCKS port
        kill_port_cmd = f"kill -15 $(lsof -t -i:{self.port}) 2>/dev/null"
        subprocess.run(kill_port_cmd, shell=True, stderr=subprocess.DEVNULL)

    def start(self):
        """Starts the SSH proxy process in the background."""
        if not self.key_path.exists():
            print(f"Error: SSH key not found at {self.key_path}")
            sys.exit(1)

        print(f"Starting SSH proxy to {self.user}@{self.host}...")
        
        ssh_cmd = (
            f"ssh -i {self.key_path} "
            f"-D {self.port} "
            f"{self.user}@{self.host} "
            "-N -f"
        )
        
        try:
            subprocess.run(ssh_cmd, shell=True, check=True)
            print(f"✅ SSH proxy successfully restarted (listening on port {self.port}).")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to start SSH proxy: {e}")

    def restart(self):
        """Performs a full restart of the proxy connection."""
        self.stop()
        time.sleep(self.restart_delay)  # Wait for sockets to close gracefully
        self.start()


if __name__ == "__main__":
    # Ensure config file path is resolved relative to the script location
    
    proxy_manager = SSHProxyManager()
    proxy_manager.restart()