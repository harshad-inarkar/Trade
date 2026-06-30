import argparse
import subprocess
import time

from utils.data.paths import (
    NSE_INDX_DATA,
    NSE_INTRADAY_DIR_PATH,
    REMOTE_INTRADAY_DIR_PATH,
    REMOTE_NSE_INDX_DATA,
)
from utils.logging.log_utils import out


def sync_with_rsync(
    remote_host: str,  # Hostname or IP of remote machine (e.g. 'server.example.com')
    remote_path: str,  # Remote path to sync from (e.g. '/home/user/data/')
    local_path: str,  # Local destination path (e.g. './data/')
    remote_src_flag: bool = True,  # If True, remote is source.
    user: str = "",  # SSH username, if needed (empty string for default)
    port: int = 22,  # SSH port (default 22)
) -> None:
    """
    Uses the system's native rsync command to synchronize
    remote data to a local directory.
    Assumes SSH keys are configured for passwordless entry.
    """
    # Build the rsync command
    # -a : Archive mode (recursive, preserves permissions, times, symlinks, etc.)
    # -z : Compress file data during the transfer
    # -e : Specify the remote shell (allows us to set custom SSH ports)

    ssh_command = f"ssh -p {port}"
    hostname = f"{user}@{remote_host}" if user else remote_host

    # Support multiple remote source directories for syncing.
    # remote_path can be a string or a list/tuple of remote dirs.

    # Normalize remote_path to a list
    remote_paths = [remote_path] if isinstance(remote_path, str) else list(remote_path)

    if remote_src_flag:
        # rsync allows multiple sources: [src1, src2, ..., dst]
        srcs = [f"{hostname}:{p}" for p in remote_paths]
        dst = local_path
    else:
        # Only allow local_path as source; remote_path must be single string
        srcs = [local_path]
        # Allow only one remote_path for remote-as-destination
        if len(remote_paths) != 1:
            err_msg = "sync local to remote, only a single remote_path is allowed."
            raise ValueError(err_msg)

        dst = f"{hostname}:{remote_paths[0]}"

    command = [
        "rsync",
        "-az",
        "--out-format=%n",  # only output copied file paths
        "-e",
        ssh_command,
        *srcs,
        dst,
    ]

    out(f"Executing: {' '.join(command)}\n", log_level="debug")

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )

        if process and process.stdout:
            out("File Transfers->")
            for line in process.stdout:
                if line.strip():
                    out(line, end="")

        process.wait()

        if process.returncode == 0:
            out("\n✅ Rsync transfer completed successfully!")
        else:
            out(f"\n❌ Rsync failed with return code {process.returncode}")

    except FileNotFoundError:
        out("Error: 'rsync' command not found.", log_level="critical")
    except (OSError, subprocess.SubprocessError) as e:
        out(f"Esception occurred during rsync: {e}", log_level="critical")


def sync_data_args(src: str, dst: str) -> None:
    cmd = [
        "rclone",
        "copy",
        src,
        dst,
        "--ignore-existing",
        "--fast-list",
        "--size-only",
        "--log-level=INFO",
        "--stats=0",
        "--exclude",
        "/.*",
        "--exclude",
        "**/.*",
    ]

    try:
        start = time.time()
        cmdout = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if cmdout.stdout:
            out(cmdout.stdout)

        out(
            f"✅ Sync from {src} to {dst} completed successfully "
            f"in {time.time() - start:.2f}s "
        )

    except subprocess.CalledProcessError as e:
        out("❌ rclone failed")
        out(e.stdout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NSE sync data")
    parser.add_argument(
        "-tr", "--to-remote", action="store_true", help="Sync to remote drive"
    )
    parser.add_argument(
        "-ix", "--index", action="store_true", help="Sync Index data to remote drive"
    )

    parser.add_argument("-rs", "--rsync-flag", action="store_true", help="Use Rsync")

    args, unknown = parser.parse_known_args()
    to_remote = False
    index_flag = False

    if args.to_remote:
        out("Sync to remote drive")
        to_remote = True

    if args.index:
        out("Sync Index Dir")
        index_flag = True

    if to_remote:
        if not index_flag:
            sync_data_args(NSE_INTRADAY_DIR_PATH, REMOTE_INTRADAY_DIR_PATH)
        else:
            sync_data_args(NSE_INDX_DATA, REMOTE_NSE_INDX_DATA)
    else:
        sync_data_args(REMOTE_INTRADAY_DIR_PATH, NSE_INTRADAY_DIR_PATH)
