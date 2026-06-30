import argparse
import shlex
import subprocess
import time
from pathlib import Path, PurePosixPath
from typing import Any

from utils.config.config_loader import load_config_toml
from utils.data.paths import (
    NSE_INDX_DATA,
    NSE_INTRADAY_DIR_PATH,
    NSE_LOGS_DIR,
    REMOTE_INTRADAY_DIR_PATH,
    REMOTE_NSE_INDX_DATA,
    ROOT_DATA_DIR,
)
from utils.logging.log_utils import (
    LogFileManager,
    out,
    set_logger_config,
    set_out_log_level,
)


def _sync_with_rsync(
    remote_host: str,
    remote_root_path: str,
    remote_dir_paths: list[str],
    local_path: str,
    remote_src_flag: bool = True,
    user: str = "",
    port: int = 22,
    exclude_files: list | None = None,
) -> None:
    """
    Uses the system's native rsync command to synchronize
    remote data to a local directory.
    Assumes SSH keys are configured for passwordless entry.
    """
    ssh_command = f"ssh -p {port}"
    hostname = f"{user}@{remote_host}" if user else remote_host
    remote_root_path_obj = Path(remote_root_path)

    if remote_src_flag:
        remote_root_path_obj = PurePosixPath(remote_root_path)

        # Escape any commas or braces in directory names (rare, but makes it robust)

        if remote_dir_paths is None or len(remote_dir_paths) == 0:
            remote_dir_paths = [""]

        if len(remote_dir_paths) == 1:
            # If joining yields same as remote_root_path_obj,
            # add trailing slash to mean the dir and not its contents
            remote_sub_path = remote_dir_paths[0]
            joined_path = remote_root_path_obj / remote_sub_path
            # Handle both Path and PurePosixPath
            base_path_str = str(remote_root_path_obj)
            joined_path_str = str(joined_path)
            if joined_path_str == base_path_str:
                src = f"{hostname}:{joined_path_str}/"
            else:
                src = f"{hostname}:{joined_path_str}"

        else:
            escaped_dirs = [
                p.replace("\\", "\\\\")
                .replace(",", "\\,")
                .replace("{", "\\{")
                .replace("}", "\\}")
                for p in remote_dir_paths
            ]

            brace_list = ",".join(escaped_dirs)
            src = f"{hostname}:{remote_root_path_obj.as_posix()}/{{{brace_list}}}"

        srcs = [src]
        dst = local_path
    else:
        srcs = [local_path]
        if len(remote_dir_paths) != 1:
            err_msg = (
                "When syncing local to remote, only a single remote_path is allowed."
            )
            raise ValueError(err_msg)

        dst = f"{hostname}:{(remote_root_path_obj / remote_dir_paths[0]).as_posix()}"

    # Construct --exclude arguments from exclude_files list
    exclude_args = []
    if exclude_files:
        for pattern in exclude_files:
            exclude_args.extend(["--exclude", pattern])

    command = [
        "rsync",
        "-az",
        "--out-format=%n",
        *exclude_args,
        "-e",
        ssh_command,
        *srcs,
        dst,
    ]

    # Use shlex.join to show exactly how the command is being escaped/quoted
    out(f"Executing: {shlex.join(command)}\n", log_level="debug")

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Redirects stderr to stdout
            universal_newlines=True,
            bufsize=1,  # Line buffered
        )

        # Collect all output at once, do not stream
        stdout, _ = process.communicate()
        if stdout:
            out("Transfer Files->")
            out(stdout)

        if process.returncode == 0:
            out("✅ Rsync transfer completed successfully!")
        else:
            out(f"❌ Rsync failed with return code {process.returncode}")

    except FileNotFoundError:
        out("Error: 'rsync' command not found.")
    except (OSError, subprocess.SubprocessError) as e:
        out(f"Exception occurred during rsync: {e}")


def sync_with_rclone(src: str, dst: str) -> None:
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


def rsync_data(remote_dir_paths: list[str] | None = None) -> None:
    config: dict[str, Any] = load_config_toml(
        Path(__file__).parent / f"{Path(__file__).stem}.toml"
    )

    sync_config: dict[str, Any] = config.get("rsync", {})

    remote_host: str = str(sync_config.get("remote_host", ""))
    remote_user: str = str(sync_config.get("remote_user", ""))
    remote_root_path: str = str(sync_config.get("remote_root_path", ""))
    exclude_files: list = sync_config.get("exclude_files", [])

    # Ensure default fallback is an empty list, not an empty string, for mypy checking
    cnfg_remote_dir_paths: list[str] = sync_config.get("remote_dir_paths", [])

    # Retrieve local path or fallback to default
    raw_local_root = sync_config.get("local_root_path", "")
    local_root_path: str = str(raw_local_root) if raw_local_root else str(ROOT_DATA_DIR)

    final_remote_dirs = (
        remote_dir_paths if remote_dir_paths is not None else cnfg_remote_dir_paths
    )

    _sync_with_rsync(
        remote_host=remote_host,
        user=remote_user,
        remote_root_path=remote_root_path,
        remote_dir_paths=final_remote_dirs,
        local_path=local_root_path,
        exclude_files=exclude_files,
    )


if __name__ == "__main__":
    # init log manager
    log_manager = LogFileManager(log_dir=NSE_LOGS_DIR)
    log_manager.start_monitor()
    script_log_name = Path(__file__).stem
    managed_file_handle = log_manager.open_log(script_log_name)
    set_logger_config(log_handle=managed_file_handle)
    set_out_log_level("critical")

    parser = argparse.ArgumentParser(description="NSE sync data")
    parser.add_argument(
        "-tr", "--to-remote", action="store_true", help="Sync to remote drive"
    )
    parser.add_argument(
        "-ix", "--index", action="store_true", help="Sync Index data to remote drive"
    )
    parser.add_argument(
        "-rc", "--rclone-flag", action="store_true", help="Use rclone instead of rsync"
    )

    args, unknown = parser.parse_known_args()
    to_remote: bool = False
    index_flag: bool = False

    if args.to_remote:
        out("Sync to remote drive")
        to_remote = True

    if args.index:
        out("Sync Index Dir")
        index_flag = True

    try:
        if args.rclone_flag:
            if to_remote:
                if not index_flag:
                    sync_with_rclone(
                        str(NSE_INTRADAY_DIR_PATH), str(REMOTE_INTRADAY_DIR_PATH)
                    )
                else:
                    sync_with_rclone(str(NSE_INDX_DATA), str(REMOTE_NSE_INDX_DATA))
            else:
                sync_with_rclone(
                    str(REMOTE_INTRADAY_DIR_PATH), str(NSE_INTRADAY_DIR_PATH)
                )
        else:
            rsync_data()
    finally:
        # Guarantee clean exit of file handlers
        log_manager.close_log(script_log_name)
        log_manager.stop_monitor()
