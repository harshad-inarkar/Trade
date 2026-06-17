import argparse
import subprocess
import time

from utils.data.paths import (
    NSE_INDX_DATA,
    NSE_INTRADAY_DIR_PATH,
    REMOTE_INTRADAY_DIR_PATH,
    REMOTE_NSE_INDX_DATA,
)
from utils.utility import out


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
