import logging
import sys
import time
from datetime import datetime, timedelta

import tomllib
from pytz import timezone as _pytz_timezone

from utils.data.paths import ROOT_SRC_DIR_PATH_OBJ

INDIA_TZ = _pytz_timezone("Asia/Kolkata")

_pyproject = {}
with (ROOT_SRC_DIR_PATH_OBJ / "pyproject.toml").open("rb") as f:
    _pyproject = tomllib.load(f)

PROJECT_NAME = _pyproject.get("project", {}).get("name", __name__)
BUFFER_SECONDS = 5
LOGGER = logging.getLogger(PROJECT_NAME)


def set_logger_config(log_level: str = "", log_handle: object = sys.stdout) -> None:
    numeric_level = logging.getLevelNamesMapping().get(
        log_level.upper(), logging.CRITICAL
    )
    logging.basicConfig(
        level=numeric_level,
        format="%(message)s",
        handlers=[logging.StreamHandler(log_handle)],
        force=True,
    )


set_logger_config()


def out(msg: str = "", end: str = "\n") -> None:
    if end == "\n":
        end = ""

    LOGGER.info("%s%s", msg, end)


def next_wall_clock(interval_min: int, buf: int = BUFFER_SECONDS) -> datetime:
    """Calculates the next wall-clock target time."""
    now = datetime.now(INDIA_TZ)

    # Calculate the next minute that is a multiple of the interval
    next_minute_multiple = ((now.minute // interval_min) + 1) * interval_min

    # Anchor to the top of the hour.
    # timedelta naturally handles rolling over to the next hour if minutes >= 60.
    base = now.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(minutes=next_minute_multiple, seconds=buf)


def wait_next_wall_clock(interval_min: int, buf: int = BUFFER_SECONDS) -> None:
    """Waits until the next wall-clock multiple, strictly ensuring a positive sleep."""
    # Failsafe to prevent 0-minute infinite loops
    if not interval_min or interval_min <= 0:
        time.sleep(10)
        return

    target = next_wall_clock(interval_min, buf)
    wait = (target - datetime.now(INDIA_TZ)).total_seconds()

    # Failsafe: If a negative buffer pushes the target time into the past,
    # advance by one full interval to guarantee a future wake-up time.
    if wait <= 0:
        target += timedelta(minutes=interval_min)
        wait = (target - datetime.now(INDIA_TZ)).total_seconds()

    out(f"\n  Next scan at {target.strftime('%H:%M:%S')}  ({wait:.0f}s)")

    if wait > 0:
        time.sleep(wait)
