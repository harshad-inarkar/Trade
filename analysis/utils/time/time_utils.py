import time
from datetime import datetime, timedelta

from pytz import timezone as _pytz_timezone

from utils.logging.log_utils import out

INDIA_TZ = _pytz_timezone("Asia/Kolkata")


_buffer_seconds = 5


def next_wall_clock(interval_min: int, buf: int = _buffer_seconds) -> datetime:
    """Calculates the next wall-clock target time."""
    now = datetime.now(INDIA_TZ)

    # Calculate the next minute that is a multiple of the interval
    next_minute_multiple = ((now.minute // interval_min) + 1) * interval_min

    # Anchor to the top of the hour.
    # timedelta naturally handles rolling over to the next hour if minutes >= 60.
    base = now.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(minutes=next_minute_multiple, seconds=buf)


def wait_next_wall_clock(interval_min: int, buf: int = _buffer_seconds) -> None:
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
