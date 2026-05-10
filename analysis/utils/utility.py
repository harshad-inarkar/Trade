
from datetime import datetime, timedelta
import time

BUFFER_SECONDS = 5


def next_wall_clock(interval_min: int, buf: int = BUFFER_SECONDS) -> datetime:
    now  = datetime.now()
    slot = (now.minute // interval_min + 1) * interval_min
    base = now.replace(second=0, microsecond=0)
    return (
        base + timedelta(minutes=(slot - now.minute)) if slot < 60
        else base.replace(minute=0) + timedelta(hours=1)
    ) + timedelta(seconds=buf)


def wait_next_wall_clock(interval_min: int, buf: int = BUFFER_SECONDS):
    target = next_wall_clock(interval_min, buf)
    wait   = (target - datetime.now()).total_seconds()
    print(f"\n  Next scan at {target.strftime('%H:%M:%S')}  ({wait:.0f}s)")
    time.sleep(max(0, wait))