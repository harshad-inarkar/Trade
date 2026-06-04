#!/usr/bin/env python3
"""
NSE Daily Data Downloader (Object-Oriented)
Downloads intraday CSV reports from NSE and syncs with GCP.
"""

import csv
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import requests
import tomllib

# ─── Custom Imports ───────────────────────────────────────────────────────────
from utils.data.paths import NSE_INTRADAY_DIR_PATH, REMOTE_INTRADAY_DIR_PATH
from utils.data.sync_data import sync_data_args


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration Data Class
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class DownloaderConfig:
    start_session: str = "0915"
    end_session: str = "1530"
    reset_remote_sched: bool = False
    gcp_state_filename: str = ".gcp_state"
    session_url: str = "https://www.nseindia.com"
    refer_url: str = "https://www.nseindia.com/market-data/most-active-underlying"
    api_url: str = (
        "https://www.nseindia.com/api/live-analysis-most-active-underlying?csv=true"
    )

    @classmethod
    def load_from_toml(cls, path: Path) -> "DownloaderConfig":
        if not path.exists():
            print(f"[!] Config file {path.name} not found. Using defaults.")
            return cls()

        with open(path, "rb") as f:
            data = tomllib.load(f)

        c = cls()
        session = data.get("session", {})
        c.start_session = session.get("start", c.start_session)
        c.end_session = session.get("end", c.end_session)

        gcp = data.get("gcp", {})
        c.reset_remote_sched = gcp.get("reset_remote_sched", c.reset_remote_sched)
        c.gcp_state_filename = gcp.get("gcp_state_filename", c.gcp_state_filename)

        api = data.get("api", {})
        c.session_url = api.get("session_url", c.session_url)
        c.refer_url = api.get("refer_url", c.refer_url)
        c.api_url = api.get("api_url", c.api_url)

        return c


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main Downloader Class
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class NSEDailyDownloader:
    COLUMN_MAP = [
        ("Symbol", "symbol"),
        ("Volume (Contracts) - Futures", "vol_cum"),
        ("Value (₹ Lakhs) - Futures", "vol_val_cum"),
        ("Underlying", "price"),
    ]
    OUTPUT_COLUMNS = [out for _, out in COLUMN_MAP]

    def __init__(self, config_filename="nse_daily_downloader_config.toml"):
        self.base_dir = Path(__file__).parent
        self.config = DownloaderConfig.load_from_toml(self.base_dir / config_filename)
        self.gcp_state_file = self.base_dir / self.config.gcp_state_filename

    # ─── Time & Session Management ─────────────────────────────────────────────

    def _calculate_intervals(
        self,
        tf=1,
        start_time_str=None,
        end_time_str=None,
    ) -> float:
        start_str = start_time_str or self.config.start_session
        end_str = end_time_str or self.config.end_session

        start = datetime.strptime(start_str, "%H%M")
        end = datetime.strptime(end_str, "%H%M")

        if start >= end:
            return 0

        total_duration = (end - start).total_seconds() / 60
        return total_duration // tf

    def _check_valid_session(self, curr_time: str) -> tuple[bool, str, bool]:
        interval = self._calculate_intervals(end_time_str=curr_time)
        valid_flag = interval > 0

        new_ts = curr_time
        time_exceeded = False

        # Check against the max allowable intervals (start_session to end_session)
        max_intervals = self._calculate_intervals()

        if valid_flag and interval > max_intervals:
            print(
                f"Current timestamp {curr_time} passed end session time {self.config.end_session}",
            )
            new_ts = self.config.end_session
            time_exceeded = True

        return valid_flag, new_ts, time_exceeded

    # ─── GCP Scheduler Management ──────────────────────────────────────────────

    def _read_gcp_state(self) -> str:
        if self.gcp_state_file.exists():
            try:
                with open(self.gcp_state_file) as f:
                    return f.read().strip()
            except Exception as ex:
                print(f"Warning: failed to read .gcp_state: {ex}")
        return ""

    def _save_gcp_state(self, gcp_state: str):
        try:
            with open(self.gcp_state_file, "w") as f:
                f.write(gcp_state)
        except Exception as ex:
            print(f"Warning: failed to write .gcp_state: {ex}")

    def _reset_gcp_sched(self, to_state: str):
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "scheduler",
                    "jobs",
                    to_state,
                    "nse-downloader-function-job",
                    "--location=asia-south1",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            print(f"✅ {to_state}d GCP scheduler job:\n{result.stdout.strip()}")
            self._save_gcp_state(to_state)
        except Exception as ex:
            print(f"Error in {to_state} GCP scheduler job: {ex}")

    def _handle_gcp_sync(self, time_exceeded_flag: bool):
        if not self.config.reset_remote_sched:
            return

        gcp_state = self._read_gcp_state()

        if not time_exceeded_flag:
            # Pause the Cloud Scheduler job if it is active
            should_pause = not gcp_state or gcp_state == "resume"
            if should_pause:
                print("Sync from remote to local")
                sync_data_args(REMOTE_INTRADAY_DIR_PATH, NSE_INTRADAY_DIR_PATH)
                self._reset_gcp_sched("pause")
            else:
                print("Scheduler pause skipped: gcp_state is not resumed")
        else:
            # Resume it if it is paused
            should_resume = not gcp_state or gcp_state == "pause"
            if should_resume:
                self._reset_gcp_sched("resume")
                print("Sync from local to remote")
                sync_data_args(NSE_INTRADAY_DIR_PATH, REMOTE_INTRADAY_DIR_PATH)
            else:
                print("Scheduler resume skipped: gcp_state is not 'pause'")

    # ─── Core Download Logic ───────────────────────────────────────────────────

    def download(self):
        t1 = time.time()

        # Calculate safer timestamp (adds 30 seconds to round up to nearest minute accurately)
        now = datetime.now() + timedelta(seconds=30)
        timestamp = now.strftime("%H%M")

        date_timestamp = datetime.now().strftime("%d%m%Y")
        data_dir = f"{NSE_INTRADAY_DIR_PATH}/{date_timestamp}"

        valid_flag, timestamp, time_exceeded_flag = self._check_valid_session(timestamp)

        if not valid_flag:
            print(f"Not Valid timestamp {timestamp}. Ignore download")
            return

        os.makedirs(data_dir, exist_ok=True)

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": self.config.refer_url,
        }

        # Initialize session and fetch cookies
        session = requests.Session()
        session.get(self.config.session_url, headers=headers)

        # Fetch actual CSV data
        try:
            response = session.get(self.config.api_url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to download data from NSE: {e}")
            return

        # Read CSV from response, filter and rename columns in map order
        content_str = response.content.decode(encoding="utf-8-sig")
        reader = csv.DictReader(StringIO(content_str))

        filtered_rows = []
        for row in reader:
            filtered_row = {}
            for src, out in self.COLUMN_MAP:
                if src in row:
                    val = row[src]
                    if out == "vol_val_cum":
                        try:
                            val = f"{float(val):.2f}"
                        except ValueError:
                            pass
                    filtered_row[out] = val

            if len(filtered_row) == len(self.COLUMN_MAP):
                filtered_rows.append(filtered_row)

        file_path = f"{data_dir}/nse_data_{timestamp}.csv"

        # Write to local CSV
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.OUTPUT_COLUMNS)
            writer.writeheader()
            writer.writerows(filtered_rows)

        print(f"Downloaded: {file_path} in {time.time() - t1:.2f}s")

        # Post-download GCP Sync operations
        self._handle_gcp_sync(time_exceeded_flag)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Execution Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    downloader = NSEDailyDownloader()

    if downloader.config.reset_remote_sched:
        print("Reset Remote Sched Config is ACTIVE")

    downloader.download()
