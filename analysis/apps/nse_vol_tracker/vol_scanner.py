import argparse
import contextlib
import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QApplication,
    QHeaderView,
    QMainWindow,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

# ── Environment Setup ────────────────────────────────────────────────────────
from utils.data.paths import OUT_DIR
from utils.logging.log_utils import out
from utils.time.time_utils import INDIA_TZ

# ── Configuration ─────────────────────────────────────────────────────────────
# DISPLAY_FIELDS = ['symbol', 'volume_fast','vol_surge','price_surge','ltp']
DISPLAY_FIELDS = ["symbol", "ltp"]

SIZE_PERCENT = 75
BUFFER_SECONDS = 18

CSV_PATH = str(Path(OUT_DIR) / "sym_table.csv")

# ── CLI args ──────────────────────────────────────────────────────────────────
_parser = argparse.ArgumentParser()
_parser.add_argument("-ri", "--reload-interval", type=float, default=3)
_parser.add_argument("-dc", "--display-count", type=int, default=10)

_ARGS = _parser.parse_args()
DISPLAY_COUNT = _ARGS.display_count


# ── Colors ────────────────────────────────────────────────────────────────────
POSITIVE_BG = QColor("#1a3d2b")
POSITIVE_FG = QColor("#4cde8a")
NEGATIVE_BG = QColor("#3d1a1a")
NEGATIVE_FG = QColor("#ff6b6b")


class NumericTableItem(QTableWidgetItem):
    def __init__(self, display_text: str, sort_value: float) -> None:
        super().__init__(display_text)
        self._sort_value = sort_value

    def __lt__(self, other: object) -> bool:
        if isinstance(other, NumericTableItem):
            return self._sort_value < other._sort_value
        return super().__lt__(other)


class CSVTableViewer(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Market Monitor")
        self._price_surge_idx = -1

        self._apply_theme()
        self._build_ui()
        self._load_data()

        if _ARGS.reload_interval > 0:
            self._schedule_reload()

    def _apply_theme(self) -> None:
        s = SIZE_PERCENT / 100
        f_size = max(8, int(13 * s))
        self.setStyleSheet(f"""
            QMainWindow, QWidget {{
                background-color: #11111b; color: #cdd6f4; font-family: Helvetica Neue;
            }}
            QTableWidget {{
                background-color: #181825; alternate-background-color: #1e1e2e;
                gridline-color: #313244; border: none; font-size: {f_size}px;
            }}
            QHeaderView::section {{
                background-color: #181825; color: #89b4fa; font-weight: 700;
                border: none; border-bottom: 2px solid #89b4fa; font-size: {f_size}px;
            }}
        """)

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(5, 5, 5, 5)

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSortingEnabled(True)
        horizontal_header = self.table.horizontalHeader()
        if horizontal_header is not None:
            horizontal_header.setSectionResizeMode(QHeaderView.Stretch)

        vert_hdr = self.table.verticalHeader()
        if vert_hdr:
            vert_hdr.setVisible(False)
        layout.addWidget(self.table)

    def _schedule_reload(self) -> None:
        now = datetime.now(INDIA_TZ)
        interval_sec = _ARGS.reload_interval * 60

        # Calculate seconds since midnight
        passed = (
            now - now.replace(hour=0, minute=0, second=0, microsecond=0)
        ).total_seconds()

        # Find next "clean" slot based on -ri
        next_slot = ((passed // interval_sec) + 1) * interval_sec
        next_time = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
            seconds=next_slot + BUFFER_SECONDS,
        )

        ms = int((next_time - now).total_seconds() * 1000)
        QTimer.singleShot(ms, self._reload_callback)

    def _reload_callback(self) -> None:
        self._load_data()
        self._schedule_reload()

    def _load_data(self) -> None:
        if not Path(CSV_PATH).exists():
            return

        try:
            filtered_rows = []
            with Path(CSV_PATH).open(newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    return
                for row in reader:
                    # Map the raw CSV keys to our target DISPLAY_FIELDS
                    cleaned_row = {k.strip(): v.strip() for k, v in row.items()}
                    filtered_rows.append(
                        {f: cleaned_row.get(f, "") for f in reader.fieldnames},
                    )

            self._price_surge_idx = (
                reader.fieldnames.index("price_surge")
                if "price_surge" in reader.fieldnames
                else -1
            )
            self._populate(filtered_rows)
        except Exception as e:  # noqa: BLE001
            out(f"❌ Error: {e}")

    def _populate(self, rows: list[dict]) -> None:
        n_rows = min(DISPLAY_COUNT, len(rows))
        rows = rows[:n_rows]
        self.table.setSortingEnabled(False)
        self.table.setRowCount(n_rows)
        self.table.setColumnCount(len(DISPLAY_FIELDS))
        self.table.setHorizontalHeaderLabels([f.upper() for f in DISPLAY_FIELDS])

        for r_idx, row in enumerate(rows):
            # Parse price_surge for coloring
            surge_val = 0.0
            with contextlib.suppress(ValueError):
                surge_val = float(row["price_surge"].replace("%", "").replace(",", ""))

            for c_idx, field in enumerate(DISPLAY_FIELDS):
                val = row[field]
                item = self._make_item(val)

                # Row coloring logic
                if surge_val > 0:
                    item.setBackground(
                        POSITIVE_BG if r_idx % 2 == 0 else QColor("#1c3326"),
                    )
                    if c_idx == self._price_surge_idx:
                        item.setForeground(POSITIVE_FG)
                elif surge_val < 0:
                    item.setBackground(
                        NEGATIVE_BG if r_idx % 2 == 0 else QColor("#331c1c"),
                    )
                    if c_idx == self._price_surge_idx:
                        item.setForeground(NEGATIVE_FG)

                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

                self.table.setItem(r_idx, c_idx, item)

        max_percent = 100
        if max_percent != SIZE_PERCENT:
            h = max(16, int(24 * SIZE_PERCENT / 100))
            for i in range(self.table.rowCount()):
                self.table.setRowHeight(i, h)

        self.table.setSortingEnabled(True)

    def _make_item(self, value: str) -> QTableWidgetItem:
        clean = value.replace(",", "").replace("%", "").strip()
        try:
            return NumericTableItem(value, float(clean))
        except ValueError:
            return QTableWidgetItem(value)


if __name__ == "__main__":
    qt_app = QApplication(sys.argv)
    out(f"Reload Interval {_ARGS.reload_interval}m and Buffer {BUFFER_SECONDS}s")
    window = CSVTableViewer()
    window.show()
    sys.exit(qt_app.exec_())
