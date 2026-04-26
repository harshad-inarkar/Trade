import sys
import csv
import argparse
import os
from datetime import datetime, timedelta

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QTableWidget, QTableWidgetItem, QHeaderView
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor

# ── Configuration ─────────────────────────────────────────────────────────────
# DISPLAY_FIELDS = ['symbol', 'volume_fast','vol_surge','price_surge','ltp']
DISPLAY_FIELDS = ['symbol', 'ltp']

SIZE_PERCENT   = 75    
BUFFER_SECONDS = 18
     

# ── Environment Setup ────────────────────────────────────────────────────────
from sys import path as _syspath
_syspath.append(os.path.abspath("../../")) 
from web_scripts.data_scripts.sync_data import OUT_DIR
CSV_PATH = os.path.join(OUT_DIR, 'sym_table.csv')

# ── CLI args ──────────────────────────────────────────────────────────────────
_parser = argparse.ArgumentParser()
_parser.add_argument("-ri", "--reload-interval", type=float, default=0)
_parser.add_argument("-dc", "--display-count", type=int, default=10)

_ARGS = _parser.parse_args()
DISPLAY_COUNT = _ARGS.display_count


# ── Colors ────────────────────────────────────────────────────────────────────
POSITIVE_BG = QColor("#1a3d2b")
POSITIVE_FG = QColor("#4cde8a")
NEGATIVE_BG = QColor("#3d1a1a")
NEGATIVE_FG = QColor("#ff6b6b")

class NumericTableItem(QTableWidgetItem):
    def __init__(self, display_text: str, sort_value: float):
        super().__init__(display_text)
        self._sort_value = sort_value

    def __lt__(self, other):
        if isinstance(other, NumericTableItem):
            return self._sort_value < other._sort_value
        return super().__lt__(other)

class CSVTableViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Market Monitor")
        self._price_surge_idx = -1
        
        self._apply_theme()
        self._build_ui()
        self._load_data()
        
        if _ARGS.reload_interval > 0:
            self._schedule_reload()

    def _apply_theme(self):
        s = SIZE_PERCENT / 100
        f_size = max(8, int(13 * s))
        self.setStyleSheet(f"""
            QMainWindow, QWidget {{ background-color: #11111b; color: #cdd6f4; font-family: Helvetica Neue; }}
            QTableWidget {{
                background-color: #181825; alternate-background-color: #1e1e2e;
                gridline-color: #313244; border: none; font-size: {f_size}px;
            }}
            QHeaderView::section {{
                background-color: #181825; color: #89b4fa; font-weight: 700;
                border: none; border-bottom: 2px solid #89b4fa; font-size: {f_size}px;
            }}
        """)

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(5, 5, 5, 5)
        
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

    def _schedule_reload(self):
        now = datetime.now()
        interval_sec = _ARGS.reload_interval * 60
        
        # Calculate seconds since midnight
        passed = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
        
        # Find next "clean" slot based on -ri
        next_slot = ((passed // interval_sec) + 1) * interval_sec
        next_time = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=next_slot + BUFFER_SECONDS)
        
        ms = int((next_time - now).total_seconds() * 1000)
        QTimer.singleShot(ms, self._reload_callback)

    def _reload_callback(self):
        self._load_data()
        self._schedule_reload()

    def _load_data(self):
        if not os.path.exists(CSV_PATH): return

        try:
            filtered_rows = []
            with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Map the raw CSV keys to our target DISPLAY_FIELDS
                    cleaned_row = {k.strip(): v.strip() for k, v in row.items()}
                    filtered_rows.append({f: cleaned_row.get(f, "") for f in reader.fieldnames})
            
            self._price_surge_idx = reader.fieldnames.index('price_surge') if 'price_surge' in reader.fieldnames else -1
            self._populate(filtered_rows)
        except Exception as e:
            print(f"❌ Error: {e}")

    def _populate(self, rows):
        n_rows = min(DISPLAY_COUNT,len(rows))
        rows = rows[:n_rows]
        self.table.setSortingEnabled(False)
        self.table.setRowCount(n_rows)
        self.table.setColumnCount(len(DISPLAY_FIELDS))
        self.table.setHorizontalHeaderLabels([f.upper() for f in DISPLAY_FIELDS])

        

        for r_idx, row in enumerate(rows):
            # Parse price_surge for coloring
            surge_val = 0.0
            try:
                surge_val = float(row['price_surge'].replace("%", "").replace(",", ""))
            except: pass

            for c_idx, field in enumerate(DISPLAY_FIELDS):
                val = row[field]
                item = self._make_item(val)

                # Row coloring logic
                if surge_val > 0:
                    item.setBackground(POSITIVE_BG if r_idx % 2 == 0 else QColor("#1c3326"))
                    if c_idx == self._price_surge_idx: item.setForeground(POSITIVE_FG)
                elif surge_val < 0:
                    item.setBackground(NEGATIVE_BG if r_idx % 2 == 0 else QColor("#331c1c"))
                    if c_idx == self._price_surge_idx: item.setForeground(NEGATIVE_FG)

                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(r_idx, c_idx, item)

        if SIZE_PERCENT != 100:
            h = max(16, int(24 * SIZE_PERCENT / 100))
            for i in range(self.table.rowCount()): self.table.setRowHeight(i, h)
            
        self.table.setSortingEnabled(True)

    def _make_item(self, value: str) -> QTableWidgetItem:
        clean = value.replace(",", "").replace("%", "").strip()
        try:
            return NumericTableItem(value, float(clean))
        except ValueError:
            return QTableWidgetItem(value)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CSVTableViewer()
    window.show()
    sys.exit(app.exec_())