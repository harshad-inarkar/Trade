#!/bin/bash

# ─────────────────────────────────────────────────────────────────────────────
# Path Helper & Environment
# ─────────────────────────────────────────────────────────────────────────────
if [ -x /usr/libexec/path_helper ]; then
    eval `/usr/libexec/path_helper -s`
fi

# Ensure the script runs in its own directory (critical for Cron)
cd "$(dirname "$0")" || exit 1

# [!] IMPORTANT FOR CRON: 
# If you are using a virtual environment, uncomment the line below and point it to your venv
# source /path/to/your/project/venv/bin/activate

# ─────────────────────────────────────────────────────────────────────────────
# Dynamic Configuration via Python
# ─────────────────────────────────────────────────────────────────────────────
# Ask the Python project exactly where the log directory is located
NSE_LOGS_DIR=$(python -c "from utils.data.paths import NSE_LOGS_DIR; print(NSE_LOGS_DIR)")

# Ensure the directory actually exists before trying to write to it
mkdir -p "$NSE_LOGS_DIR"

MAX_SIZE_KB=100
LOG_FILE="nse_downloader.log"
LOG_FILE_PATH="$NSE_LOGS_DIR/$LOG_FILE"
DOWNLOAD=true

# ─────────────────────────────────────────────────────────────────────────────
# Log Rotation (Safe Truncation)
# ─────────────────────────────────────────────────────────────────────────────
# Check file size safely before we start redirecting stdout/stderr
if [ -f "$LOG_FILE_PATH" ]; then
    # macOS uses stat -f %z. (If you ever move to Linux, change to: stat -c %s)
    FILE_SIZE_BYTES=$(stat -f %z "$LOG_FILE_PATH")
    FILE_SIZE_KB=$((FILE_SIZE_BYTES / 1024))

    if [ "$FILE_SIZE_KB" -gt "$MAX_SIZE_KB" ]; then
        # Use truncation (>) instead of rm so we don't break file descriptors
        > "$LOG_FILE_PATH"
        echo "[System] Log exceeded ${MAX_SIZE_KB} KB. Auto-truncated." >> "$LOG_FILE_PATH"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# Execution
# ─────────────────────────────────────────────────────────────────────────────
# Redirect all following standard output and errors into the log file
exec >> "$LOG_FILE_PATH" 2>&1

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Start: $(date)"

if [ "$DOWNLOAD" = true ]; then
    # Run the object-oriented downloader
    export log_level="info"
    python3 nse_daily_data_downloader.py
fi

echo "Finish: $(date)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"