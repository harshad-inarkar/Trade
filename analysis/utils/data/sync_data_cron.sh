#!/bin/bash

 ########
 ─────────────────────────────────────────────────────────────────────────────
# Path Helper & Environment
# ─────────────────────────────────────────────────────────────────────────────
if [ -x /usr/libexec/path_helper ]; then
    eval `/usr/libexec/path_helper -s`
fi

# Ensure the script runs in its own directory (critical for Cron)
cd "$(dirname "$0")" || exit 1

python sync_data.py

