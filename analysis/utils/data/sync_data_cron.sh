#!/bin/bash

source ~/venv/bin/activate

# Ensure the script runs in its own directory (critical for Cron)
cd "$(dirname "$0")" || exit 1
python sync_data.py

