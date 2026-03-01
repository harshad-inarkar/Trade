#!/bin/bash

 ########
#Source the system PATH helper (mimics /etc/profile behavior)
if [ -x /usr/libexec/path_helper ]; then
    eval `/usr/libexec/path_helper -s`
fi

MAX_SIZE_KB='10'
nse_logs_dir="../../nse_data/logs"
log_file="nse_remote_sync.log"

log_file_path="$nse_logs_dir/$log_file"



cd "$(dirname "$0")"
exec >> "$log_file_path" 2>&1

echo "######"
# Check if file exists
if [ ! -f "$log_file_path" ]; then
    echo "Error: File '$log_file_path' does not exist."
fi

# Get file size in KB (bytes / 1024)
FILE_SIZE_KB=$(($(stat -f %z "$log_file_path") / 1024))

if [ "$FILE_SIZE_KB" -gt "$MAX_SIZE_KB" ]; then
    rm "$log_file_path"
    echo "Deleted $log_file_path (size: ${FILE_SIZE_KB} KB > ${MAX_SIZE_KB} KB)."
else
    echo "$log_file_path size is ${FILE_SIZE_KB} KB, within limit ${MAX_SIZE_KB} "
fi


echo "######"
echo "$(date)"

python sync_data.py