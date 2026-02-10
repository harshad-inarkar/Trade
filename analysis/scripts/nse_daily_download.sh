#!/bin/bash

 ########
#Source the system PATH helper (mimics /etc/profile behavior)
if [ -x /usr/libexec/path_helper ]; then
    eval `/usr/libexec/path_helper -s`
fi

##########

MAX_SIZE_KB='10'
nse_logs_dir="../nse_data/logs"
log_file="nse_downloader.log"
download=true


usage() {
  echo "Usage: $0 -lo <log_file>  -dl <download>"
  exit 1
}

# Parse arguments

#######################################
# Convert boolean
#######################################

parse_bool() {
  local val
  # Convert to lowercase (portable)
  val="$(echo "$1" | tr '[:upper:]' '[:lower:]')"

  case "$val" in
    1|true|yes|y|on)
      echo "true"
      ;;
    0|false|no|n|off)
      echo "false"
      ;;
    *)
      echo "ERROR: Invalid boolean value: $1" >&2
      exit 1
      ;;
  esac
}


while [[ $# -gt 0 ]]; do
  case "$1" in
    -lo)
      log_file="$2"
      shift 2
      ;;
   -dl)
      download="$(parse_bool "$2")"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done


log_file_path="$nse_logs_dir/$log_file"

####################

cd /Users/harshad/Documents/trade/analysis/scripts/
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



if $download; then
	python nse_daily_data_downloader.py
fi

