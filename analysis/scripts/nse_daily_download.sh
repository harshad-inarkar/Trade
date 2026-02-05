#!/bin/bash

 ########
#Source the system PATH helper (mimics /etc/profile behavior)
if [ -x /usr/libexec/path_helper ]; then
    eval `/usr/libexec/path_helper -s`
fi

##########

nse_logs_dir="nse_data/logs"
log_file="nse_downloader.log"
lastn="5"
topk="20"
final_cands="60"
download=true
intraday=false
ftp_range="600-6000"


usage() {
  echo "Usage: $0 -lo <log_file> -ln <lastn> -tk <topk> -fc <final_cands> -dl <download> -id <intraday> -fr <ftp_range>"
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
    -ln)
      lastn="$2"
      shift 2
      ;;
    -tk)
      topk="$2"
      shift 2
      ;;
   -fc)
      final_cands="$2"
      shift 2
      ;;
   -dl)
      download="$(parse_bool "$2")"
      shift 2
      ;;
   -id)
      intraday="$(parse_bool "$2")"
      shift 2
      ;;
   -fr)
      ftp_range="$2"
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


intraday_flag='0'
# Use as real boolean
if $intraday; then
  intraday_flag='1'
else
  intraday_flag='0'
fi

log_file_path="$nse_logs_dir/$log_file"

####################


cd /Users/harshad/Documents/trade/analysis
exec > "$log_file_path" 2>&1

echo "######"
echo "$(date)"


if $download; then
	python nse_daily_data_downloader.py $intraday_flag
fi

if $intraday; then 
    echo 'Intraday'
else
    echo 'Run generate candidates scripts.'
    python generate_top_percentile.py $final_cands $lastn $topk $ftp_range o 0
    python generate_top_percentile.py $final_cands $lastn $topk $ftp_range f 1
    python dedup.py out/merged.txt out/candidates.txt $final_cands
fi
