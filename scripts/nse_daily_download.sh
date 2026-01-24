#!/bin/bash
 
echo "######"
echo "$(date)"

########
#Source the system PATH helper (mimics /etc/profile behavior)
if [ -x /usr/libexec/path_helper ]; then
    eval `/usr/libexec/path_helper -s`
fi

##########

lastn="5"
topk="20"
final_cands="60"
download=true


if [ -n "$1" ]; then
    lastn=$1
fi

if [ -n "$2" ]; then
    topk=$2
fi

if [ -n "$3" ]; then
    download=false
fi

echo "Use last $lastn and top $topk"

cd /Users/harshad/Documents/trade/analysis

if $download; then
	python nse_data_downloader.py
fi

python generate_top_percentile.py $final_cands $lastn $topk 600-6000 o 0
python generate_top_percentile.py $final_cands $lastn $topk 600-6000 f 1
python dedup.py out/merged.txt out/candidates.txt $final_cands



