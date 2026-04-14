#!/usr/bin/env bash
set -euo pipefail
cd /home/chungnt/crawlvip
source venv/bin/activate
mapfile -t URLS < tmp_bds_new_wards_20260319.txt
exec ./venv/bin/python -u craw/bds_category_link_crawler.py "${URLS[@]}" >> logs/bds_new_wards_20260319.log 2>&1
