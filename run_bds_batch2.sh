#!/usr/bin/env bash
set -euo pipefail
cd /home/chungnt/crawlvip
source venv/bin/activate
mapfile -t URLS < tmp_bds_supplement_urls_20260318.txt
./venv/bin/python -u craw/bds_category_link_crawler.py "${URLS[@]}" --also-rent >> logs/bds_category_link_crawler_20260318_batch2.log 2>&1
