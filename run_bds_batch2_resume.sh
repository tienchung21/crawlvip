#!/usr/bin/env bash
set -euo pipefail

cd /home/chungnt/crawlvip
source venv/bin/activate
mapfile -t URLS < tmp_bds_batch2_resume_urls.txt
exec ./venv/bin/python -u craw/bds_category_link_crawler.py "${URLS[@]}" --start-page 4 >> logs/bds_category_link_crawler_20260318_batch2_resume.log 2>&1
