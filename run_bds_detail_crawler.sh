#!/usr/bin/env bash
set -euo pipefail

cd /home/chungnt/crawlvip
source venv/bin/activate
exec ./venv/bin/python3 -u craw/bds_detail_crawler.py >> logs/bds_detail_crawler.log 2>&1
