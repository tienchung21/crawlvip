#!/usr/bin/env bash
cd /home/chungnt/crawlvip || exit 1
source venv/bin/activate || exit 1
SLUGS=$(cat /home/chungnt/crawlvip/tmp_guland_new_wards_20260319.slugs)
exec ./venv/bin/python -u craw/guland_link_crawler.py "$SLUGS" --max-pages 200 --stop-no-new-pages 5 --delay-min 9 --delay-max 12 >> /home/chungnt/crawlvip/logs/guland_new_wards_20260319.log 2>&1
