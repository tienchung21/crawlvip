#!/bin/bash
cd /home/chungnt/crawlvip
echo "Stopping existing data_no_full convert service..."
pkill -f "convert_nhatot_to_data_no_full.py --batch-size" || true
sleep 2

echo "Starting data_no_full auto convert loop..."
nohup /bin/bash -c 'while true; do /home/chungnt/crawlvip/venv/bin/python /home/chungnt/crawlvip/craw/auto/convert_nhatot_to_data_no_full.py --batch-size 500 --loop-until-done >> /home/chungnt/crawlvip/convert_data_no_full.log 2>&1; sleep 60; done' >/dev/null 2>&1 &
echo "Started. Log: /home/chungnt/crawlvip/convert_data_no_full.log"
