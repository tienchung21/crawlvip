#!/bin/bash
set -euo pipefail

ROOT="/home/chungnt/crawlvip"
cd "$ROOT"
source venv/bin/activate
mkdir -p logs

CONVERT_PATTERN="craw/auto/convert_nhatot_to_data_no_full.py --batch-size 50 --loop-until-done"
FTP_PATTERN="craw/ftp_image_processor.py --table data_no_full --batch 500 --workers 5"
UPLOAD_PATTERN="craw/listing_uploader.py --table data_no_full --api-mode null-contact --area-filter-lt20 --continuous --workers 1"

echo "Stopping existing data_no_full processes..."
pkill -f "$CONVERT_PATTERN" || true
pkill -f "$FTP_PATTERN" || true
pkill -f "$UPLOAD_PATTERN" || true
sleep 2

echo "Starting data_no_full convert loop..."
nohup bash -lc "cd '$ROOT' && source venv/bin/activate && while true; do python3 -u craw/auto/convert_nhatot_to_data_no_full.py --batch-size 50 --loop-until-done; sleep 60; done" > "$ROOT/logs/convert_nhatot_to_data_no_full_loop.log" 2>&1 &
PID_CONVERT=$!
echo "Convert loop started with PID $PID_CONVERT"

echo "Starting data_no_full FTP loop (workers=5)..."
nohup bash -lc "cd '$ROOT' && source venv/bin/activate && while true; do python3 -u craw/ftp_image_processor.py --table data_no_full --batch 500 --workers 5; sleep 5; done" > "$ROOT/logs/ftp_image_processor_data_no_full_loop.log" 2>&1 &
PID_FTP=$!
echo "FTP loop started with PID $PID_FTP"

echo "Starting data_no_full listing uploader (null-contact, lt20, workers=1)..."
nohup bash -lc "cd '$ROOT' && source venv/bin/activate && python3 -u craw/listing_uploader.py --table data_no_full --api-mode null-contact --area-filter-lt20 --continuous --workers 1" > "$ROOT/logs/listing_uploader_data_no_full_lt20_loop.log" 2>&1 &
PID_UPLOAD=$!
echo "Listing uploader started with PID $PID_UPLOAD"

echo "All data_no_full services started."
echo "Logs:"
echo "  $ROOT/logs/convert_nhatot_to_data_no_full_loop.log"
echo "  $ROOT/logs/ftp_image_processor_data_no_full_loop.log"
echo "  $ROOT/logs/listing_uploader_data_no_full_lt20_loop.log"
