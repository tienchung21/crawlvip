#!/bin/bash
cd /home/chungnt/crawlvip
source venv/bin/activate

echo "Stopping existing data_no_full services..."
pkill -f "ftp_image_processor.py --table data_no_full" || true
pkill -f "listing_uploader.py --table data_no_full" || true
pkill -f "refresh_upload_data_no_full_pretty.py" || true
sleep 2

echo "Refreshing upload_data_no_full_pretty.txt and DB count table..."
python3 craw/refresh_upload_data_no_full_pretty.py > upload_data_no_full_refresh.log 2>&1

echo "Starting data_no_full FTP image processor..."
nohup bash -c "while true; do python3 craw/ftp_image_processor.py --table data_no_full --batch 100 --limit 500; sleep 5; done" \
  > ftp_processor_data_no_full.log 2>&1 &
PID_FTP=$!
echo "data_no_full FTP processor started with PID $PID_FTP"

echo "Starting data_no_full null-contact uploader..."
nohup python3 craw/listing_uploader.py \
  --table data_no_full \
  --api-mode null-contact \
  --continuous \
  --limit 0 \
  --workers 1 \
  --area-filter-table upload_data_no_full_area_counts \
  --area-filter-max-total 10 \
  > listing_uploader_data_no_full.log 2>&1 &
PID_UPLOADER=$!
echo "data_no_full uploader started with PID $PID_UPLOADER"

echo "Starting nightly refresh scheduler..."
nohup bash -c '
last_run="";
while true; do
  now_date=$(date +%F)
  now_hour=$(date +%H)
  if [ "$now_hour" = "00" ] && [ "$last_run" != "$now_date" ]; then
    python3 /home/chungnt/crawlvip/craw/refresh_upload_data_no_full_pretty.py >> /home/chungnt/crawlvip/upload_data_no_full_refresh.log 2>&1
    last_run="$now_date"
  fi
  sleep 300
done
' > upload_data_no_full_scheduler.log 2>&1 &
PID_REFRESH=$!
echo "Nightly refresh scheduler started with PID $PID_REFRESH"

echo "All data_no_full services started."
