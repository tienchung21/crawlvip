#!/bin/bash
# Move to project root
cd /home/chungnt/crawlvip
source venv/bin/activate

# Stop existing processes
echo "Stopping existing upload processes..."
pkill -f ftp_image_processor.py || true
pkill -f listing_uploader.py || true
pkill -f run_mogi_etl.py || true
pkill -f convert_alonhadat_to_data_full.py || true
pkill -f convert_guland_to_data_full.py || true
pkill -f update_project_id.py || true
sleep 2

# Start Mogi ETL (Loop)
echo "Starting Mogi ETL (Loop)..."
nohup bash -c "while true; do python3 craw/auto/run_mogi_etl.py; sleep 60; done" > mogi_etl.log 2>&1 &
PID_ETL=$!
echo "Mogi ETL started with PID $PID_ETL"

# Start Alonhadat ETL (Loop)
echo "Starting Alonhadat ETL (Loop)..."
nohup bash -c "while true; do python3 craw/auto/convert_alonhadat_to_data_full.py --preview-limit 500 --insert; sleep 60; done" > alonhadat_etl.log 2>&1 &
PID_ALO=$!
echo "Alonhadat ETL started with PID $PID_ALO"

# Start Guland ETL (Loop)
echo "Starting Guland ETL (Loop)..."
nohup bash -c "while true; do python3 craw/auto/convert_guland_to_data_full.py --preview-limit 500 --insert; sleep 60; done" > guland_etl.log 2>&1 &
PID_GULAND=$!
echo "Guland ETL started with PID $PID_GULAND"

# Start Project ID Backfill (Loop)
# Keep data_full.project_id updated for newly inserted rows
echo "Starting Project ID Backfill (Loop)..."
nohup bash -c "while true; do python3 craw/update_project_id.py --new-only --batch 2000; sleep 60; done" > project_id_backfill.log 2>&1 &
PID_PROJECT_ID=$!
echo "Project ID Backfill started with PID $PID_PROJECT_ID"

# Start FTP Image Processor (Loop)
# Runs in a loop because the script itself processes a batch and exits
echo "Starting FTP Image Processor (Loop)..."
nohup bash -c "while true; do python3 craw/ftp_image_processor.py --batch 100 --limit 500; sleep 5; done" > ftp_processor.log 2>&1 &
PID_FTP=$!
echo "FTP Image Processor started with PID $PID_FTP"

# Start Listing Uploader (Continuous)
echo "Starting Listing Uploader..."
nohup python3 craw/listing_uploader.py --continuous --limit 0 --workers 1 > listing_uploader.log 2>&1 &
PID_LISTING=$!
echo "Listing Uploader started with PID $PID_LISTING"

echo "All services started."
