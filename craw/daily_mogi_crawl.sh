#!/bin/bash

# Mogi Daily Crawler Wrapper
# Run this script via cron at 8:00 AM

# Set project directory
PROJECT_DIR="/home/chungnt/crawlvip/craw"
VENV_ACTIVATE="/home/chungnt/crawlvip/venv/bin/activate"

# Navigate to project dir
cd "$PROJECT_DIR" || exit

# Activate Virtual Environment
if [ -f "$VENV_ACTIVATE" ]; then
    source "$VENV_ACTIVATE"
else
    echo "Error: Virtual environment not found at $VENV_ACTIVATE"
    exit 1
fi

# Run the crawler script
# Unset proxy to ensure fresh connection or use script defaults
unset http_proxy
unset https_proxy

echo "Starting Mogi Daily Crawl at $(date)" >> daily_mogi_cron.log
python daily_mogi_crawl.py >> daily_mogi_cron.log 2>&1
echo "Finished Mogi Daily Crawl at $(date)" >> daily_mogi_cron.log
