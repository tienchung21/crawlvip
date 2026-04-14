#!/bin/bash

# Navigate to project directory
cd /home/chungnt/crawlvip

# Activate virtual environment (assuming it's in venv or .venv, or using system python if not)
# Based on previous scripts, user uses /usr/bin/python3 or a specific venv. 
# `daily_mogi_crawl.sh` uses `source venv/bin/activate`. I should check that.

source venv/bin/activate

# Run the script
python3 craw/auto/daily_nhatot_api.py >> craw/auto/daily_nhatot_api.log 2>&1

echo "Finished daily Nhatot API crawl at $(date)" >> craw/auto/daily_nhatot_api.log
