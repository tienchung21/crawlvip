#!/bin/bash
set -euo pipefail

BASE_DIR="/home/chungnt/crawlvip"
PYTHON_BIN="$BASE_DIR/venv/bin/python"
CRAWLER="$BASE_DIR/craw/homedy_crawler.py"
LOG_DIR="$BASE_DIR/craw/logs"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/homedy_daily_$(date +%F).log"

{
  echo "==== [$(date '+%Y-%m-%d %H:%M:%S')] START homedy daily ===="
  "$PYTHON_BIN" "$CRAWLER" \
    --page-size 200 \
    --delay 0.2 \
    --max-retries 4 \
    --dup-stop-threshold 200
  echo "==== [$(date '+%Y-%m-%d %H:%M:%S')] END homedy daily ===="
} >> "$LOG_FILE" 2>&1

