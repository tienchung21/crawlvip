#!/bin/bash
set -euo pipefail

BASE_DIR="/home/chungnt/crawlvip"
PYTHON_BIN="$BASE_DIR/venv/bin/python"
JOB="$BASE_DIR/craw/auto/guland/guland_daily_api_job.py"
LOG_DIR="$BASE_DIR/craw/logs"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/guland_daily_runner_$(date +%F).log"

{
  echo "==== [$(date '+%Y-%m-%d %H:%M:%S')] START guland daily ===="
  "$PYTHON_BIN" "$JOB" --mode all
  echo "==== [$(date '+%Y-%m-%d %H:%M:%S')] END guland daily ===="
} >> "$LOG_FILE" 2>&1

