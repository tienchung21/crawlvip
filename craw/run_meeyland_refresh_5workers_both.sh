#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/chungnt/crawlvip"
CRAW="$ROOT/craw"
PY="$ROOT/venv/bin/python"
SCRIPT="$CRAW/meeymap_search_crawler.py"
LOG_DIR="$CRAW/logs"

LIMIT="${LIMIT:-30}"
DELAY="${DELAY:-1.2}"
THRESHOLD="${THRESHOLD:-20000}"
TABLE="${LOCATION_TABLE:-location_meeland}"
LEVEL="${LOCATION_LEVEL:-district}"

SALE_CAT="5deb722db4367252525c1d00"
RENT_CAT="5deb722db4367252525c1d11"

mkdir -p "$LOG_DIR"

for idx in 1 2 3 4 5; do
  wlog="$LOG_DIR/meeyland_refresh_both_w${idx}.log"
  sale_chk="$LOG_DIR/meeyland_refresh_sale_w${idx}.checkpoint.json"
  sale_jsonl="$LOG_DIR/meeyland_refresh_sale_w${idx}.jsonl"
  rent_chk="$LOG_DIR/meeyland_refresh_rent_w${idx}.checkpoint.json"
  rent_jsonl="$LOG_DIR/meeyland_refresh_rent_w${idx}.jsonl"

  nohup setsid bash -lc "
    $PY $SCRIPT \
      --use-location-table \
      --location-table '$TABLE' \
      --location-level '$LEVEL' \
      --auto-split-threshold '$THRESHOLD' \
      --worker-total 5 \
      --worker-index '$idx' \
      --category '$SALE_CAT' \
      --limit '$LIMIT' \
      --delay '$DELAY' \
      --dup-stop-threshold 0 \
      --refresh-existing-contact \
      --checkpoint-file '$sale_chk' \
      --log-jsonl '$sale_jsonl' \
      --log-file '$wlog'

    $PY $SCRIPT \
      --use-location-table \
      --location-table '$TABLE' \
      --location-level '$LEVEL' \
      --auto-split-threshold '$THRESHOLD' \
      --worker-total 5 \
      --worker-index '$idx' \
      --category '$RENT_CAT' \
      --limit '$LIMIT' \
      --delay '$DELAY' \
      --dup-stop-threshold 0 \
      --refresh-existing-contact \
      --checkpoint-file '$rent_chk' \
      --log-jsonl '$rent_jsonl' \
      --log-file '$wlog'
  " < /dev/null >> "$wlog" 2>&1 &
  pid=$!
  disown "$pid" 2>/dev/null || true
  echo "started worker $idx pid=$pid log=$wlog"
done

echo "done"
