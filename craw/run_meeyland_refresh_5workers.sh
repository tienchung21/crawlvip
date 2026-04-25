#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/chungnt/crawlvip"
CRAW_DIR="$ROOT_DIR/craw"
PY="$ROOT_DIR/venv/bin/python"
SCRIPT="$CRAW_DIR/meeymap_search_crawler.py"
LOG_DIR="$CRAW_DIR/logs"

MODE="${1:-both}" # sale | rent | both
LIMIT="${LIMIT:-30}"
DELAY="${DELAY:-1.2}"
TABLE="${LOCATION_TABLE:-location_meeland}"
LEVEL="${LOCATION_LEVEL:-district}"
RESUME="${RESUME:-0}"

mkdir -p "$LOG_DIR"

start_group() {
  local category="$1"
  local name="$2"
  for idx in 1 2 3 4 5; do
    local out="$LOG_DIR/meeyland_refresh_${name}_w${idx}.log"
    local checkpoint="$LOG_DIR/meeyland_refresh_${name}_w${idx}.checkpoint.json"
    local jsonl="$LOG_DIR/meeyland_refresh_${name}_w${idx}.jsonl"
    nohup "$PY" "$SCRIPT" \
      --use-location-table \
      --location-table "$TABLE" \
      --location-level "$LEVEL" \
      --worker-total 5 \
      --worker-index "$idx" \
      --category "$category" \
      --limit "$LIMIT" \
      --delay "$DELAY" \
      --dup-stop-threshold 0 \
      --refresh-existing-contact \
      --checkpoint-file "$checkpoint" \
      --log-jsonl "$jsonl" \
      > "$out" 2>&1 &
    if [[ "$RESUME" == "1" ]]; then
      echo "RESUME=1 requested but currently disabled in launcher for isolated worker checkpoints."
    fi
    echo "started ${name} worker ${idx}: PID=$! log=$out"
  done
}

if [[ "$MODE" == "sale" || "$MODE" == "both" ]]; then
  start_group "5deb722db4367252525c1d00" "sale"
fi

if [[ "$MODE" == "rent" || "$MODE" == "both" ]]; then
  start_group "5deb722db4367252525c1d11" "rent"
fi

echo "done"
