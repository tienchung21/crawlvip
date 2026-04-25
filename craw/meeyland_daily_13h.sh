#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/chungnt/crawlvip"
VENV_PY="$ROOT_DIR/venv/bin/python"
LOG_DIR="$ROOT_DIR/craw/logs"
mkdir -p "$LOG_DIR"

RUN_TS="$(date '+%Y%m%d_%H%M%S')"
MASTER_LOG="$LOG_DIR/meeyland_daily_13h.log"

# Search config (can override by env)
API_URL="${API_URL:-https://api5.meeyland.com/v1/articles/search}"
DOMAIN="${DOMAIN:-meeyland.com}"
ORIGIN="${ORIGIN:-https://meeyland.com}"
REFERER="${REFERER:-https://meeyland.com/}"
X_TENANT="${X_TENANT:-bWVleWxhbmQ=}"
LIMIT="${LIMIT:-200}"
DELAY="${DELAY:-3}"
SPLIT_THRESHOLD="${SPLIT_THRESHOLD:-20000}"
DUP_STOP_THRESHOLD="${DUP_STOP_THRESHOLD:-300}"
CITY_SPLIT_THRESHOLD="${CITY_SPLIT_THRESHOLD:-5000}"
DISTRICT_SPLIT_THRESHOLD="${DISTRICT_SPLIT_THRESHOLD:-5000}"
AUTO_SPLIT_THRESHOLD="${AUTO_SPLIT_THRESHOLD:-20000}"
MAX_RETRIES="${MAX_RETRIES:-4}"
START_PART="${START_PART:-1}"
LOCATION_START_PART="${LOCATION_START_PART:-L1}"
SALE_CATEGORY="${SALE_CATEGORY:-5deb722db4367252525c1d00}"
RENT_CATEGORY="${RENT_CATEGORY:-5deb722db4367252525c1d11}"
FAKE_COORDINATES="${FAKE_COORDINATES:-1}" # 1=true,0=false
USE_LOCATION_TABLE="${USE_LOCATION_TABLE:-1}" # 1=true,0=false
LOCATION_TABLE="${LOCATION_TABLE:-location_meeland}"
LOCATION_LEVEL="${LOCATION_LEVEL:-district}" # district|ward
LOCATION_CITY_MEEY_ID="${LOCATION_CITY_MEEY_ID:-}"
LOCATION_DISTRICT_MEEY_ID="${LOCATION_DISTRICT_MEEY_ID:-}"

# Detail config (can override by env)
DETAIL_URL_TEMPLATE="${DETAIL_URL_TEMPLATE:-https://api5.meeyland.com/v1/articles/{code}}"
DETAIL_WORKERS="${DETAIL_WORKERS:-3}"
DETAIL_BATCH_SIZE="${DETAIL_BATCH_SIZE:-100}"
DETAIL_MIN_DELAY="${DETAIL_MIN_DELAY:-0.75}"
DETAIL_MAX_DELAY="${DETAIL_MAX_DELAY:-1.5}"
DETAIL_MAX_RETRIES="${DETAIL_MAX_RETRIES:-3}"
DETAIL_STOP_ERROR_STREAK="${DETAIL_STOP_ERROR_STREAK:-10}"

AUTH_TOKEN="${AUTH_TOKEN:-}"
EXPIRE_TOKEN="${EXPIRE_TOKEN:-}"

log() {
  echo "[$(date '+%F %T')] $*" | tee -a "$MASTER_LOG"
}

run_step() {
  local step_name="$1"
  shift
  log "[STEP_START] $step_name"
  "$@" 2>&1 | tee -a "$MASTER_LOG"
  log "[STEP_DONE] $step_name"
}

build_common_search_args() {
  local start_part_value="$START_PART"
  if [[ "${USE_LOCATION_TABLE}" == "1" ]]; then
    start_part_value="$LOCATION_START_PART"
  fi
  local arr=(
    --api-url "$API_URL"
    --domain "$DOMAIN"
    --origin "$ORIGIN"
    --referer "$REFERER"
    --x-tenant "$X_TENANT"
    --limit "$LIMIT"
    --delay "$DELAY"
    --split-threshold "$SPLIT_THRESHOLD"
    --city-split-threshold "$CITY_SPLIT_THRESHOLD"
    --district-split-threshold "$DISTRICT_SPLIT_THRESHOLD"
    --auto-split-threshold "$AUTO_SPLIT_THRESHOLD"
    --dup-stop-threshold "$DUP_STOP_THRESHOLD"
    --max-retries "$MAX_RETRIES"
    --start-part "$start_part_value"
    --resume
  )
  if [[ "${FAKE_COORDINATES}" == "1" ]]; then
    arr+=(--fake-coordinates)
  fi
  if [[ -n "$AUTH_TOKEN" ]]; then
    arr+=(--auth-token "$AUTH_TOKEN")
  fi
  if [[ -n "$EXPIRE_TOKEN" ]]; then
    arr+=(--expire-token "$EXPIRE_TOKEN")
  fi
  if [[ "${USE_LOCATION_TABLE}" == "1" ]]; then
    arr+=(--use-location-table --location-table "$LOCATION_TABLE" --location-level "$LOCATION_LEVEL")
    if [[ -n "$LOCATION_CITY_MEEY_ID" ]]; then
      arr+=(--location-city-meey-id "$LOCATION_CITY_MEEY_ID")
    fi
    if [[ -n "$LOCATION_DISTRICT_MEEY_ID" ]]; then
      arr+=(--location-district-meey-id "$LOCATION_DISTRICT_MEEY_ID")
    fi
  fi
  printf '%s\0' "${arr[@]}"
}

log "=== MEEYLAND DAILY 13H PIPELINE START (run=$RUN_TS) ==="
log "[CONFIG] domain=$DOMAIN api=$API_URL limit=$LIMIT delay=$DELAY split_threshold=$SPLIT_THRESHOLD city_split_threshold=$CITY_SPLIT_THRESHOLD district_split_threshold=$DISTRICT_SPLIT_THRESHOLD auto_split_threshold=$AUTO_SPLIT_THRESHOLD dup_stop_threshold=$DUP_STOP_THRESHOLD use_location_table=$USE_LOCATION_TABLE location_table=$LOCATION_TABLE location_level=$LOCATION_LEVEL"

mapfile -d '' COMMON_SEARCH_ARGS < <(build_common_search_args)

run_step "search_sale" \
  "$VENV_PY" "$ROOT_DIR/craw/meeymap_search_crawler.py" \
  "${COMMON_SEARCH_ARGS[@]}" \
  --category "$SALE_CATEGORY" \
  --checkpoint-file "$LOG_DIR/meeyland_sale_checkpoint.json" \
  --log-file "$LOG_DIR/meeyland_sale_search.log" \
  --log-jsonl "$LOG_DIR/meeyland_sale_search.jsonl" \
  --split-log-file "$LOG_DIR/meeyland_sale_split.log"

run_step "search_rent" \
  "$VENV_PY" "$ROOT_DIR/craw/meeymap_search_crawler.py" \
  "${COMMON_SEARCH_ARGS[@]}" \
  --category "$RENT_CATEGORY" \
  --checkpoint-file "$LOG_DIR/meeyland_rent_checkpoint.json" \
  --log-file "$LOG_DIR/meeyland_rent_search.log" \
  --log-jsonl "$LOG_DIR/meeyland_rent_search.jsonl" \
  --split-log-file "$LOG_DIR/meeyland_rent_split.log"

DETAIL_ARGS=(
  --domain "$DOMAIN"
  --detail-url-template "$DETAIL_URL_TEMPLATE"
  --workers "$DETAIL_WORKERS"
  --batch-size "$DETAIL_BATCH_SIZE"
  --min-delay "$DETAIL_MIN_DELAY"
  --max-delay "$DETAIL_MAX_DELAY"
  --max-retries "$DETAIL_MAX_RETRIES"
  --stop-error-streak "$DETAIL_STOP_ERROR_STREAK"
  --log-file "$LOG_DIR/meeyland_detail.log"
  --checkpoint-file "$LOG_DIR/meeyland_detail_checkpoint.json"
  --resume
)
if [[ -n "$AUTH_TOKEN" ]]; then
  DETAIL_ARGS+=(--auth-token "$AUTH_TOKEN")
fi
if [[ -n "$EXPIRE_TOKEN" ]]; then
  DETAIL_ARGS+=(--expire-token "$EXPIRE_TOKEN")
fi
if [[ -n "$X_TENANT" ]]; then
  DETAIL_ARGS+=(--x-tenant "$X_TENANT")
fi
if [[ -n "$ORIGIN" ]]; then
  DETAIL_ARGS+=(--origin "$ORIGIN")
fi
if [[ -n "$REFERER" ]]; then
  DETAIL_ARGS+=(--referer "$REFERER")
fi

run_step "detail_update" \
  "$VENV_PY" "$ROOT_DIR/craw/meeymap_detail_updater.py" \
  "${DETAIL_ARGS[@]}"

log "=== MEEYLAND DAILY 13H PIPELINE DONE (run=$RUN_TS) ==="
