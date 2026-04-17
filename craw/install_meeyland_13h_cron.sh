#!/usr/bin/env bash
set -euo pipefail

CRON_CMD="/bin/bash /home/chungnt/crawlvip/craw/meeyland_daily_13h.sh"
CRON_LINE="0 13 * * * $CRON_CMD"

TMP_FILE="$(mktemp)"
crontab -l 2>/dev/null | grep -vF "$CRON_CMD" > "$TMP_FILE" || true
echo "$CRON_LINE" >> "$TMP_FILE"
crontab "$TMP_FILE"
rm -f "$TMP_FILE"

echo "Installed cron:"
echo "$CRON_LINE"
