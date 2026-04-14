#!/usr/bin/env bash
set -euo pipefail
cd /home/chungnt/crawlvip
source /home/chungnt/crawlvip/venv/bin/activate
while true; do
  echo "=== $(date '+%F %T') batch start ==="
  tmp=$(mktemp)
  python3 -u /home/chungnt/crawlvip/craw/auto/convert_guland_to_data_clean_v1.py --preview-limit 200 --insert > "$tmp" 2>&1
  cat "$tmp"
  sel=$(awk -F= '/^selected=/{print $2}' "$tmp" | tail -n1)
  rm -f "$tmp"
  [ "${sel:-0}" = "0" ] && break
  rem=$(python3 - <<'PY'
import sys
sys.path.insert(0, '/home/chungnt/crawlvip/craw')
from database import Database

db = Database(); conn = db.get_connection()
with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*) c
        FROM scraped_details_flat sdf
        JOIN data_full df
          ON df.source = 'guland.vn'
         AND df.source_post_id = sdf.matin
        WHERE sdf.domain='guland.vn'
          AND COALESCE(sdf.cleanv1_converted,0)=0
          AND COALESCE(sdf.datafull_converted,0)=1
          AND df.price > 0
          AND df.area > 0
          AND sdf.trade_type IN ('s', 'u')
          AND sdf.loaibds IN ('Căn hộ chung cư', 'Kho, nhà xưởng', 'Nhà riêng', 'Đất')
    """)
    print(cur.fetchone()['c'])
conn.close()
PY
)
  echo "remaining=$rem"
  [ "$rem" = "0" ] && break
  sleep 1
done
