#!/usr/bin/env bash
set -euo pipefail
cd /home/chungnt/crawlvip
source /home/chungnt/crawlvip/venv/bin/activate
while true; do
  echo "=== $(date '+%F %T') batch start ==="
  python3 -u /home/chungnt/crawlvip/craw/auto/convert_alonhadat_to_data_full.py --preview-limit 50 --insert
  rem=$(python3 - <<'PY'
import sys
sys.path.insert(0, '/home/chungnt/crawlvip/craw')
from database import Database

db = Database()
conn = db.get_connection()
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) c FROM scraped_details_flat WHERE domain='alonhadat.com.vn' AND COALESCE(datafull_converted,0)=0 AND COALESCE(datafull_skip_reason,'')='' AND dientich IS NOT NULL AND khoanggia IS NOT NULL")
    print(cur.fetchone()['c'])
conn.close()
PY
)
  echo "remaining=$rem"
  [ "$rem" = "0" ] && break
  sleep 2
done
