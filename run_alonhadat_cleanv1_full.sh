#!/usr/bin/env bash
set -euo pipefail
cd /home/chungnt/crawlvip
source /home/chungnt/crawlvip/venv/bin/activate
while true; do
  echo "=== $(date '+%F %T') batch start ==="
  tmp=$(mktemp)
  python3 -u /home/chungnt/crawlvip/craw/auto/convert_alonhadat_to_data_clean_v1.py --preview-limit 50 --insert > "$tmp" 2>&1
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
          ON df.source = 'alonhadat.com.vn'
         AND df.source_post_id = sdf.matin
        WHERE sdf.domain='alonhadat.com.vn'
          AND COALESCE(sdf.cleanv1_converted,0)=0
          AND COALESCE(sdf.datafull_converted,0)=1
          AND df.price > 0
          AND df.area > 0
          AND (
                (sdf.trade_type = 'u' AND sdf.loaibds IS NOT NULL AND sdf.loaibds <> '')
             OR (sdf.trade_type = 's' AND sdf.loaibds IN (
                    'Biệt thự, nhà liền kề',
                    'Căn hộ chung cư',
                    'Kho, xưởng',
                    'Mặt bằng',
                    'Nhà mặt tiền',
                    'Nhà trong hẻm',
                    'Shop, kiot, quán',
                    'Trang trại',
                    'Đất nền, liền kề, đất dự án',
                    'Đất nông, lâm nghiệp',
                    'Đất thổ cư, đất ở'
                ))
          )
    """)
    print(cur.fetchone()['c'])
conn.close()
PY
)
  echo "remaining=$rem"
  [ "$rem" = "0" ] && break
  sleep 2
done
