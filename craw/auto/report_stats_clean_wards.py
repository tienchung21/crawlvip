from datetime import date, datetime, timedelta
from pathlib import Path
import pymysql
import re

ROOT = Path("/home/chungnt/crawlvip")
REPORT_DIR = ROOT / 'report_data'
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# Ưu tiên thứ tự mới do user chuẩn hóa
# file format: province_id\tprovince_name\tward_id\tward_name\ttotal_uploaded
order_file_new = REPORT_DIR / 'uploaded_stats_weekly_wards_total.tsv'
# fallback thứ tự cũ
order_file_old = REPORT_DIR / 'ward_order.txt'

ordered_ward_ids = []

if order_file_new.exists():
    with open(order_file_new, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            raw = line.strip()
            if not raw:
                continue
            # skip header
            if i == 0 and 'ward_id' in raw.lower():
                continue
            parts = raw.split('\t')
            if len(parts) >= 3 and parts[2].strip().isdigit():
                ordered_ward_ids.append(int(parts[2].strip()))
            else:
                m = re.match(r'^\s*(\d+)', raw)
                if m:
                    ordered_ward_ids.append(int(m.group(1)))

# fallback về file cũ nếu file mới rỗng/lỗi
if not ordered_ward_ids and order_file_old.exists():
    with open(order_file_old, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            m = re.match(r'^\s*(\d+)', raw)
            if m:
                ordered_ward_ids.append(int(m.group(1)))

# dedup giữ thứ tự
if ordered_ward_ids:
    seen = set()
    ordered_ward_ids = [x for x in ordered_ward_ids if not (x in seen or seen.add(x))]

conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='',
    database='craw_db',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
)
wards_dict = {}

try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                p.new_city_id AS province_id,
                p.new_city_name AS province_name,
                w.new_city_id AS ward_id,
                w.new_city_name AS ward_name
            FROM transaction_city_merge w
            JOIN transaction_city_merge p ON w.new_city_parent_id = p.new_city_id
            WHERE w.new_city_parent_id > 0 AND p.new_city_parent_id = 0
        """)
        for r in cur.fetchall():
            wards_dict[r['ward_id']] = {
                'province_id': r['province_id'],
                'province_name': r['province_name'],
                'ward_id': r['ward_id'],
                'ward_name': r['ward_name'],
                'total': 0,
            }

    today = date.today()
    days_back = (today.weekday() + 1) % 7
    if days_back == 0 and today.weekday() == 6:
        days_back = 7
    sunday = today - timedelta(days=days_back)
    start_dt = f"{sunday.isoformat()} 00:00:00"
    end_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT cf_ward_id, COUNT(*) as c
            FROM data_clean_v1
            WHERE id > 3500000
              AND time_crawl >= %s
              AND time_crawl <= %s
              AND cf_ward_id IS NOT NULL
            GROUP BY cf_ward_id
        """, (start_dt, end_dt))

        for r in cur.fetchall():
            w_id = r['cf_ward_id']
            if w_id in wards_dict:
                wards_dict[w_id]['total'] += r['c']
finally:
    conn.close()

out_file = REPORT_DIR / 'uploaded_stats_weekly_wards_total.tsv'
with out_file.open('w', encoding='utf-8') as f:
    f.write("province_id\tprovince_name\tward_id\tward_name\ttotal_uploaded\n")

    export_ward_ids = ordered_ward_ids
    if not export_ward_ids:
        export_ward_ids = sorted(
            wards_dict.keys(),
            key=lambda wid: (
                wards_dict[wid]['province_id'],
                wards_dict[wid]['ward_name'] or "",
                wid,
            ),
        )

    for w_id in export_ward_ids:
        if w_id in wards_dict:
            s = wards_dict[w_id]
            f.write(f"{s['province_id']}\t{s['province_name']}\t{s['ward_id']}\t{s['ward_name']}\t{s['total']}\n")
        else:
            f.write(f"N/A\tUnknown\t{w_id}\tUnknown\t0\n")

print("\n--- KẾT QUẢ PHƯỜNG/XÃ ---")
print(f"Order source ưu tiên: {order_file_new}")
print(f"Fallback order source: {order_file_old}")
print(f"Đã xuất ra {len(export_ward_ids)} khu vực!")
print(f"Chi tiết lưu tại: {out_file}")
