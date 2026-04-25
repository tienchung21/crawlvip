import csv
import re
from pathlib import Path
import pymysql

ROOT = Path("/home/chungnt/crawlvip")
REPORT_DIR = ROOT / 'report_data'
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Đọc thứ tự danh sách ward_id từ file 
order_file = REPORT_DIR / 'ward_order.txt'
ordered_ward_ids = []
if order_file.exists():
    with open(order_file, 'r', encoding='utf-8') as f:
        for line in f:
            match = re.match(r'^(\d+)', line.strip())
            if match:
                ordered_ward_ids.append(int(match.group(1)))

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
wards_dict = {}

try:
    with conn.cursor() as cur:
        # Lấy thông tin phường/xã từ DB để map tên và province
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
                'df': 0,
                'dnf': 0
            }

    with conn.cursor() as cur:
        # Kéo từ data_full
        cur.execute("""
            SELECT ward_id, COUNT(*) as c
            FROM data_full
            WHERE images_status = 'LISTING_UPLOADED'
              AND ward_id IS NOT NULL
            GROUP BY ward_id
        """)
        for r in cur.fetchall():
            w_id = r['ward_id']
            if w_id in wards_dict:
                wards_dict[w_id]['df'] += r['c']
                
        # Kéo từ data_no_full
        cur.execute("""
            SELECT ward_id, COUNT(*) as c
            FROM data_no_full
            WHERE images_status = 'LISTING_UPLOADED'
              AND ward_id IS NOT NULL
            GROUP BY ward_id
        """)
        for r in cur.fetchall():
            w_id = r['ward_id']
            if w_id in wards_dict:
                wards_dict[w_id]['dnf'] += r['c']
finally:
    conn.close()

out_file = REPORT_DIR / 'uploaded_by_ward_full_no_full_updated.tsv'
with out_file.open('w', encoding='utf-8') as f:
    f.write("province_id\tprovince_name\tward_id\tward_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    
    # Chỉ in ra theo đúng danh sách được yêu cầu
    for w_id in ordered_ward_ids:
        if w_id in wards_dict:
            s = wards_dict[w_id]
            total = s['df'] + s['dnf']
            f.write(f"{s['province_id']}\t{s['province_name']}\t{s['ward_id']}\t{s['ward_name']}\t{s['df']}\t{s['dnf']}\t{total}\n")
        else:
            # Nếu id nằm ngoài CSDL nhưng user yêu cầu, in số 0
            f.write(f"N/A\tUnknown\t{w_id}\tUnknown\t0\t0\t0\n")

print("\n--- KẾT QUẢ UPLOAD ALL-TIME (FULL/NO_FULL) THEO PHƯỜNG/XÃ ---")
print(f"Đã xuất ra {len(ordered_ward_ids)} khu vực theo đúng thứ tự file yêu cầu!")
print(f"Chi tiết lưu tại: {out_file}")

# Hiển thị demo một vài dòng đầu:
print("\n[Preview 5 records]")
print("province_name | ward_name | df | dnf | total")
for i, w_id in enumerate(ordered_ward_ids[:5]):
    if w_id in wards_dict:
        s = wards_dict[w_id]
        total = s['df'] + s['dnf']
        print(f"{s['province_name']} | {s['ward_name']} | {s['df']} | {s['dnf']} | {total}")

