import csv
from pathlib import Path
import pymysql

ROOT = Path("/home/chungnt/crawlvip")
REPORT_DIR = ROOT / 'report_data'
REPORT_DIR.mkdir(parents=True, exist_ok=True)

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

provinces_dict = {}

try:
    with conn.cursor() as cur:
        # Lấy danh sách tỉnh/thành phố trực tiếp từ database
        cur.execute("""
            SELECT new_city_id AS province_id, MIN(new_city_name) AS province_name
            FROM transaction_city_merge
            WHERE new_city_parent_id = 0
            GROUP BY new_city_id
        """)
        for r in cur.fetchall():
             provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

    stats = provinces_dict

    with conn.cursor() as cur:
        # Kéo từ data_full
        cur.execute("""
            SELECT province_id, COUNT(*) as c
            FROM data_full
            WHERE images_status = 'LISTING_UPLOADED'
              AND province_id IS NOT NULL
            GROUP BY province_id
        """)
        for r in cur.fetchall():
            pid = r['province_id']
            if pid in stats:
                stats[pid]['df'] += r['c']
                
        # Kéo từ data_no_full
        cur.execute("""
            SELECT province_id, COUNT(*) as c
            FROM data_no_full
            WHERE images_status = 'LISTING_UPLOADED'
              AND province_id IS NOT NULL
            GROUP BY province_id
        """)
        for r in cur.fetchall():
            pid = r['province_id']
            if pid in stats:
                stats[pid]['dnf'] += r['c']
finally:
    conn.close()

# Sắp xếp giống với report_stats_clean.py
desired_order = [
    63, 1, 61, 43, 12, 62, 33, 29, 60, 34, 
    46, 4, 42, 54, 16, 21, 59, 23, 8, 41, 
    9, 10, 17, 24, 48, 15, 38, 27, 13, 28, 
    5, 44, 30, 47
]

ordered_stats = [(pid, stats[pid]) for pid in desired_order if pid in stats]
for pid, s in stats.items():
    if pid not in desired_order:
        ordered_stats.append((pid, s))

# Xuất ra file trả kết quả
out_file = REPORT_DIR / 'uploaded_listing_province_all_time_updated.tsv'
with out_file.open('w', encoding='utf-8') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in ordered_stats:
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("\n--- KẾT QUẢ ALL-TIME UPLOADED THEO TỈNH ---")
print("province_id\tprovince_name\tdata_full\tdata_no_full\ttotal")
for pid, s in ordered_stats:
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")

print(f"\nĐã lưu kết quả tại: {out_file}")
