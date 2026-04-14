import csv
from pathlib import Path
import pymysql

# Load in the exact order the user requested
order_file = Path('/home/chungnt/crawlvip/uploaded_listing_province_all_time.tsv')

provinces_dict = {}
ordered_pids = []

with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        pid = int(r['province_id'])
        if pid not in provinces_dict:
            ordered_pids.append(pid)
            provinces_dict[pid] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

stats = provinces_dict

try:
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

# Xuất ra file trả kết quả
out_file = '/home/chungnt/crawlvip/uploaded_listing_province_all_time_updated.tsv'
with open(out_file, 'w', encoding='utf-8') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid in ordered_pids:
        s = stats[pid]
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("\n--- KẾT QUẢ ALL-TIME UPLOADED THEO TỈNH ---")
print("province_id\tprovince_name\tdata_full\tdata_no_full\ttotal_uploaded")
for pid in ordered_pids:
    s = stats[pid]
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")

print(f"\nĐã lưu kết quả tại: {out_file}")
