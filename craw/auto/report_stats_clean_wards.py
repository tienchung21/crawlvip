import csv
from datetime import date, datetime, timedelta
from pathlib import Path
import pymysql

# Load danh sách xã/phường (theo thứ tự file mẫu)
order_file = Path('/home/chungnt/crawlvip/cleanv1_ward_counts_2026-03-29_to_now.tsv')
today = date.today()
days_back = (today.weekday() + 1) % 7
if days_back == 0 and today.weekday() == 6:
    days_back = 7 
sunday = today - timedelta(days=days_back)
start_dt = f"{sunday.isoformat()} 00:00:00"
end_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

ward_list = []
stats = {}

with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for r in reader:
        try:
            w_id = int(r['ward_id'])
            ward_info = {
                'province_id': r['province_id'],
                'province_name': r['province_name'],
                'ward_id': w_id,
                'ward_name': r['ward_name'],
                'total': 0
            }
            if w_id not in stats:
                ward_list.append(w_id)
                stats[w_id] = ward_info
        except Exception:
            pass

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cur:
        # Sử dụng id > 3500000 để quét nhanh bảo vệ database tương tự script tỉnh
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
            wid = r['cf_ward_id']
            if wid in stats:
                stats[wid]['total'] += r['c']
finally:
    conn.close()

# Xuất kết quả
out_file = 'uploaded_stats_weekly_wards_total.tsv'
with open(out_file, 'w', encoding='utf-8') as f:
    f.write("province_id\tprovince_name\tward_id\tward_name\ttotal_uploaded\n")
    # Duyệt theo thứ tự của file file mẫu
    for w_id in ward_list:
        s = stats[w_id]
        f.write(f"{s['province_id']}\t{s['province_name']}\t{s['ward_id']}\t{s['ward_name']}\t{s['total']}\n")

print(f"Đã lưu kết quả theo Xã (Tuần này) tại: {out_file}")
