import csv
from datetime import date, datetime, timedelta
from pathlib import Path
import pymysql

# Load danh sách tỉnh thành chuẩn
order_file = Path('/home/chungnt/crawlvip/uploaded_listing_province_2026-03-15_to_now.tsv')
today = date.today()
days_back = (today.weekday() + 1) % 7
if days_back == 0 and today.weekday() == 6:
    days_back = 7 
sunday = today - timedelta(days=days_back)
start_dt = f"{sunday.isoformat()} 00:00:00"
end_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

provinces_dict = {}
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'total': 0}

stats = {k: {'name': v['name'], 'total': 0} for k, v in provinces_dict.items()}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cur:
        # Sử dụng id > 3500000 để quét nhanh trong khi vùng cũ đang được backfill
        cur.execute("""
            SELECT cf_province_id, COUNT(*) as c
            FROM data_clean_v1
            WHERE id > 3500000 
              AND time_crawl >= %s 
              AND time_crawl <= %s
              AND cf_province_id IS NOT NULL
            GROUP BY cf_province_id
        """, (start_dt, end_dt))
        
        for r in cur.fetchall():
            pid = r['cf_province_id']
            if pid in stats:
                stats[pid]['total'] += r['c']
finally:
    conn.close()

# Xuất kết quả đầy đủ
out_file = 'uploaded_stats_weekly_total.tsv'
with open(out_file, 'w') as f:
    f.write("province_id\tprovince_name\ttotal_uploaded\n")
    for pid, s in stats.items():
        f.write(f"{pid}\t{s['name']}\t{s['total']}\n")

print("\n--- KẾT QUẢ TỔNG (TUẦN NÀY) ---")
print("province_id\tprovince_name\ttotal_uploaded")
for pid, s in stats.items():
    print(f"{pid}\t{s['name']}\t{s['total']}")

print(f"\nĐã lưu kết quả tại: {out_file}")
