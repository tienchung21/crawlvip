import csv
from datetime import date, datetime, timedelta
from pathlib import Path
import pymysql

ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT / 'report_data'
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# Lấy danh sách tỉnh thành trực tiếp từ database thay vì đọc file TSV cứng
conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

provinces_dict = {}
try:
    with conn.cursor() as cur:
        # Lấy danh sách tỉnh/thành phố chuẩn từ transaction_city_merge
        cur.execute("""
            SELECT new_city_id AS province_id, MIN(new_city_name) AS province_name
            FROM transaction_city_merge
            WHERE new_city_parent_id = 0
            GROUP BY new_city_id
        """)
        for r in cur.fetchall():
             provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'total': 0}
finally:
    pass # we keep connection open for next query

today = date.today()
days_back = (today.weekday() + 1) % 7
if days_back == 0 and today.weekday() == 6:
    days_back = 7 
sunday = today - timedelta(days=days_back)
start_dt = f"{sunday.isoformat()} 00:00:00"
end_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

stats = {k: {'name': v['name'], 'total': 0} for k, v in provinces_dict.items()}

try:
    with conn.cursor() as cur:
        # Quét data_clean_v1
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
# Sắp xếp theo order mong muốn
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

out_file = REPORT_DIR / 'uploaded_stats_weekly_total.tsv'
with out_file.open('w', encoding='utf-8') as f:
    f.write("province_id\tprovince_name\ttotal_uploaded\n")
    for pid, s in ordered_stats:
        f.write(f"{pid}\t{s['name']}\t{s['total']}\n")

print("\n--- KẾT QUẢ TỔNG (TUẦN NÀY) ---")
print("province_id\tprovince_name\ttotal_uploaded")
for pid, s in ordered_stats:
    print(f"{pid}\t{s['name']}\t{s['total']}")

print(f"\nĐã lưu kết quả tại: {out_file}")
