import csv
from datetime import date, timedelta
from pathlib import Path
import pymysql

order_file = Path('/home/chungnt/crawlvip/uploaded_listing_province_2026-03-15_to_now.tsv')
today = date(2026, 4, 12)
days_back = (today.weekday() + 1) % 7
if days_back == 0 and today.weekday() == 6:
    days_back = 7 
sunday = today - timedelta(days=days_back)
start_dt = f"{sunday.isoformat()} 00:00:00"
start_ms = int(sunday.strftime("%s")) * 1000

print(f"Sunday: {start_dt}")

provinces_dict = {}
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

stats = {k: {'name': v['name'], 'df': 0, 'dnf':0} for k, v in provinces_dict.items()}

try:
    with conn.cursor() as cur:
        # Pre-aggregate nhatot
        print("Counting nhatot...")
        cur.execute("""
            SELECT d.cf_province_id, count(*) as c
            FROM (
                SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s
            ) a
            INNER JOIN data_clean_v1 d ON d.ad_id = CONCAT('nhatot_', a.list_id)
            WHERE d.domain = 'nhatot' AND d.cf_province_id IS NOT NULL
            GROUP BY d.cf_province_id
        """, (start_ms,))
        for r in cur.fetchall():
            if r['cf_province_id'] in stats:
                stats[r['cf_province_id']]['dnf'] += r['c']
                
        # Pre-aggregate rest
        print("Counting FULL domains...")
        cur.execute("""
            SELECT d.cf_province_id, count(*) as c
            FROM (
                SELECT url, domain FROM scraped_details_flat WHERE created_at >= %s
                AND domain IN ('batdongsan.com.vn', 'alonhadat.com.vn', 'mogi', 'guland.vn')
            ) s
            JOIN data_clean_v1 d ON d.url = s.url AND d.domain = s.domain
            WHERE d.cf_province_id IS NOT NULL
            GROUP BY d.cf_province_id
        """, (start_dt,))
        for r in cur.fetchall():
            if r['cf_province_id'] in stats:
                stats[r['cf_province_id']]['df'] += r['c']
             
finally:
    conn.close()

with open('uploaded_stats_result_fast.tsv', 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in stats.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded")
for pid, s in stats.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")
