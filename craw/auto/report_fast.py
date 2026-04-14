import csv
from datetime import date, timedelta
from pathlib import Path
import pymysql
import pymysql.cursors

order_file = Path('/home/chungnt/crawlvip/uploaded_listing_province_2026-03-15_to_now.tsv')
today = date(2026, 4, 12)
days_back = (today.weekday() + 1) % 7
if days_back == 0 and today.weekday() == 6:
    days_back = 7 
sunday = today - timedelta(days=days_back)

start_dt = f"{sunday.isoformat()} 00:00:00"
start_ms = int(sunday.strftime("%s")) * 1000

provinces = []
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces.append((int(r['province_id']), r['province_name']))

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

stats = {p[0]: {'name': p[1], 'df': 0, 'dnf': 0} for p in provinces}

try:
    with conn.cursor() as cur:
        print("Querying scraped_details_flat...")
        cur.execute("""
            SELECT d.cf_province_id, COUNT(DISTINCT d.id) as c
            FROM data_clean_v1 d
            JOIN (
                SELECT url, domain 
                FROM scraped_details_flat 
                WHERE created_at >= %s 
                  AND domain IN ('batdongsan.com.vn', 'mogi', 'alonhadat.com.vn', 'guland.vn')
            ) s ON s.url = d.url AND s.domain = d.domain
            WHERE d.cf_province_id IS NOT NULL
            GROUP BY d.cf_province_id
        """, (start_dt,))
        for row in cur.fetchall():
            pid = row['cf_province_id']
            if pid in stats:
                stats[pid]['df'] = row['c']
                
        print("Querying ad_listing_detail (nhatot)...")
        cur.execute("""
            SELECT d.cf_province_id, COUNT(DISTINCT d.id) as c
            FROM data_clean_v1 d
            JOIN (
                SELECT list_id
                FROM ad_listing_detail
                WHERE time_crawl IS NOT NULL AND time_crawl >= %s
            ) a ON a.list_id = SUBSTRING_INDEX(d.ad_id, '_', -1)
            WHERE d.domain = 'nhatot' AND d.cf_province_id IS NOT NULL
            GROUP BY d.cf_province_id
        """, (start_ms,))
        for row in cur.fetchall():
            pid = row['cf_province_id']
            if pid in stats:
                stats[pid]['dnf'] = row['c']

finally:
    conn.close()

print("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded")
for pid in stats:
    s = stats[pid]
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")
