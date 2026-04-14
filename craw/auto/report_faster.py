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

print(f"Sunday: {start_dt}, MS: {start_ms}")

provinces = {}
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces[int(r['province_id'])] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cur:
        print("1. Counting Nhatot directly from data_clean_v1 joined to ad_listing_detail...")
        cur.execute("""
            SELECT d.cf_province_id, COUNT(d.id) as c
            FROM data_clean_v1 d
            JOIN ad_listing_detail a ON a.list_id = SUBSTRING_INDEX(d.ad_id, '_', -1)
            WHERE d.domain = 'nhatot'
              AND d.cf_province_id IS NOT NULL
              AND a.time_crawl >= %s
            GROUP BY d.cf_province_id
        """, (start_ms,))
        for row in cur.fetchall():
            pid = row['cf_province_id']
            if pid in provinces:
                provinces[pid]['dnf'] += row['c']

        print("2. Fetching Mogi/Alonhadat/Guland/Batdongsan...")
        for domain in ['batdongsan.com.vn', 'mogi', 'alonhadat.com.vn', 'guland.vn']:
            print(f"   -> {domain}")
            cur.execute("""
                SELECT d.cf_province_id, COUNT(d.id) as c
                FROM data_clean_v1 d
                JOIN scraped_details_flat s ON s.url = d.url AND s.domain = d.domain
                WHERE d.domain = %s
                  AND d.cf_province_id IS NOT NULL
                  AND s.created_at >= %s
                GROUP BY d.cf_province_id
            """, (domain, start_dt))
            for row in cur.fetchall():
                pid = row['cf_province_id']
                if pid in provinces:
                    provinces[pid]['df'] += row['c']

finally:
    conn.close()

with open('uploaded_stats_result_fast.tsv', 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in provinces.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")
        print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")
