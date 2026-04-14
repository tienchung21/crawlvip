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

provinces_dict = {}
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

stats = {k: {'name': v['name'], 'df': 0, 'dnf':0} for k, v in provinces_dict.items()}

try:
    with conn.cursor() as cur:
        # Nhatot query
        print("Fetching nhatot count...")
        # Since domain is indexed, let's reverse the query direction to scan from right table first explicitly
        cur.execute("""
            SELECT d.cf_province_id, count(a.list_id) as c
            FROM data_clean_v1 d FORCE INDEX (idx_data_clean_DOMAIN)
            CROSS JOIN ad_listing_detail a
            WHERE d.domain = 'nhatot' 
              AND d.ad_id = CONCAT('nhatot_', a.list_id)
              AND a.time_crawl >= %s
              AND d.cf_province_id IS NOT NULL
            GROUP BY d.cf_province_id
        """, (start_ms,))
        for r in cur.fetchall():
            if r['cf_province_id'] in stats:
                stats[r['cf_province_id']]['dnf'] += r['c']
                
        for domain in ['batdongsan.com.vn', 'alonhadat.com.vn', 'mogi', 'guland.vn']:
            print(f"Fetching {domain} count...")
            cur.execute("""
                SELECT d.cf_province_id, count(s.url) as c
                FROM data_clean_v1 d FORCE INDEX (idx_data_clean_DOMAIN)
                CROSS JOIN scraped_details_flat s FORCE INDEX (idx_sdf_domain)
                WHERE d.domain = %s
                  AND s.domain = %s
                  AND d.url = s.url
                  AND s.created_at >= %s
                  AND d.cf_province_id IS NOT NULL
                GROUP BY d.cf_province_id
            """, (domain, domain, start_dt))
            for r in cur.fetchall():
                if r['cf_province_id'] in stats:
                    stats[r['cf_province_id']]['df'] += r['c']
finally:
    conn.close()

with open('uploaded_stats_result_final.tsv', 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in stats.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("\ntotal_results:")
for pid, s in stats.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")
