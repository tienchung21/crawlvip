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

provinces_dict = {}
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

stats = {k: {'name': v['name'], 'df': 0, 'dnf':0} for k, v in provinces_dict.items()}

try:
    with conn.cursor() as cur:
        
        # 1. Grab domains that have indices first
        print("Grabbing counts using EXISTS for speed...")
        for domain in ['batdongsan.com.vn', 'mogi', 'alonhadat.com.vn', 'guland.vn']:
            print(f"Domain: {domain}")
            cur.execute("""
                SELECT d.cf_province_id, count(d.id) as c
                FROM data_clean_v1 d
                WHERE d.domain = %s AND d.cf_province_id IS NOT NULL
                  AND d.url IN (
                      SELECT url FROM scraped_details_flat FORCE INDEX (idx_sdf_domain)
                      WHERE domain = %s AND created_at >= %s
                  )
                GROUP BY d.cf_province_id
            """, (domain, domain, start_dt))
            for r in cur.fetchall():
                if r['cf_province_id'] in stats:
                    stats[r['cf_province_id']]['df'] += r['c']
                    
        print("Nhatot...")
        cur.execute("""
            SELECT d.cf_province_id, count(d.id) as c
            FROM data_clean_v1 d
            WHERE d.domain = 'nhatot' AND d.cf_province_id IS NOT NULL
              AND REPLACE(d.ad_id, 'nhatot_', '') IN (
                  SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s
              )
            GROUP BY d.cf_province_id
        """, (start_ms,))
        for r in cur.fetchall():
            if r['cf_province_id'] in stats:
                stats[r['cf_province_id']]['dnf'] += r['c']
                
finally:
    conn.close()

print("\n--- RESULTS ---")
print("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded")
for pid, s in stats.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")

with open('uploaded_stats_result_fast.tsv', 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in stats.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

