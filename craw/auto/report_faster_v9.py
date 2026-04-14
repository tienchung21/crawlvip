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
        # Nhatot
        print("Fetching nhatot IDs...")
        cur.execute("SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s", (start_ms,))
        nhatot_ids = {f"nhatot_{r['list_id']}" for r in cur.fetchall()}
        
        # Scraped domains
        print("Fetching scraped URLs...")
        cur.execute("SELECT url FROM scraped_details_flat WHERE created_at >= %s AND domain IN ('alonhadat.com.vn', 'guland.vn', 'mogi', 'batdongsan.com.vn')", (start_dt,))
        scraped_urls = {r['url'] for r in cur.fetchall()}

        print("Streaming data_clean_v1...")
        # Stream result without buffering to avoid memory hang
        conn2 = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                                charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor)
        try:
            with conn2.cursor() as s_cur:
                s_cur.execute("SELECT ad_id, url, domain, cf_province_id FROM data_clean_v1 WHERE cf_province_id IS NOT NULL")
                while True:
                    r = s_cur.fetchone()
                    if not r: break
                    pid = r['cf_province_id']
                    if pid in stats:
                        if r['domain'] == 'nhatot':
                            if r['ad_id'] in nhatot_ids:
                                stats[pid]['dnf'] += 1
                        else:
                            if r['url'] in scraped_urls:
                                stats[pid]['df'] += 1
        finally:
            conn2.close()
            
finally:
    conn.close()

with open('uploaded_stats_since_sunday_final.tsv', 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in stats.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("\ntotal_results:")
for pid, s in stats.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")
