import csv
from datetime import date, timedelta
from pathlib import Path
import pymysql
import sys

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
        # Load keys from data_clean_v1 explicitly filtering
        cur.execute("SELECT ad_id, cf_province_id FROM data_clean_v1 WHERE domain = 'nhatot' AND cf_province_id IS NOT NULL")
        nhatot_map = {r['ad_id'].replace('nhatot_', ''): r['cf_province_id'] for r in cur.fetchall()}
        
        cur.execute("SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s", (start_ms,))
        for r in cur.fetchall():
            lst = str(r['list_id'])
            if lst in nhatot_map:
                pid = nhatot_map[lst]
                if pid in stats:
                    stats[pid]['dnf'] += 1
                    
        for domain in ['alonhadat.com.vn', 'guland.vn', 'mogi', 'batdongsan.com.vn']:
            cur.execute("SELECT url FROM scraped_details_flat WHERE domain = %s AND created_at >= %s", (domain, start_dt))
            urls = [r['url'] for r in cur.fetchall()]
            if not urls: continue
            
            chunk_size = 5000
            for i in range(0, len(urls), chunk_size):
                chunk = set(urls[i:i+chunk_size])
                
                cur.execute("SELECT url, cf_province_id FROM data_clean_v1 WHERE domain = %s AND cf_province_id IS NOT NULL", (domain,))
                for r in cur.fetchall():
                    if r['url'] in chunk:
                        pid = r['cf_province_id']
                        if pid in stats:
                            stats[pid]['df'] += 1
                        
finally:
    conn.close()

with open('uploaded_stats_since_sunday_ok.tsv', 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in stats.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded")
for pid, s in stats.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")
