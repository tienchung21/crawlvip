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

print(f"Tracking from {start_dt}")

provinces_dict = {}
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cur:
        # Load mapping first
        print("Pulling data_clean_v1...")
        cur.execute("SELECT ad_id, domain, cf_province_id, url FROM data_clean_v1 WHERE cf_province_id IS NOT NULL")
        rows = cur.fetchall()
        
        nhatot_map = {}
        sdf_map = {}
        for r in rows:
            if r['domain'] == 'nhatot':
                clean_id = str(r['ad_id']).replace('nhatot_', '')
                nhatot_map[clean_id] = r['cf_province_id']
            elif r['domain'] in ('batdongsan.com.vn', 'mogi', 'alonhadat.com.vn', 'guland.vn'):
                # Handle possible missing URLs gracefully
                if r['url']:
                    key = f"{r['domain']}_{r['url']}"
                    sdf_map[key] = r['cf_province_id']
                    
        # Nhatot
        print(f"Parsing ad_listing_detail for >= {start_ms}...")
        cur.execute("SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s", (start_ms,))
        for r in cur.fetchall():
            lst_id = str(r['list_id'])
            if lst_id in nhatot_map:
                pid = nhatot_map[lst_id]
                if pid in provinces_dict:
                    provinces_dict[pid]['dnf'] += 1
                    
        # Full domains
        print(f"Parsing scraped_details_flat for >= {start_dt}...")
        cur.execute("SELECT url, domain FROM scraped_details_flat WHERE created_at >= %s", (start_dt,))
        for r in cur.fetchall():
            key = f"{r['domain']}_{r['url']}"
            if key in sdf_map:
                pid = sdf_map[key]
                if pid in provinces_dict:
                    provinces_dict[pid]['df'] += 1
                    
finally:
    conn.close()

with open('uploaded_stats_result_fast.tsv', 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in provinces_dict.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded")
for pid, s in provinces_dict.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")

