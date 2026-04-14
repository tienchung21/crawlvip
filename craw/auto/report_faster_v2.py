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

import sys
try:
    with conn.cursor() as cur:
        # Nhatot 
        cur.execute("""
            SELECT REPLACE(ad_id, 'nhatot_', '') as list_id, cf_province_id 
            FROM data_clean_v1 
            WHERE domain = 'nhatot' AND cf_province_id IS NOT NULL
        """)
        clean_nhatot = {str(r['list_id']): r['cf_province_id'] for r in cur.fetchall()}
        
        cur.execute("SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s", (start_ms,))
        for r in cur.fetchall():
            list_id = str(r['list_id'])
            if list_id in clean_nhatot:
                pid = clean_nhatot[list_id]
                if pid in provinces_dict:
                    provinces_dict[pid]['dnf'] += 1
                    
        # Other domains
        cur.execute("""
            SELECT url, domain, cf_province_id 
            FROM data_clean_v1 
            WHERE domain IN ('batdongsan.com.vn', 'mogi', 'alonhadat.com.vn', 'guland.vn') 
            AND cf_province_id IS NOT NULL
        """)
        clean_others = {f"{r['domain']}_{r['url']}": r['cf_province_id'] for r in cur.fetchall()}
        
        cur.execute("SELECT url, domain FROM scraped_details_flat WHERE created_at >= %s", (start_dt,))
        for r in cur.fetchall():
            key = f"{r['domain']}_{r['url']}"
            if key in clean_others:
                pid = clean_others[key]
                if pid in provinces_dict:
                    provinces_dict[pid]['df'] += 1
finally:
    conn.close()

print("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded")
for pid, s in provinces_dict.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")
