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
        # For Mogi, Alonhadat, Batdongsan, Guland
        for domain in ['batdongsan.com.vn', 'mogi', 'alonhadat.com.vn', 'guland.vn']:
            print(f"Fetching {domain} from SDF...")
            # Getting only the URLs inserted after the Sunday date from SDF
            cur.execute("""
                SELECT url 
                FROM scraped_details_flat FORCE INDEX (idx_sdf_domain) 
                WHERE domain = %s AND created_at >= %s
            """, (domain, start_dt))
            urls = {r['url'] for r in cur.fetchall()}
            if not urls:
                continue
            
            # Now querying data_clean_v1 matching those URLs
            print(f"  matching {len(urls)} urls against data_clean_v1")
            
            # Batch urls for IN clause to avoid giant query buffer or pulling all data_clean_v1 strings
            url_list = list(urls)
            chunk_size = 5000
            for i in range(0, len(url_list), chunk_size):
                chunk = url_list[i:i+chunk_size]
                format_strings = ','.join(['%s'] * len(chunk))
                cur.execute(f"""
                    SELECT cf_province_id, count(id) as c 
                    FROM data_clean_v1 
                    WHERE domain = %s AND cf_province_id IS NOT NULL AND url IN ({format_strings})
                    GROUP BY cf_province_id
                """, [domain] + chunk)
                for r in cur.fetchall():
                    if r['cf_province_id'] in stats:
                        stats[r['cf_province_id']]['df'] += r['c']
                        
        print("Fetching nhatot from ad_listing_detail...")
        cur.execute("SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s", (start_ms,))
        list_ids = {str(r['list_id']) for r in cur.fetchall()}
        
        if list_ids:
            print(f"  matching {len(list_ids)} IDs against data_clean_v1")
            lids_array = list(list_ids)
            chunk_size = 5000
            for i in range(0, len(lids_array), chunk_size):
                chunk = lids_array[i:i+chunk_size]
                chunk_ad_ids = [f"nhatot_{x}" for x in chunk] + chunk
                format_strings = ','.join(['%s'] * len(chunk_ad_ids))
                cur.execute(f"""
                    SELECT cf_province_id, count(id) as c 
                    FROM data_clean_v1 
                    WHERE domain = 'nhatot' AND cf_province_id IS NOT NULL AND ad_id IN ({format_strings})
                    GROUP BY cf_province_id
                """, chunk_ad_ids)
                for r in cur.fetchall():
                    if r['cf_province_id'] in stats:
                        stats[r['cf_province_id']]['dnf'] += r['c']
        
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
