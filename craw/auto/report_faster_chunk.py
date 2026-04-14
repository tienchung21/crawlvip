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

# We will collect the generated IDs and URLs, then count them in data_clean_v1
try:
    with conn.cursor() as cur:
        # Nhatot query
        print("Fetching nhatot IDs...")
        cur.execute("""
            SELECT CONCAT('nhatot_', list_id) as ad_id FROM ad_listing_detail WHERE time_crawl >= %s
        """, (start_ms,))
        nhatot_ids = [r['ad_id'] for r in cur.fetchall()]
        print(f"Got {len(nhatot_ids)} Nhatot IDs.")
        
        print("Fetching scraped URLs...")
        # Since this query is timing out fetching all URLs into memory, let's optimize it!
        
        def chunker(seq, size):
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))
            
        print("Counting Nhatot in data_clean_v1...")
        for chunk in chunker(nhatot_ids, 5000):
            format_strings = ','.join(['%s'] * len(chunk))
            cur.execute(f"SELECT cf_province_id, COUNT(*) as c FROM data_clean_v1 WHERE domain = 'nhatot' AND ad_id IN ({format_strings}) AND cf_province_id IS NOT NULL GROUP BY cf_province_id", tuple(chunk))
            for r in cur.fetchall():
                pid = r['cf_province_id']
                if pid in stats:
                    stats[pid]['dnf'] += r['c']

        # To avoid blowing up URL fetch, let's select URL AND count where they match directly using EXISTS
        print("Counting Scraped in data_clean_v1...")
        
        for p_id in stats.keys():
            cur.execute("""
                SELECT COUNT(*) as c 
                FROM data_clean_v1 d FORCE INDEX (idx_data_clean_DOMAIN)
                WHERE d.cf_province_id = %s
                  AND d.domain != 'nhatot'
                  AND EXISTS (
                      SELECT 1 FROM scraped_details_flat s
                      WHERE s.url = d.url
                        AND s.created_at >= %s
                  )
            """, (p_id, start_dt))
            r = cur.fetchone()
            stats[p_id]['df'] = r['c']
            print(f"Province {p_id}: {r['c']} (df)")

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
