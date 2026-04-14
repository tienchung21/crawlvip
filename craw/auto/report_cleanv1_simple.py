import csv
from datetime import date, timedelta
from pathlib import Path
import pymysql

# Load province mapping
order_file = Path('/home/chungnt/crawlvip/uploaded_listing_province_2026-03-15_to_now.tsv')
today = date.today()
# User requested from last Sunday. If today is Sunday, last Sunday is 7 days ago.
days_back = (today.weekday() + 1) % 7
if days_back == 0 and today.weekday() == 6:
    days_back = 7 
sunday = today - timedelta(days=days_back)
start_dt = f"{sunday.isoformat()} 00:00:00"

provinces_dict = {}
with order_file.open('r', encoding='utf-8-sig', newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        provinces_dict[int(r['province_id'])] = {'name': r['province_name'], 'df': 0, 'dnf': 0}

stats = {k: {'name': v['name'], 'df': 0, 'dnf': 0} for k, v in provinces_dict.items()}

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

print(f"Fetching counts from data_clean_v1 since {start_dt}...")
try:
    with conn.cursor() as cur:
        # We can just run a single fast query now that we have time_crawl!
        cur.execute("""
            SELECT cf_province_id, domain, COUNT(*) as c
            FROM data_clean_v1
            WHERE time_crawl >= %s 
              AND cf_province_id IS NOT NULL
            GROUP BY cf_province_id, domain
        """, (start_dt,))
        
        for r in cur.fetchall():
            pid = r['cf_province_id']
            if pid in stats:
                if r['domain'] == 'nhatot':
                    stats[pid]['dnf'] += r['c']
                else:
                    stats[pid]['df'] += r['c']
finally:
    conn.close()

# Write output
out_file = 'uploaded_stats_result_final.tsv'
with open(out_file, 'w') as f:
    f.write("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded\n")
    for pid, s in stats.items():
        total = s['df'] + s['dnf']
        f.write(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}\n")

print("\n--- RESULTS ---")
print("province_id\tprovince_name\tdata_full_uploaded\tdata_no_full_uploaded\ttotal_uploaded")
for pid, s in stats.items():
    total = s['df'] + s['dnf']
    print(f"{pid}\t{s['name']}\t{s['df']}\t{s['dnf']}\t{total}")

print(f"\nResults written to {out_file}")
