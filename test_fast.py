import pymysql
import pymysql.cursors

start_dt = "2026-04-05 00:00:00"
start_ms = 1775350800000

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn.cursor() as cur:
    print("Fetch ads...")
    cur.execute("SELECT list_id FROM ad_listing_detail WHERE time_crawl >= %s", (start_ms,))
    print(f"nhatot records = {len(cur.fetchall())}")

    print("Fetch scraped...")
    cur.execute("SELECT url, domain FROM scraped_details_flat WHERE created_at >= %s", (start_dt,))
    print(f"scraped records = {len(cur.fetchall())}")
