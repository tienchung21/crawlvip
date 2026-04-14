
import pymysql
import re

def check_reposts():
    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
    cur = conn.cursor()
    
    # 1. Check Date Range
    print("--- Crawled Batches ---")
    cur.execute("SELECT batch_date, COUNT(*) FROM collected_links GROUP BY batch_date ORDER BY batch_date DESC")
    batches = cur.fetchall()
    for b in batches:
        print(f"Date {b[0]}: {b[1]} links")
        
    # 2. Check Reposts (Same Slug, Diff ID)
    # URL Format: .../slug-prID
    # We extract Slug by removing -pr\d+ at end?
    # Or just group by substring?
    
    print("\n--- Checking Potential Reposts (Same Slug, Diff URL) ---")
    # Fetch all URLs in range
    # Assuming batch_date format YYYYMMDD
    cur.execute("SELECT url, batch_date FROM collected_links WHERE batch_date BETWEEN '20260120' AND '20260128'")
    rows = cur.fetchall()
    
    slug_map = {}
    repost_count = 0
    
    for url, date in rows:
        # Extract ID and Slug
        # Example: /ban-nha-a-pr123
        # Regex: (.*)-pr(\d+)
        match = re.search(r'/(.*?)-pr(\d+)', url)
        if match:
            slug = match.group(1)
            pr_id = match.group(2)
            
            if slug in slug_map:
                prev_id, prev_date, prev_url = slug_map[slug]
                if prev_id != pr_id:
                    print(f"AVAILABLE REPOST FOUND:")
                    print(f"  Orig ({prev_date}): .../{slug}-pr{prev_id}")
                    print(f"  New  ({date}):     .../{slug}-pr{pr_id}")
                    repost_count += 1
            else:
                slug_map[slug] = (pr_id, date, url)
                
    if repost_count == 0:
        print("No obvious reposts found (based on Slug matching).")
    else:
        print(f"Total Duplicate Slugs Found: {repost_count}")

    conn.close()

if __name__ == "__main__":
    check_reposts()
