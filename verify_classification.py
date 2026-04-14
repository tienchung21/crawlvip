
import os
import sys

sys.path.append(os.getcwd())
from craw.database import Database

def main():
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    
    # 1. Count Classified vs Unclassified (Batdongsan)
    print("Checking Classification Progress...")
    cur.execute("""
        SELECT 
            SUM(CASE WHEN loaihinh IS NOT NULL AND loaihinh != '' THEN 1 ELSE 0 END) as classified,
            SUM(CASE WHEN prj_id IS NOT NULL AND prj_id != 0 THEN 1 ELSE 0 END) as has_id,
            COUNT(*) as total
        FROM collected_links
        WHERE domain = 'batdongsan.com.vn' OR url LIKE '%%batdongsan.com.vn%%';
    """)
    stats = cur.fetchone()
    if isinstance(stats, dict):
         print(f"Total: {stats['total']}")
         print(f"Classified: {stats['classified']}")
         print(f"With ID: {stats['has_id']}")
    else:
         print(f"Total: {stats[2]}")
         print(f"Classified: {stats[0]}")
         print(f"With ID: {stats[1]}")

    # 2. Show Samples of Classified
    print("\nSample Updated Rows:")
    cur.execute("""
        SELECT url, loaihinh, trade_type 
        FROM collected_links 
        WHERE (domain = 'batdongsan.com.vn' OR url LIKE '%%batdongsan.com.vn%%')
          AND loaihinh IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT 10;
    """)
    rows = cur.fetchall()
    for r in rows:
        if isinstance(r, dict):
            print(f"[{r['trade_type']}] {r['loaihinh']} \n   <- {r['url']}")
        else:
            print(f"[{r[2]}] {r[1]} \n   <- {r[0]}")

    conn.close()

if __name__ == "__main__":
    main()
