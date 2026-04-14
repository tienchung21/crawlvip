
import os
import sys

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    # Handle if database.py is not in craw/ but root? No, verified it is in craw/database.py
    sys.path.append(os.path.join(os.getcwd(), 'craw'))
    from database import Database

def main():
    print("Checking Schema and Sample URLs...")
    db = Database()
    conn = db.get_connection()
    if not conn:
        print("DB Connection Failed")
        return

    cur = conn.cursor()
    
    # 1. Check Columns
    print("Columns in collected_links:")
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'collected_links';
    """)
    cols = cur.fetchall()
    for c in cols:
        # Handle Tuple or Dict
        if isinstance(c, dict):
            print(f" - {c['column_name']} ({c['data_type']})")
        else:
            print(f" - {c[0]} ({c[1]})")
            
    # 2. Sample URLs to verify mapping (Batdongsan Only)
    print("\nSample URLs (Batdongsan Only) (Top 20):")
    query = "SELECT url FROM collected_links WHERE domain = 'batdongsan.com.vn' OR url LIKE '%batdongsan.com.vn%' LIMIT 20;"
    cur.execute(query)
    rows = cur.fetchall()
    for r in rows:
        if isinstance(r, dict):
             print(f" - {r['url']}")
        else:
             print(f" - {r[0]}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
