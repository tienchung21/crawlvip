
import os
import sys

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    pass

def main():
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT loaihinh, COUNT(*) as cnt
        FROM collected_links 
        WHERE domain = 'batdongsan.com.vn' 
        GROUP BY loaihinh
        ORDER BY loaihinh
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    print("LOAI HINH BATDONGSAN:")
    for r in rows:
        if isinstance(r, dict):
            key = r.get('loaihinh', 'Unknown')
            cnt = r.get('cnt', 0)
        else:
            key = r[0]
            cnt = r[1]
        print(f"{key}: {cnt}")

if __name__ == "__main__":
    main()
