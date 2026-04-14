
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
        SELECT 
            CASE 
                WHEN url LIKE '%/ban-%' THEN 'Bán' 
                WHEN url LIKE '%/cho-thue-%' THEN 'Cho Thuê' 
                ELSE 'Khác' 
            END as nhom,
            loaihinh, 
            COUNT(*) as cnt
        FROM collected_links 
        WHERE domain = 'batdongsan.com.vn' 
        GROUP BY nhom, loaihinh
        ORDER BY nhom, loaihinh
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    print("CHI TIẾT LOẠI HÌNH (MUA BÁN / CHO THUÊ):")
    current_group = ""
    for r in rows:
        if isinstance(r, dict):
            group = r.get('nhom', 'Unknown')
            name = r.get('loaihinh', 'Unknown')
            cnt = r.get('cnt', 0)
        else:
            group = r[0]
            name = r[1]
            cnt = r[2]
            
        if group != current_group:
            print(f"\n--- {group} ---")
            current_group = group
            
        print(f"  - {name}: {cnt}")

if __name__ == "__main__":
    main()
