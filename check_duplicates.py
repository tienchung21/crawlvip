
import sys
import os
sys.path.insert(0, 'craw')
from database import Database

def check_duplicates():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("Checking for duplicate Lat/Long...")
    query = """
        SELECT lat, `long` as lng, COUNT(*) as cnt 
        FROM data_full 
        WHERE lat IS NOT NULL AND `long` IS NOT NULL
        GROUP BY lat, `long` 
        HAVING cnt > 1 
        ORDER BY cnt DESC 
        LIMIT 20
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print("\nTop 20 Duplicate Lat/Long:")
    print("-" * 50)
    for r in rows:
        lat = r['lat'] if isinstance(r, dict) else r[0]
        lng = r['lng'] if isinstance(r, dict) else r[1]
        cnt = r['cnt'] if isinstance(r, dict) else r[2]
        print(f"Lat: {lat:<15} Long: {lng:<15} -> {cnt} records")

    cursor.execute("SELECT COUNT(*) FROM data_full WHERE lat IS NOT NULL")
    total = cursor.fetchone()
    total_count = total['COUNT(*)'] if isinstance(total, dict) else total[0]
    
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT lat, `long` 
            FROM data_full 
            WHERE lat IS NOT NULL 
            GROUP BY lat, `long` 
            HAVING COUNT(*) > 1
        ) as t
    """)
    dup_groups = cursor.fetchone()
    dup_group_count = dup_groups['COUNT(*)'] if isinstance(dup_groups, dict) else dup_groups[0]
    
    print("-" * 50)
    print(f"Total records with Lat/Long: {total_count}")
    print(f"Number of coordinate locations with duplicates: {dup_group_count}")
    
    conn.close()

if __name__ == "__main__":
    check_duplicates()
