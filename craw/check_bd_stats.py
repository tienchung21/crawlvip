
import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== CHECKING MERGE STATUS (BINH DUONG) ===\n")
    
    # 1. Get City Code
    cursor.execute("SELECT DISTINCT city_code, city_name FROM location_batdongsan WHERE city_name LIKE '%Bình Dương%'")
    rows = cursor.fetchall()
    if not rows:
        print("Binh Duong not found in location_batdongsan!")
        return
        
    for code, name in rows:
        print(f"City: {name} (Code: {code})")
        
        # 2. Stats
        cursor.execute(f"SELECT COUNT(*) FROM location_batdongsan WHERE city_code = '{code}'")
        total = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM location_batdongsan WHERE city_code = '{code}' AND cafeland_id IS NOT NULL")
        mapped = cursor.fetchone()[0]
        
        unmapped = total - mapped
        percent = (mapped/total)*100 if total > 0 else 0
        
        print(f"  Total Wards: {total}")
        print(f"  Mapped:      {mapped}")
        print(f"  Unmapped:    {unmapped}")
        print(f"  Percentage:  {percent:.2f}%")
        
        if unmapped > 0:
            print("\n  [Unmapped Samples]")
            cursor.execute(f"SELECT ward_name, district_name FROM location_batdongsan WHERE city_code = '{code}' AND cafeland_id IS NULL LIMIT 10")
            for w, d in cursor.fetchall():
                print(f"  - {w} ({d})")

    conn.close()

if __name__ == "__main__":
    run()
