
import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== CHECKING MERGE STATUS (HCM) ===\n")
    
    # 1. Total HCM Wards in BDS Data
    # Note: Using city_code 'SG' as identified in previous step
    cursor.execute("SELECT COUNT(*) FROM location_batdongsan WHERE city_code = 'SG'")
    total = cursor.fetchone()[0]
    
    # 2. Mapped Wards
    cursor.execute("SELECT COUNT(*) FROM location_batdongsan WHERE city_code = 'SG' AND cafeland_id IS NOT NULL")
    mapped = cursor.fetchone()[0]
    
    # 3. Unmapped
    unmapped = total - mapped
    
    print(f"Total Wards (BDS): {total}")
    print(f"Mapped Wards:      {mapped}")
    print(f"Unmapped Wards:    {unmapped}")
    
    if total > 0:
        percent = (mapped / total) * 100
        print(f"Percentage:        {percent:.2f}%")
    else:
        print("Percentage: N/A (Total is 0)")
        
    print("\n--- Unmapped Wards Samples ---")
    if unmapped > 0:
        cursor.execute("SELECT ward_name, district_name FROM location_batdongsan WHERE city_code = 'SG' AND cafeland_id IS NULL LIMIT 20")
        fails = cursor.fetchall()
        for w, d in fails:
            print(f"- {w} ({d})")
            
    conn.close()

if __name__ == "__main__":
    run()
