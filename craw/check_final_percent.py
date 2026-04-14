
import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== FINAL MERGE ACCURACY CHECK ===\n")
    
    # 1. Total Wards
    cursor.execute("SELECT COUNT(*) FROM location_batdongsan")
    total = cursor.fetchone()[0]
    
    # 2. Mapped Wards (Old)
    cursor.execute("SELECT COUNT(*) FROM location_batdongsan WHERE cafeland_ward_id_old IS NOT NULL")
    mapped_old = cursor.fetchone()[0]
    
    # 3. Mapped Wards (New)
    cursor.execute("SELECT COUNT(*) FROM location_batdongsan WHERE cafeland_ward_id_new IS NOT NULL")
    mapped_new = cursor.fetchone()[0]
    
    print(f"Total Wards:          {total}")
    print(f"Mapped via Name (Old):{mapped_old} ({(mapped_old/total)*100:.2f}%)")
    print(f"Mapped New ID:        {mapped_new} ({(mapped_new/total)*100:.2f}%)")
    
    print("\n--- Breakdown by status ---")
    if mapped_new < mapped_old:
        print(f"Warning: {mapped_old - mapped_new} records mapped to Old ID but failed to find New ID in Merge Table.")
    else:
        print("All Old IDs successfully resolved to New IDs.")
        
    conn.close()

if __name__ == "__main__":
    run()
