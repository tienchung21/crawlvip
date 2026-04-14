
import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== UPDATING PROVINCE IDs FROM MERGE TABLE ===\n")
    
    # 1. Add Column
    try:
        cursor.execute("DESCRIBE location_batdongsan")
        cols = [r[0] for r in cursor.fetchall()]
        if 'cafeland_province_id' not in cols:
            print("Adding 'cafeland_province_id' column...")
            cursor.execute("ALTER TABLE location_batdongsan ADD COLUMN cafeland_province_id INT DEFAULT NULL, ADD INDEX (cafeland_province_id)")
            conn.commit()
    except Exception as e:
        print(f"Schema Error: {e}")

    # 2. Get All Mapped Wards
    # We can fetch distinct cafeland_id to optimize
    # Or fetch distinct city?
    # Let's fetch distinct cafeland_id (Ward) and find its Province.
    cursor.execute("SELECT DISTINCT cafeland_id FROM location_batdongsan WHERE cafeland_id IS NOT NULL")
    ward_ids = [r[0] for r in cursor.fetchall()]
    print(f"Found {len(ward_ids)} distinct Ward IDs to check.")
    
    updates = {} # ward_id -> province_id
    
    for w_id in ward_ids:
        # Lookup in Merge Table to get New District ID
        # User said: cafeland_id is "Old". Merge table maps Old -> New.
        # Find row where old_city_id = w_id
        cursor.execute("SELECT new_city_parent_id FROM transaction_city_merge WHERE old_city_id = %s", (w_id,))
        merge_row = cursor.fetchone()
        
        new_dist_id = None
        if merge_row:
             new_dist_id = merge_row[0]
        else:
             # Fallback: Maybe cafeland_id IS ALREADY New?
             # Check transaction_city parent
             cursor.execute("SELECT city_parent_id FROM transaction_city WHERE city_id = %s", (w_id,))
             city_row = cursor.fetchone()
             if city_row:
                 new_dist_id = city_row[0]
        
        if not new_dist_id:
            # print(f"  [Warn] Cannot find District for Ward {w_id}")
            continue
            
        # Get Province ID (Parent of District)
        cursor.execute("SELECT city_parent_id FROM transaction_city WHERE city_id = %s", (new_dist_id,))
        prov_row = cursor.fetchone()
        if prov_row:
            prov_id = prov_row[0]
            updates[w_id] = prov_id
            
    print(f"identified Provinces for {len(updates)} Wards.")
    
    # 3. Batch Update
    # UPDATE location_batdongsan SET cafeland_province_id = ... WHERE cafeland_id = ...
    # Group by Province ID to minimize queries?
    # UPDATE location_batdongsan SET cafeland_province_id = X WHERE cafeland_id IN (...)
    
    prov_to_wards = {}
    for w_id, p_id in updates.items():
        if p_id not in prov_to_wards: prov_to_wards[p_id] = []
        prov_to_wards[p_id].append(w_id)
        
    print(f"Updating DB ({len(prov_to_wards)} distinct provinces)...")
    
    total_updated = 0
    for p_id, w_ids in prov_to_wards.items():
        # Chunking w_ids
        chunk_size = 1000
        for i in range(0, len(w_ids), chunk_size):
            chunk = w_ids[i:i+chunk_size]
            placeholders = ','.join(['%s'] * len(chunk))
            sql = f"UPDATE location_batdongsan SET cafeland_province_id = %s WHERE cafeland_id IN ({placeholders})"
            cursor.execute(sql, (p_id, *chunk))
            total_updated += cursor.rowcount
            
    conn.commit()
    print(f"Updated {total_updated} rows.")
    conn.close()

if __name__ == "__main__":
    run()
