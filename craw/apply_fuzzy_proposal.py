
import pymysql
import csv
import sys
import os

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'
INPUT_FILE = 'fuzzy_proposal.csv'

def run():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print(f"=== APPLYING FUZZY PROPOSAL FROM {INPUT_FILE} ===\n")
    
    # Check Columns First: BDS_Location, Map, Cafeland_Suggestion, Score, BDS_ID, Sys_Ward_ID
    # We need BDS_ID (ward_id in BDS table) and Sys_Ward_ID (city_id in Trans table).
    
    updates = []
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        # Verify header structure/index
        # Usually: BDS_Location(0), Map(1), Cafeland_Suggestion(2), Score(3), BDS_ID(4), Sys_Ward_ID(5)
        try:
             bds_id_idx = header.index("BDS_ID")
             sys_id_idx = header.index("Sys_Ward_ID")
        except ValueError:
             # Fallback indices
             bds_id_idx = 4
             sys_id_idx = 5
             
        for row in reader:
            if not row: continue
            try:
                bds_id = int(row[bds_id_idx])
                sys_id = int(row[sys_id_idx])
                updates.append((bds_id, sys_id))
            except Exception as e:
                print(f"Skipping Row: {row} ({e})")
                
    print(f"Found {len(updates)} approved mappings.")
    
    if not updates:
        print("No updates to apply.")
        return
        
    print("Resolving Metadata (Name, Province) for System IDs...")
    
    # Fetch Metadata for all Sys IDs
    # We need: Sys Name, Sys Province ID.
    sys_ids = [u[1] for u in updates]
    # Chunk metadata fetch
    
    meta_map = {} # sys_id -> {name, prov_id}
    
    # Helper for Prov ID
    def get_top_parent(curr_id):
         # We can't fetch one by one efficiently.
         # But script is running locally.
         # Let's just fetch ALL info first?
         pass

    # Better: Query transaction_city IN (sys_ids)
    # Then for each, recursively find Prov?
    # Or load efficient tree.
    
    # Let's load ALL cities into memory mapping (id -> parent, title)
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city")
    all_cities = {r[0]: {'title': r[1], 'parent': r[2]} for r in cursor.fetchall()}
    
    def find_prov(cid):
        if cid not in all_cities: return 0
        p_id = all_cities[cid]['parent']
        if p_id == 0: return cid
        return find_prov(p_id)
        
    final_updates = []
    for bds_id, sys_id in updates:
        if sys_id not in all_cities:
            print(f"  [Warn] Sys ID {sys_id} not found in DB! Skipping.")
            continue
            
        sys_name = all_cities[sys_id]['title']
        sys_prov = find_prov(sys_id)
        
        # Tuple: (old_id, new_id, new_name, new_prov, target_bds_id)
        # cafeland_ward_id_old = sys_id (Mapped)
        # cafeland_ward_id_new = sys_id
        final_updates.append((sys_id, sys_id, sys_name, sys_prov, bds_id))
        
    print(f"Ready to Update {len(final_updates)} rows in Database.")
    
    cursor.executemany("""
        UPDATE location_batdongsan 
        SET cafeland_ward_id_old = %s,
            cafeland_ward_id_new = %s,
            cafeland_ward_name_new = %s,
            cafeland_province_id_new = %s
        WHERE ward_id = %s
    """, final_updates)
    
    conn.commit()
    print("Update Complete.")
    conn.close()

if __name__ == "__main__":
    run()
