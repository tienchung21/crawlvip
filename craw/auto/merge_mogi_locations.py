# Mogi Location Merge Script
# Maps location_mogi to transaction_city and inserts into transaction_city_merge

import pymysql
import re
import unicodedata

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def remove_accents(s):
    if not s: return ""
    s = str(s)
    s = s.replace('đ', 'd').replace('Đ', 'd')
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not name: return ""
    if '(' in name:
        name = name.split('(')[0]
    name = re.sub(r'\s+', ' ', name)
    name = name.lower().strip()
    name = remove_accents(name)
    
    prefixes = ['thanh pho ', 'tinh ', 'quan ', 'huyen ', 'thi xa ', 'tx ', 
                'phuong ', 'xa ', 'thi tran ', 'tt ', 'tp. ', 'tp ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    
    # Special alias
    if name == 'tphcm':
        name = 'ho chi minh'
    
    if name.isdigit():
        name = str(int(name))
    return name.strip()

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== MAPPING MOGI LOCATIONS TO TRANSACTION_CITY (Full Merge) ===")
    
    # ---- PHASE 1: MAP CITIES ----
    print("\n[PHASE 1] Mapping Cities/Provinces...")
    cursor.execute("SELECT id, mogi_id, name FROM location_mogi WHERE type = 'CITY'")
    mogi_cities = cursor.fetchall()
    
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
    sys_cities = cursor.fetchall()
    sys_city_lookup = {normalize_name(c[1]): c[0] for c in sys_cities}
    
    city_map = {}  # mogi_id -> cafeland_id
    city_updates = []
    
    for pk_id, mogi_id, name in mogi_cities:
        norm = normalize_name(name)
        match_id = sys_city_lookup.get(norm)
        
        # Fuzzy match
        if not match_id:
            for sys_norm, sys_id in sys_city_lookup.items():
                if len(norm) > 4 and (norm in sys_norm or sys_norm in norm):
                    match_id = sys_id
                    break
        
        if match_id:
            city_map[mogi_id] = match_id
            city_updates.append((match_id, pk_id))
        else:
            print(f"  [WARN] Unmatched City: {name}")
    
    if city_updates:
        cursor.executemany("UPDATE location_mogi SET cafeland_id = %s WHERE id = %s", city_updates)
        conn.commit()
        print(f"  -> Updated {len(city_updates)} Cities.")
    
    # ---- PHASE 2: MAP DISTRICTS ----
    print("\n[PHASE 2] Mapping Districts...")
    
    # Get all mapped cities
    cursor.execute("SELECT mogi_id, cafeland_id, name FROM location_mogi WHERE type = 'CITY' AND cafeland_id IS NOT NULL")
    mapped_cities = cursor.fetchall()
    
    dist_updates = []
    dist_fails = []
    
    for p_mogi_id, p_sys_id, p_name in mapped_cities:
        # Get Mogi Districts
        cursor.execute("SELECT id, mogi_id, name FROM location_mogi WHERE type = 'DISTRICT' AND parent_id = %s", (p_mogi_id,))
        mogi_dists = cursor.fetchall()
        
        # Get System Districts
        cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = %s", (p_sys_id,))
        sys_dists = cursor.fetchall()
        sys_dist_lookup = {normalize_name(d[1]): d[0] for d in sys_dists}
        
        for pk_id, mogi_id, name in mogi_dists:
            norm = normalize_name(name)
            match_id = sys_dist_lookup.get(norm)
            
            # Fuzzy match
            if not match_id:
                for sys_norm, sys_id in sys_dist_lookup.items():
                    if len(norm) > 2 and (norm in sys_norm or sys_norm in norm):
                        match_id = sys_id
                        break
            
            if match_id:
                dist_updates.append((match_id, pk_id))
            else:
                dist_fails.append(f"{name} (in {p_name})")
    
    if dist_updates:
        cursor.executemany("UPDATE location_mogi SET cafeland_id = %s WHERE id = %s", dist_updates)
        conn.commit()
        print(f"  -> Updated {len(dist_updates)} Districts.")
    print(f"  -> Unmatched: {len(dist_fails)}")
    if dist_fails and len(dist_fails) <= 10:
        for f in dist_fails:
            print(f"     - {f}")
    
    # ---- PHASE 3: MAP WARDS ----
    print("\n[PHASE 3] Mapping Wards...")
    
    # Get all mapped districts
    cursor.execute("SELECT mogi_id, cafeland_id, name FROM location_mogi WHERE type = 'DISTRICT' AND cafeland_id IS NOT NULL")
    mapped_dists = cursor.fetchall()
    
    ward_updates = []
    ward_fails = []
    
    for d_mogi_id, d_sys_id, d_name in mapped_dists:
        # Get Mogi Wards
        cursor.execute("SELECT id, mogi_id, name FROM location_mogi WHERE type = 'WARD' AND parent_id = %s", (d_mogi_id,))
        mogi_wards = cursor.fetchall()
        
        if not mogi_wards:
            continue
        
        # Get System Wards
        cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = %s", (d_sys_id,))
        sys_wards = cursor.fetchall()
        sys_ward_lookup = {normalize_name(w[1]): w[0] for w in sys_wards}
        
        for pk_id, mogi_id, name in mogi_wards:
            norm = normalize_name(name)
            match_id = sys_ward_lookup.get(norm)
            
            # Fuzzy match
            if not match_id:
                for sys_norm, sys_id in sys_ward_lookup.items():
                    if len(norm) > 2 and (norm in sys_norm or sys_norm in norm):
                        match_id = sys_id
                        break
            
            if match_id:
                ward_updates.append((match_id, pk_id))
            else:
                ward_fails.append(f"{name} (in {d_name})")
    
    if ward_updates:
        cursor.executemany("UPDATE location_mogi SET cafeland_id = %s WHERE id = %s", ward_updates)
        conn.commit()
        print(f"  -> Updated {len(ward_updates)} Wards.")
    print(f"  -> Unmatched: {len(ward_fails)}")
    
    # ---- PHASE 4: INSERT INTO TRANSACTION_CITY_MERGE ----
    print("\n[PHASE 4] Inserting into transaction_city_merge...")
    
    # Get all mapped Mogi locations
    cursor.execute("""
        SELECT mogi_id, cafeland_id, name, type 
        FROM location_mogi 
        WHERE cafeland_id IS NOT NULL AND type != 'STREET'
    """)
    all_mapped = cursor.fetchall()
    
    insert_count = 0
    for mogi_id, cf_id, name, loc_type in all_mapped:
        # Check if already exists
        cursor.execute("SELECT id FROM transaction_city_merge WHERE old_city_id = %s", (cf_id,))
        if cursor.fetchone():
            continue  # Already in merge table
        
        # Get parent info
        cursor.execute("SELECT city_parent_id FROM transaction_city WHERE city_id = %s", (cf_id,))
        parent_row = cursor.fetchone()
        parent_id = parent_row[0] if parent_row else 0
        
        # Insert (self-mapping for now, old=new)
        cursor.execute("""
            INSERT INTO transaction_city_merge 
            (new_city_id, old_city_id, new_city_parent_id, old_city_parent_id, new_city_name, old_city_name)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (cf_id, cf_id, parent_id, parent_id, name, name))
        insert_count += 1
    
    conn.commit()
    print(f"  -> Inserted {insert_count} new rows into transaction_city_merge.")
    
    # Summary
    print("\n=== SUMMARY ===")
    cursor.execute("SELECT COUNT(*) FROM location_mogi WHERE cafeland_id IS NOT NULL AND type = 'CITY'")
    print(f"Cities Mapped: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM location_mogi WHERE cafeland_id IS NOT NULL AND type = 'DISTRICT'")
    print(f"Districts Mapped: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM location_mogi WHERE cafeland_id IS NOT NULL AND type = 'WARD'")
    print(f"Wards Mapped: {cursor.fetchone()[0]}")
    
    cursor.close()
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    run()
