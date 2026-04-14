
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
    s = s.replace('đ', 'd').replace('Đ', 'd').replace('Ð', 'd').replace('ð', 'd') # Handle both standard and eth like chars
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
            
    if name == 'tphcm': return 'ho chi minh'
    
    if name.isdigit():
        name = str(int(name))
    return name.strip()

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== MAPPING BATDONGSAN LOCATIONS TO TRANSACTION_CITY (ALL 63 PROVINCES) ===\n")
    
    # 0. Alter Table if needed
    try:
        cursor.execute("DESCRIBE location_batdongsan")
        cols = [r[0] for r in cursor.fetchall()]
        if 'cafeland_ward_id_old' not in cols:
            # We assume it exists or was handled by external script.
            # But let's be safe:
            pass
        if 'cafeland_id' in cols and 'cafeland_ward_id_old' not in cols:
             # Legacy
             cursor.execute("ALTER TABLE location_batdongsan CHANGE COLUMN cafeland_id cafeland_ward_id_old INT DEFAULT NULL")

    except Exception as e:
        print(f"Schema Check Error: {e}")

    # 1. Get All Cities from BDS
    cursor.execute("SELECT DISTINCT city_code, city_name FROM location_batdongsan")
    bds_cities = cursor.fetchall()
    print(f"Found {len(bds_cities)} Cities/Provinces in BDS Data.")
    
    # Get All System Cities (Level 1)
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
    sys_cities = cursor.fetchall()
    sys_city_lookup = {normalize_name(c[1]): c[0] for c in sys_cities}
    
    total_wards_processed = 0
    total_wards_mapped = 0
    
    for city_code, city_name in bds_cities:
        # if city_code != 'SG': continue # Debug Mode
        
        print(f"\n>>> Processing: {city_name} (Code: {city_code})")
        
        # Map City
        norm_c_name = normalize_name(city_name)
        sys_city_id = sys_city_lookup.get(norm_c_name)
        
        if not sys_city_id:
            # Fuzzy City
            for sys_norm, sys_id in sys_city_lookup.items():
                if len(norm_c_name) > 3 and (norm_c_name in sys_norm or sys_norm in norm_c_name):
                    sys_city_id = sys_id
                    break
        
        if not sys_city_id:
            print(f"  [ERROR] System City Not Found for {city_name}")
            continue
            
        # Update Province ID? No, we do it per Ward now for accuracy.
        # But we could update Old Province ID if needed. User asked for Ward Columns mainly.
        # Let's skip batch Province update, logic is inside Ward Loop now.

        
        # 2. Map Districts 
        cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = %s", (sys_city_id,))
        sys_dists = cursor.fetchall()
        sys_dist_lookup = {normalize_name(d[1]): d[0] for d in sys_dists}
        
        cursor.execute(f"SELECT DISTINCT district_id, district_name FROM location_batdongsan WHERE city_code = %s", (city_code,))
        bds_dists = cursor.fetchall()
        
        dist_map = {} 
        for d_id, d_name in bds_dists:
            norm = normalize_name(d_name)
            match_id = sys_dist_lookup.get(norm)
            if not match_id:
                for sys_norm, sys_id in sys_dist_lookup.items():
                    if len(norm) > 2 and (norm in sys_norm or sys_norm in norm):
                        match_id = sys_id
                        break
            if match_id:
                dist_map[d_id] = match_id
        
        # SPECIAL MAPPING for Thu Duc (Only HCM)
        special_mapping = {}
        if city_code == 'SG': # HCM
            cursor.execute("SELECT city_id FROM transaction_city WHERE city_parent_id = %s AND city_title IN ('Quận 2', 'Quận 9', 'Thủ Đức')", (sys_city_id,))
            thuduc_targets = [r[0] for r in cursor.fetchall()]
            special_mapping['thuc duc'] = thuduc_targets
            special_mapping['thu duc'] = thuduc_targets
            special_mapping['tp thu duc'] = thuduc_targets

        # 3. Map Wards in this City
        cursor.execute(f"SELECT ward_id, ward_name, district_id, district_name FROM location_batdongsan WHERE city_code = %s", (city_code,))
        bds_wards = cursor.fetchall()
        
        ward_updates = []
        
        for w_id, w_name, d_id, d_name in bds_wards:
            total_wards_processed += 1
            
            target_dist_ids = []
            if dist_map.get(d_id):
                target_dist_ids.append(dist_map.get(d_id))
                
            norm_d_name = normalize_name(d_name)
            if norm_d_name in special_mapping:
                target_dist_ids.extend(special_mapping[norm_d_name])
            
            target_dist_ids = list(set(target_dist_ids))
            
            if not target_dist_ids:
                # print(f"  [Skip Ward] District Unmapped: {d_name}")
                continue
                
            match_id = None
            
            # Start Search Trans City
            for t_id in target_dist_ids:
                cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = %s", (t_id,))
                sys_wards = cursor.fetchall()
                sys_ward_lookup = {normalize_name(w[1]): w[0] for w in sys_wards}
                
                norm = normalize_name(w_name)
                m = sys_ward_lookup.get(norm)
                if not m:
                     for sys_norm, sys_id in sys_ward_lookup.items():
                        if len(norm) > 2 and (norm in sys_norm or sys_norm in norm):
                            m = sys_id
                            break
                if m:
                    match_id = m
                    break 
            
            # Fallback Search Trans City Merge
            if not match_id and target_dist_ids:
                 placeholders = ','.join(['%s'] * len(target_dist_ids))
                 cursor.execute(f"""
                    SELECT new_city_id, old_city_name 
                    FROM transaction_city_merge 
                    WHERE new_city_parent_id IN ({placeholders})
                 """, tuple(target_dist_ids))
                 merge_rules = cursor.fetchall()
                 merge_lookup = {normalize_name(r[1]): r[0] for r in merge_rules}
                 
                 norm = normalize_name(w_name)
                 m = merge_lookup.get(norm)
                 if m:
                     match_id = m
                     
            if match_id:
                # Step 2: Lookup NEW ID from Merge Table
                # Query: SELECT new_city_id, new_city_parent_id, (SELECT city_title FROM transaction_city WHERE city_id = new_city_id) as new_name 
                #        FROM transaction_city_merge WHERE old_city_id = match_id
                
                new_id = None
                new_name = None
                new_prov = None
                
                cursor.execute("""
                    SELECT m.new_city_id, m.new_city_parent_id, c.city_title
                    FROM transaction_city_merge m
                    JOIN transaction_city c ON c.city_id = m.new_city_id
                    WHERE m.old_city_id = %s
                """, (match_id,))
                merge_row = cursor.fetchone()
                
                if merge_row:
                    new_id = merge_row[0]
                    dist_id = merge_row[1]
                    new_name = merge_row[2]
                else:
                    # Fallback: If not in Merge Table, assume Self-Mapped (Old = New)
                    # Verify by querying transaction_city directly
                    cursor.execute("SELECT city_id, city_parent_id, city_title FROM transaction_city WHERE city_id = %s", (match_id,))
                    self_row = cursor.fetchone()
                    if self_row:
                        new_id = self_row[0]
                        dist_id = self_row[1]
                        new_name = self_row[2]
                
                # Get Province ID Robustly
                new_prov = None
                if new_id:
                     # Start from the Parent in Merge table (which might be District OR Province)
                     # Or even better, start from new_id (Ward) and go up
                     
                     # Helper to traverse up
                     def get_top_parent(curr_id):
                         if not curr_id: return None
                         cursor.execute("SELECT city_id, city_parent_id FROM transaction_city WHERE city_id = %s", (curr_id,))
                         row = cursor.fetchone()
                         if not row: return None
                         cid, pid = row
                         if pid == 0: return cid
                         return get_top_parent(pid)

                     # If we have district_id from merge table, start there
                     start_node = dist_id if dist_id else new_id
                     # Actually, merging table has new_city_parent_id (dist_id) and new_district_id (maybe real district).
                     # Merge 742 has: new_city_parent_id=1 (Hanoi), new_district_id=75 (Dong Da).
                     # If we start at 1, parent is 0, returns 1. Correct.
                     # If we start at 75, parent is 1, recurse -> 1. Correct.
                     # So starting at dist_id (which is 1 or 75) works.
                     new_prov = get_top_parent(dist_id)
                
                # Append to Updates: (old, new, name_new, prov_new, w_id)
                ward_updates.append((match_id, new_id, new_name, new_prov, w_id))
                total_wards_mapped += 1
            else:
                pass
                
        if ward_updates:
            # Update Multiple Columns
            cursor.executemany("""
                UPDATE location_batdongsan 
                SET cafeland_ward_id_old = %s, 
                    cafeland_ward_id_new = %s, 
                    cafeland_ward_name_new = %s,
                    cafeland_province_id_new = %s
                WHERE ward_id = %s
            """, ward_updates)
            conn.commit()
            print(f"  -> Updated {len(ward_updates)}/{len(bds_wards)} Wards.")
        else:
            print("  -> No wards updated.")

    print(f"\n=== GLOBAL SUMMARY ===")
    print(f"Total Wards Processed: {total_wards_processed}")
    print(f"Total Wards Mapped:    {total_wards_mapped}")
    if total_wards_processed > 0:
        print(f"Global Success Rate:   {(total_wards_mapped/total_wards_processed)*100:.2f}%")
        
    conn.close()

if __name__ == "__main__":
    run()
