#!/usr/bin/env python3
"""
Remap location_mogi:
1. Map location_mogi -> transaction_city by name (get old cafeland_id)
2. Use transaction_city_merge to convert old_city_id -> new_city_id
"""

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
    
    if name == 'tphcm':
        name = 'ho chi minh'
    
    if name.isdigit():
        name = str(int(name))
    return name.strip()

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, 
                          database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== REMAP LOCATION_MOGI (2-step: transaction_city -> transaction_city_merge) ===")
    
    # ---- STEP 1: Load transaction_city (old cafeland IDs) ----
    print("\n[STEP 1] Loading transaction_city (old cafeland data)...")
    
    # PROVINCES (parent = 0)
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
    old_provinces = cursor.fetchall()
    old_province_lookup = {}
    for cid, cname in old_provinces:
        norm = normalize_name(cname)
        old_province_lookup[norm] = {'old_id': cid, 'name': cname}
    print(f"  Loaded {len(old_provinces)} provinces from transaction_city")
    
    # ---- STEP 2: Load transaction_city_merge (old -> new mapping) ----
    print("\n[STEP 2] Loading transaction_city_merge (old->new mapping)...")
    cursor.execute("""
        SELECT old_city_id, new_city_id, new_city_name, new_city_parent_id, action_type
        FROM transaction_city_merge
    """)
    merge_rows = cursor.fetchall()
    
    # Build lookup: old_city_id -> new info
    # action_type = 0: original record, action_type = 1: redirect
    merge_lookup = {}
    for old_id, new_id, new_name, new_parent, action_type in merge_rows:
        if old_id not in merge_lookup:
            merge_lookup[old_id] = {'new_id': new_id, 'new_name': new_name, 'new_parent': new_parent}
        # Prefer action_type = 0 over action_type = 1
        elif action_type == 0:
            merge_lookup[old_id] = {'new_id': new_id, 'new_name': new_name, 'new_parent': new_parent}
    print(f"  Loaded {len(merge_lookup)} mappings from transaction_city_merge")
    
    # ---- PHASE 1: MAP CITIES ----
    print("\n[PHASE 1] Mapping Cities/Provinces...")
    cursor.execute("SELECT id, mogi_id, name FROM location_mogi WHERE type = 'CITY'")
    mogi_cities = cursor.fetchall()
    
    city_map = {}  # mogi_id -> {old_id, new_id, new_name, new_parent}
    city_updates = []
    city_fails = []
    
    for pk_id, mogi_id, name in mogi_cities:
        norm = normalize_name(name)
        old_match = old_province_lookup.get(norm)
        
        # Fuzzy match
        if not old_match:
            for p_norm, p_data in old_province_lookup.items():
                if len(norm) > 4 and (norm in p_norm or p_norm in norm):
                    old_match = p_data
                    break
        
        if old_match:
            old_id = old_match['old_id']
            # Get new ID from merge table
            new_info = merge_lookup.get(old_id)
            if new_info:
                city_map[mogi_id] = {
                    'old_id': old_id,
                    'new_id': new_info['new_id'],
                    'new_name': new_info['new_name'],
                    'new_parent': new_info['new_parent']
                }
                city_updates.append((old_id, new_info['new_id'], new_info['new_name'], new_info['new_parent'], pk_id))
            else:
                # Not in merge table, use old_id as new_id
                city_map[mogi_id] = {'old_id': old_id, 'new_id': old_id, 'new_name': old_match['name'], 'new_parent': 0}
                city_updates.append((old_id, old_id, old_match['name'], 0, pk_id))
        else:
            city_fails.append(name)
    
    if city_updates:
        cursor.executemany("""
            UPDATE location_mogi 
            SET cafeland_id = %s, cafeland_new_id = %s, cafeland_new_name = %s, cafeland_parent_id = %s 
            WHERE id = %s
        """, city_updates)
        conn.commit()
        print(f"  -> Updated {len(city_updates)} Cities")
    if city_fails:
        print(f"  -> Unmatched: {len(city_fails)} - {city_fails[:5]}")
    
    # ---- PHASE 2: MAP DISTRICTS ----
    print("\n[PHASE 2] Mapping Districts...")
    
    # Build district lookup from transaction_city grouped by parent
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city WHERE city_parent_id > 0")
    old_districts = cursor.fetchall()
    old_dist_by_parent = {}
    for cid, cname, pid in old_districts:
        if pid not in old_dist_by_parent:
            old_dist_by_parent[pid] = []
        old_dist_by_parent[pid].append({'old_id': cid, 'name': cname, 'norm': normalize_name(cname)})
    
    cursor.execute("SELECT id, mogi_id, name, parent_id FROM location_mogi WHERE type = 'DISTRICT'")
    mogi_districts = cursor.fetchall()
    
    dist_map = {}
    dist_updates = []
    dist_fails = []
    
    for pk_id, mogi_id, name, parent_mogi_id in mogi_districts:
        norm = normalize_name(name)
        
        # Get parent province info
        parent_info = city_map.get(parent_mogi_id)
        if not parent_info:
            dist_fails.append(f"{name} (parent not mapped)")
            continue
        
        parent_old_id = parent_info['old_id']
        parent_new_id = parent_info['new_id']
        
        # Find district in transaction_city under this parent
        candidates = old_dist_by_parent.get(parent_old_id, [])
        old_match = None
        
        for c in candidates:
            if c['norm'] == norm:
                old_match = c
                break
        
        if not old_match:
            for c in candidates:
                if len(norm) > 2 and (norm in c['norm'] or c['norm'] in norm):
                    old_match = c
                    break
        
        if old_match:
            old_id = old_match['old_id']
            new_info = merge_lookup.get(old_id)
            
            if new_info:
                dist_map[mogi_id] = {
                    'old_id': old_id,
                    'new_id': new_info['new_id'],
                    'new_name': new_info['new_name'],
                    'new_parent': new_info['new_parent'],
                    'province_new_id': parent_new_id
                }
                dist_updates.append((old_id, new_info['new_id'], new_info['new_name'], new_info['new_parent'], pk_id))
            else:
                dist_map[mogi_id] = {
                    'old_id': old_id, 'new_id': old_id, 'new_name': old_match['name'],
                    'new_parent': parent_new_id, 'province_new_id': parent_new_id
                }
                dist_updates.append((old_id, old_id, old_match['name'], parent_new_id, pk_id))
        else:
            dist_fails.append(f"{name} (in {parent_old_id})")
    
    if dist_updates:
        cursor.executemany("""
            UPDATE location_mogi 
            SET cafeland_id = %s, cafeland_new_id = %s, cafeland_new_name = %s, cafeland_parent_id = %s 
            WHERE id = %s
        """, dist_updates)
        conn.commit()
        print(f"  -> Updated {len(dist_updates)} Districts")
    print(f"  -> Unmatched: {len(dist_fails)}")
    
    # ---- PHASE 3: MAP WARDS ----
    print("\n[PHASE 3] Mapping Wards...")
    
    # Build ward lookup grouped by parent district
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city")
    all_locations = cursor.fetchall()
    old_loc_by_parent = {}
    for cid, cname, pid in all_locations:
        if pid not in old_loc_by_parent:
            old_loc_by_parent[pid] = []
        old_loc_by_parent[pid].append({'old_id': cid, 'name': cname, 'norm': normalize_name(cname)})
    
    cursor.execute("SELECT id, mogi_id, name, parent_id FROM location_mogi WHERE type = 'WARD'")
    mogi_wards = cursor.fetchall()
    
    ward_updates = []
    ward_fails = []
    
    for pk_id, mogi_id, name, parent_mogi_id in mogi_wards:
        norm = normalize_name(name)
        
        parent_info = dist_map.get(parent_mogi_id)
        if not parent_info:
            ward_fails.append(f"{name} (parent district not mapped)")
            continue
        
        parent_old_id = parent_info['old_id']
        province_new_id = parent_info['province_new_id']
        
        # Find ward in transaction_city under this district
        candidates = old_loc_by_parent.get(parent_old_id, [])
        old_match = None
        
        for c in candidates:
            if c['norm'] == norm:
                old_match = c
                break
        
        if not old_match:
            for c in candidates:
                if len(norm) > 1 and (norm in c['norm'] or c['norm'] in norm):
                    old_match = c
                    break
        
        if old_match:
            old_id = old_match['old_id']
            new_info = merge_lookup.get(old_id)
            
            if new_info:
                # Ward's new_parent from merge table (should be province)
                ward_updates.append((old_id, new_info['new_id'], new_info['new_name'], new_info['new_parent'], pk_id))
            else:
                # Not in merge, use old values, parent = province
                ward_updates.append((old_id, old_id, old_match['name'], province_new_id, pk_id))
        else:
            ward_fails.append(f"{name} (in district {parent_old_id})")
    
    if ward_updates:
        cursor.executemany("""
            UPDATE location_mogi 
            SET cafeland_id = %s, cafeland_new_id = %s, cafeland_new_name = %s, cafeland_parent_id = %s 
            WHERE id = %s
        """, ward_updates)
        conn.commit()
        print(f"  -> Updated {len(ward_updates)} Wards")
    print(f"  -> Unmatched: {len(ward_fails)}")
    
    # ---- SUMMARY ----
    print("\n=== SUMMARY ===")
    cursor.execute("SELECT COUNT(*) FROM location_mogi WHERE cafeland_new_id IS NOT NULL AND type = 'CITY'")
    print(f"Cities Mapped: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM location_mogi WHERE cafeland_new_id IS NOT NULL AND type = 'DISTRICT'")
    print(f"Districts Mapped: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM location_mogi WHERE cafeland_new_id IS NOT NULL AND type = 'WARD'")
    print(f"Wards Mapped: {cursor.fetchone()[0]}")
    
    # Verify
    print("\n=== VERIFY WARD 355, 211 ===")
    cursor.execute("""
        SELECT cafeland_id, cafeland_new_id, cafeland_new_name, cafeland_parent_id, name
        FROM location_mogi 
        WHERE cafeland_new_id IN (355, 211) AND type = 'WARD'
    """)
    for row in cursor.fetchall():
        print(f"  {row}")
    
    cursor.close()
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    run()
