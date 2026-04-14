
import pymysql
import unicodedata
import re
from difflib import SequenceMatcher

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def normalize_name(name):
    if not name: return ""
    name = str(name).lower()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    
    prefixes = ['thanh pho ', 'tinh ', 'quan ', 'huyen ', 'thi xa ', 'tx ', 'phuong ', 'xa ', 'thi tran ', 'tt ', 'tp. ', 'tp ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
            
    name = re.sub(r'\b0+(\d+)', r'\1', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== APPLYING FUZZY FIXES ===\n")
    
    # query Unmapped
    cursor.execute("""
        SELECT ward_id, ward_name, district_name, city_name, cafeland_province_id_new 
        FROM location_batdongsan 
        WHERE cafeland_ward_id_new IS NULL
    """)
    unmapped = cursor.fetchall()
    
    print("Loading System Wards...")
    cursor.execute("""
        SELECT w.city_id, w.city_title, d.city_id as dist_id, d.city_title as dist_title, p.city_id as prov_id
        FROM transaction_city w
        JOIN transaction_city d ON w.city_parent_id = d.city_id
        JOIN transaction_city p ON d.city_parent_id = p.city_id
        WHERE w.city_parent_id > 0
    """)
    sys_data = cursor.fetchall()
    
    sys_tree = {}
    for w_id, w_title, d_id, d_title, p_id in sys_data:
        if p_id not in sys_tree: sys_tree[p_id] = {}
        if d_id not in sys_tree[p_id]: sys_tree[p_id][d_id] = {'title': d_title, 'wards': []}
        w_norm = normalize_name(w_title)
        sys_tree[p_id][d_id]['wards'].append((w_id, w_norm, w_title))

    # Prov Lookup
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
    prov_rows = cursor.fetchall()
    prov_lookup = {}
    for pid, ptitle in prov_rows:
        prov_lookup[normalize_name(ptitle)] = pid
        
    updates = []
    
    for w_id, w_name, d_name, c_name, prov_id in unmapped:
        real_prov_id = prov_id
        if not real_prov_id:
            norm_c = normalize_name(c_name)
            real_prov_id = prov_lookup.get(norm_c)
            if not real_prov_id:
                for p_norm, pid in prov_lookup.items():
                     if p_norm in norm_c or norm_c in p_norm:
                         real_prov_id = pid
                         break
                         
        if not real_prov_id or real_prov_id not in sys_tree:
            continue
            
        prov_data = sys_tree[real_prov_id]
        d_norm = normalize_name(d_name)
        d_norm_nospace = d_norm.replace(" ", "")
        
        best_d_id = None
        best_d_score = 0
        
        for sys_d_id, info in prov_data.items():
            sys_d_norm = normalize_name(info['title'])
            sys_d_norm_nospace = sys_d_norm.replace(" ", "")
            
            # Check NoSpace Match (for KonTum)
            if d_norm_nospace == sys_d_norm_nospace:
                best_d_id = sys_d_id
                best_d_score = 1.0
                break
                
            score = similarity(d_norm, sys_d_norm)
            if score > best_d_score:
                best_d_score = score
                best_d_id = sys_d_id
        
        if not best_d_id or best_d_score < 0.6: # Relaxed District Threshold if Ward matches well?
             continue
             
        wards = prov_data[best_d_id]['wards']
        w_norm = normalize_name(w_name)
        
        best_w_id = None
        best_w_name = None
        best_w_score = 0
        
        for sys_w_id, sys_w_norm, sys_w_raw in wards:
            score = similarity(w_norm, sys_w_norm)
            if score > best_w_score:
                best_w_score = score
                best_w_id = sys_w_id
                best_w_name = sys_w_raw
                
        # Confidence Threshold
        if best_w_score > 0.88:
             # Additional check: Length diff not too big
             if abs(len(w_norm) - len(normalize_name(best_w_name))) > 5:
                 continue
             
             print(f"Applying: '{w_name}' -> '{best_w_name}' (Score: {best_w_score:.2f})")
             # We update: cafeland_ward_id_old (Mapped as if it was old), 
             # cafeland_ward_id_new (The same, since System ID is New), 
             # cafeland_ward_name_new, cafeland_province_id_new.
             
             # Actually, if we map to System ID, we can assume it's New ID.
             updates.append((best_w_id, best_w_id, best_w_name, real_prov_id, w_id))
             
    if updates:
        print(f"Updating {len(updates)} records...")
        cursor.executemany("""
            UPDATE location_batdongsan 
            SET cafeland_ward_id_old = %s,
                cafeland_ward_id_new = %s,
                cafeland_ward_name_new = %s,
                cafeland_province_id_new = %s
            WHERE ward_id = %s
        """, updates)
        conn.commit()
        print("Done.")
    else:
        print("No high confidence updates found.")
        
    conn.close()

if __name__ == "__main__":
    run()
