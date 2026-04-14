
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
    
    prefixes = ['thanh pho ', 'tinh ', 'quan ', 'huyen ', 'thi xa ', 'tx ', 'phuong ', 'xa ', 'thi tran ', 'tt ', 'tp ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
            
    # Remove leading zeros in numbers? e.g. '01' -> '1'
    # "phuong 01" -> "phuong 1" matches "phuong 1"
    # But normalization removed prefix. So "01" -> "1".
    name = re.sub(r'\b0+(\d+)', r'\1', name)
    
    return name.strip()

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== ANALYZING UNMAPPED WARDS ===\n")
    
    # Get Unmapped
    cursor.execute("""
        SELECT ward_id, ward_name, district_name, city_name, cafeland_province_id 
        FROM location_batdongsan 
        WHERE cafeland_ward_id_new IS NULL
    """)
    unmapped = cursor.fetchall()
    print(f"Found {len(unmapped)} Unmapped Rows.")
    
    # Cache System Wards by District
    # We need mapping from BDS District -> Sys District ID first.
    # Can we get Sys District ID using cafeland_province_id?
    # BDS table doesn't store District ID map yet (I calculated it in memory).
    # But I can retry Mapping District here.
    
    # Let's verify against ALL Wards in the Province.
    # Get All Sys Wards grouped by Province.
    
    # Get All System Data mapped by Province -> District -> Ward
    # Cache: prov_id -> { dist_name_norm: { ward_name_norm: ward_id } } 
    # Actually just load all Wards with their IDs, Names, Parent(District), Parent(Province).
    
    print("Loading System Wards...")
    # Select Ward, District, Province
    cursor.execute("""
        SELECT w.city_id, w.city_title, d.city_id as dist_id, d.city_title as dist_title, p.city_id as prov_id
        FROM transaction_city w
        JOIN transaction_city d ON w.city_parent_id = d.city_id
        JOIN transaction_city p ON d.city_parent_id = p.city_id
        WHERE w.city_parent_id > 0
    """)
    sys_data = cursor.fetchall()
    
    # Organize by Province -> District
    # prov_id -> dist_id -> list of (ward_id, ward_name_norm, ward_name_raw)
    sys_tree = {}
    
    for w_id, w_title, d_id, d_title, p_id in sys_data:
        if p_id not in sys_tree: sys_tree[p_id] = {}
        if d_id not in sys_tree[p_id]: sys_tree[p_id][d_id] = {'title': d_title, 'wards': []}
        
        w_norm = normalize_name(w_title)
        sys_tree[p_id][d_id]['wards'].append((w_id, w_norm, w_title))
        
    print(f"Loaded System Tree for {len(sys_tree)} Provinces.")
    
    match_candidates = []
    
    # Create Prov Name Lookup
    prov_name_lookup = {}
    for pid, data in sys_tree.items():
        # Any district title? No, we need Prov Title.
        # Sys Data query returned w.city, d.city, p.city.
        # Let's extract Province Title from sys_data.
        # But sys_data organized by rows.
        pass
        
    # Query Provinces explicitly to build lookup
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
    prov_rows = cursor.fetchall()
    prov_lookup = {}
    for pid, ptitle in prov_rows:
        prov_lookup[normalize_name(ptitle)] = pid
    
    count = 0
    for w_id, w_name, d_name, c_name, prov_id in unmapped:
        count += 1
        
        real_prov_id = prov_id
        if not real_prov_id:
            # Lookup by Name
            norm_c = normalize_name(c_name)
            real_prov_id = prov_lookup.get(norm_c)
            # Fuzzy Prov?
            if not real_prov_id:
                for p_norm, pid in prov_lookup.items():
                     if p_norm in norm_c or norm_c in p_norm:
                         real_prov_id = pid
                         break
                         
        if not real_prov_id:
             print(f"  [Skip] Prov Not Found: {c_name}")
             continue
             
        if real_prov_id not in sys_tree:
            # Maybe sys_tree query filtered only provs with Wards?
            # It joined p.city_id. It should be fine.
            continue
            
        prov_data = sys_tree[real_prov_id]
        
        # 1. Find District Match
        d_norm = normalize_name(d_name)
        best_d_id = None
        best_d_score = 0
        
        # Exact/Fuzzy District Match
        for sys_d_id, info in prov_data.items():
            sys_d_norm = normalize_name(info['title'])
            if d_norm == sys_d_norm:
                best_d_id = sys_d_id
                best_d_score = 1.0
                break
            score = similarity(d_norm, sys_d_norm)
            if score > best_d_score:
                best_d_score = score
                best_d_id = sys_d_id
                
        if not best_d_id or best_d_score < 0.8:
            # District mismatch?
            print(f"  [Skip] District mismatch: '{d_name}' vs {list(prov_data.keys())} (Best: {best_d_score})")
            # Print available titles
            titles = [prov_data[did]['title'] for did in prov_data]
            print(f"    Available: {titles}")
            continue
            
        # 2. Find Ward Match in Best District
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
                
        if best_w_score > 0.85: # High confidence threshold
            print(f"Match: '{w_name}' ({d_name}) -> '{best_w_name}' (Score: {best_w_score:.2f}) [ID: {best_w_id}]")
            match_candidates.append((best_w_id, best_w_name, prov_id, w_id))
        elif best_d_score > 0.8:
            # Print low score match for debugging
             print(f"Low Score: '{w_name}' ({d_name}) -> '{best_w_name}' (Score: {best_w_score:.2f})")
            
    print(f"\nFound {len(match_candidates)} High Confidence Matches.")
    
    if match_candidates:
        # Ask to Apply?
        # For now just print.
        # Save to a file or Apply directly?
        pass
    
    conn.close()

if __name__ == "__main__":
    run()
