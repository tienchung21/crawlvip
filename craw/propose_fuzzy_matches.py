
import pymysql
import unicodedata
import re
from difflib import SequenceMatcher
import csv

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'
OUTPUT_FILE = 'fuzzy_proposal.csv'

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
    
    print(f"=== GENERATING FUZZY PROPOSAL ({OUTPUT_FILE}) ===\n")
    
    # 1. Unmapped
    cursor.execute("""
        SELECT ward_id, ward_name, district_name, city_name, cafeland_province_id_new 
        FROM location_batdongsan 
        WHERE cafeland_ward_id_new IS NULL
    """)
    unmapped = cursor.fetchall()
    
    # 2. Sys Data
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

    proposals = []
    
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
            
            if d_norm_nospace == sys_d_norm_nospace:
                best_d_id = sys_d_id
                best_d_score = 1.0
                break
            score = similarity(d_norm, sys_d_norm)
            if score > best_d_score:
                best_d_score = score
                best_d_id = sys_d_id
        
        if not best_d_id or best_d_score < 0.6: 
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
                
        if best_w_score > 0.88:
             if abs(len(w_norm) - len(normalize_name(best_w_name))) > 5:
                 continue
             
             # Format: BDS_Full, ARROW, Cafeland_Full, Score, IDs
             bds_loc = f"{w_name}, {d_name}, {c_name}"
             
             # Get Matched District Name
             sys_d_title = prov_data[best_d_id]['title']
             # Get Matched Prov Name
             # sys_data query returned p.city_title but we didn't store it in sys_tree easily.
             # But we can assume Prov Name is similar to c_name (or get from lookup).
             # Let's just show Ward + District.
             
             cafeland_loc = f"{best_w_name}, {sys_d_title}"
             
             proposals.append([bds_loc, "->", cafeland_loc, f"{best_w_score:.2f}", w_id, best_w_id])

    # Write CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['BDS_Location', 'Map', 'Cafeland_Suggestion', 'Score', 'BDS_ID', 'Sys_Ward_ID'])
        writer.writerows(proposals)
        
    print(f"Generated {len(proposals)} proposals in {OUTPUT_FILE}.")
    # Preview
    print("Preview first 5 lines:")
    for row in proposals[:5]:
        print(row)
    conn.close()

if __name__ == "__main__":
    run()
