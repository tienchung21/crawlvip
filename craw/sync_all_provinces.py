import pymysql
import re
import unicodedata
import time

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

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
    name = name.replace('\xa0', ' ').replace('\t', ' ')
    name = name.lower().strip()
    name = remove_accents(name)
    
    prefixes = [
        'phuong ', 'xa ', 'thitran ', 'thi tran ', 'quan ', 'huyen ', 
        'thanh pho ', 'tp ', 'thi xa ', 'tx ', 'tinh '
    ]
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
            
    if name.isdigit():
        name = str(int(name))
    return name.strip()

def get_province_mapping():
    print("Mapping Provinces...")
    cursor.execute("SELECT region_id, name FROM location_detail WHERE level = 1")
    nt_provs = cursor.fetchall()
    
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
    cf_provs = cursor.fetchall()
    
    mapping = [] 
    
    for nt_id, nt_name in nt_provs:
        nt_norm = normalize_name(nt_name)
        match = None
        for cf_id, cf_name in cf_provs:
            cf_norm = normalize_name(cf_name)
            if nt_norm == cf_norm:
                match = cf_id
                break
            if (nt_norm in cf_norm or cf_norm in nt_norm) and len(nt_norm) > 5:
                 match = cf_id
                 break
        
        if match:
            mapping.append((nt_id, match, nt_name))
        else:
            try: print(f"  [WARN] Could not map Province: {nt_name}")
            except: print("  [WARN] Could not map Province (encoding error)")
            
    print(f"Mapped {len(mapping)}/{len(nt_provs)} provinces.")
    return mapping

def sync_province(nt_pid, cf_pid, prov_name):
    # 1. Load Cafeland Wards
    cursor.execute(f"""
        SELECT w.city_id, w.city_title, d.city_title
        FROM transaction_city w
        JOIN transaction_city d ON w.city_parent_id = d.city_id
        WHERE w.city_parent_id IN (
            SELECT city_id FROM transaction_city WHERE city_parent_id = {cf_pid} OR city_id = {cf_pid}
        )
    """)
    rows = cursor.fetchall()
    
    cf_candidates = []
    for r in rows:
        cf_candidates.append({
            'id': r[0], 'name': r[1], 'norm': normalize_name(r[1]),
            'dname': r[2], 'dnorm': normalize_name(r[2])
        })
        
    # 2. Load Nhatot Wards (Only Unmatched)
    cursor.execute(f"""
        SELECT ward_id, name, area_id 
        FROM location_detail 
        WHERE region_id = {nt_pid} AND level = 3 AND cafeland_id IS NULL
    """)
    nt_wards = cursor.fetchall()
    
    if not nt_wards:
        return 0, 0, 0

    cursor.execute(f"SELECT area_id, name FROM location_detail WHERE region_id = {nt_pid} AND level = 2")
    nt_dist_map = {r[0]: normalize_name(r[1]) for r in cursor.fetchall()}
    
    updates = []
    fails = 0
    
    for w_id, w_name, d_id in nt_wards:
        nt_wnorm = normalize_name(w_name)
        nt_dnorm = nt_dist_map.get(d_id, "")
        
        match_id = None
        
        possible_cands = [c for c in cf_candidates if c['dnorm'] == nt_dnorm or 
                          (nt_dnorm in c['dnorm'] and len(nt_dnorm)>3) or
                          (c['dnorm'] in nt_dnorm and len(c['dnorm'])>3)]
        
        search_pool = possible_cands if possible_cands else cf_candidates
        
        for cand in search_pool:
            if cand['norm'] == nt_wnorm:
                match_id = cand['id']
                break
            if len(nt_wnorm) > 3 and (cand['norm'] == nt_wnorm or nt_wnorm in cand['norm'] or cand['norm'] in nt_wnorm):
                 match_id = cand['id']
                 break
        
        if match_id:
            updates.append((match_id, nt_pid, d_id, w_id))
        else:
            fails += 1

    if updates:
        cursor.executemany("""
            UPDATE location_detail 
            SET cafeland_id = %s 
            WHERE region_id = %s AND area_id = %s AND ward_id = %s
        """, updates)
        conn.commit()
        
    return len(nt_wards), len(updates), fails

# === MAIN ===
print("=== STARTING FULL COUNTRY SYNC (SAFE) ===")
prov_map = get_province_mapping()
total_fixed = 0
total_processed = 0

with open("sync_all_log.txt", "w", encoding="utf-8") as f:
    for idx, (nt_pid, cf_pid, p_name) in enumerate(prov_map):
        pid_str = f"Province {idx+1}/{len(prov_map)} (ID {nt_pid})"
        print(f"Processing {pid_str}...")
        try:
             total, fixed, failed = sync_province(nt_pid, cf_pid, p_name)
             log_line = f"{p_name}: {fixed}/{total} (Fail: {failed})"
             f.write(log_line + "\n")
             total_fixed += fixed
             total_processed += total
        except Exception as e:
            err_msg = f"Error processing {pid_str}: {e}"
            print(err_msg)
            f.write(err_msg + "\n")

print(f"\nDONE! Total Records Processed: {total_processed}")
print(f"Total Updated: {total_fixed}")
print("Detailed log saved to sync_all_log.txt")

cursor.close()
conn.close()
