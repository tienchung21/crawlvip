import pymysql
import re
import unicodedata

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
    name = name.lower().strip()
    name = remove_accents(name)
    prefixes = ['tinh ', 'thanh pho ', 'tp ', 'quan ', 'huyen ', 'thi xa ', 'tx ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    if name.isdigit():
        name = str(int(name))
    return name.strip()

print("=== SYNCING LEVEL 1 (PROVINCE) & LEVEL 2 (DISTRICT) ===")

# --- LEVEL 1: PROVINCE ---
print("\n1. Syncing Provinces...")
cursor.execute("SELECT region_id, name FROM location_detail WHERE level = 1")
nt_provs = cursor.fetchall()

cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
cf_provs = cursor.fetchall()

p_updates = []
for pid, pname in nt_provs:
    nt_norm = normalize_name(pname)
    match_id = None
    for cid, cname in cf_provs:
        cf_norm = normalize_name(cname)
        if nt_norm == cf_norm or (len(nt_norm)>5 and (nt_norm in cf_norm or cf_norm in nt_norm)):
            match_id = cid
            break
            
    if match_id:
        p_updates.append((match_id, pid))
    else:
        print(f"  [WARN] Unmatched Prov: {pname}")

if p_updates:
    cursor.executemany("UPDATE location_detail SET cafeland_id = %s WHERE region_id = %s AND level = 1", p_updates)
    conn.commit()
    print(f"  -> Updated {len(p_updates)} Provinces.")

# --- LEVEL 2: DISTRICT ---
print("\n2. Syncing Districts...")
# Iterate via mapped provinces to reduce scope
cursor.execute("SELECT region_id, cafeland_id, name FROM location_detail WHERE level = 1 AND cafeland_id IS NOT NULL")
mapped_provs = cursor.fetchall()

total_d_fixed = 0
d_fails = []

for nt_pid, cf_pid, p_name in mapped_provs:
    # Get Nhatot Districts
    cursor.execute(f"SELECT area_id, name FROM location_detail WHERE region_id = {nt_pid} AND level = 2")
    nt_dists = cursor.fetchall()
    
    # Get Cafeland Districts
    cursor.execute(f"SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = {cf_pid} OR city_id = {cf_pid}")
    # Note: Cafeland district parent is Province (city_parent_id = cf_pid)
    # OR sometimes structure is weird? Usually parent=ProvID.
    rows = cursor.fetchall()
    # But wait, query above gets wards if logic is recursive? 
    # Prov(0) -> Dist(ProvID) -> Ward(DistID).
    # Correct query for Districts of a Province:
    cursor.execute(f"SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = {cf_pid}")
    cf_dists = cursor.fetchall()

    cf_lookup = []
    for cid, cname in cf_dists:
        cf_lookup.append({
            'id': cid, 'name': cname, 'norm': normalize_name(cname)
        })
        
    d_updates = []
    for did, dname in nt_dists:
        nt_norm = normalize_name(dname)
        match_id = None
        
        # Exact Match
        for c in cf_lookup:
            if nt_norm == c['norm']:
                match_id = c['id']
                break
        
        # Fuzzy Match
        if not match_id:
            for c in cf_lookup:
                if (nt_norm in c['norm'] or c['norm'] in nt_norm) and len(nt_norm) > 2: # 'ba' is short 
                     match_id = c['id']
                     break
        
        # Special Case: TP Thu Duc?
        if not match_id and 'thu duc' in nt_norm:
            # Map to one of them? No, leave NULL is better because it is many.
            pass
            
        if match_id:
            d_updates.append((match_id, did))
        else:
            d_fails.append(f"{dname} ({p_name})")

    if d_updates:
        cursor.executemany("UPDATE location_detail SET cafeland_id = %s WHERE area_id = %s AND level = 2", d_updates)
        conn.commit()
        total_d_fixed += len(d_updates)

print(f"  -> Updated {total_d_fixed} Districts.")
print(f"  -> Unmatched Districts: {len(d_fails)}")
if d_fails:
    with open("unmatched_level2.txt", "w", encoding="utf-8") as f:
         for x in d_fails: f.write(x + "\n")
    print("  -> Saved unmatched to unmatched_level2.txt")

cursor.close()
conn.close()
