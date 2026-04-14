import pymysql
import re
import unicodedata

# Database Connection
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
    
    # Common prefixes to remove for better matching
    prefixes = ['thanh pho ', 'tinh ', 'quan ', 'huyen ', 'thi xa ', 'tx ', 'phuong ', 'xa ', 'thi tran ', 'tt ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
            
    # Remove ordinal number words if causing issues, but usually strict matching handles it.
    if name.isdigit():
        name = str(int(name))
    return name.strip()

print("=== MAPPING MOGI LOCATIONS TO TRANSACTION_CITY ===")

# --- LEVEL 1: CITY / PROVINCE ---
print("\n1. Mapping Cities/Provinces...")
cursor.execute("SELECT id, name FROM location_mogi WHERE type = 'CITY'")
mogi_cities = cursor.fetchall()

cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
sys_cities = cursor.fetchall()

city_updates = []
tphcm_mogi_id = None # Save for Level 2 filtering

for mid, mname in mogi_cities:
    m_norm = normalize_name(mname)
    match_id = None
    
    # Special fix for Ho Chi Minh
    if 'ho chi minh' in m_norm or m_norm == 'tphcm':
        m_norm = 'ho chi minh'

    for cid, cname in sys_cities:
        c_norm = normalize_name(cname)
        if m_norm == c_norm:
            match_id = cid
            break
        # Fuzzy match for complex names
        if len(m_norm) > 5 and (m_norm in c_norm or c_norm in m_norm):
             match_id = cid
             break
    
    if match_id:
        city_updates.append((match_id, mid))
        if 'ho chi minh' in m_norm:
            tphcm_mogi_id = mid
            print(f"  > Found TPHCM: MogiID={mid} -> SysID={match_id} ({cname})")
    else:
        print(f"  [WARN] Unmatched City: {mname}")

if city_updates:
    cursor.executemany("UPDATE location_mogi SET cafeland_id = %s WHERE id = %s", city_updates)
    conn.commit()
    print(f"  -> Updated {len(city_updates)} Cities.")

# --- LEVEL 2: DISTRICT (Filter for TPHCM as requested) ---
print("\n2. Mapping Districts (Focus: Ho Chi Minh)...")

# Get All Mogi Cities that have been mapped (or just TPHCM if restrictive)
# Using the mapping we just did:
cursor.execute("""
    SELECT m.mogi_id, m.cafeland_id, m.name 
    FROM location_mogi m 
    WHERE m.type = 'CITY' AND m.cafeland_id IS NOT NULL
""")
mapped_cities_info = cursor.fetchall()

dist_updates = []
dist_fails = []

for p_mogi_code, p_sys_id, p_name in mapped_cities_info:
    # Filter: Run only for TPHCM as requested check
    p_norm = normalize_name(p_name)
    if 'ho chi minh' not in p_norm and p_norm != 'tphcm':
        continue
        
    print(f"  Processing Province: {p_name} (SysID: {p_sys_id})...")
    
    # Get Mogi Districts for this Parent
    cursor.execute(f"SELECT id, name FROM location_mogi WHERE type = 'DISTRICT' AND parent_id = {p_mogi_code}")
    mogi_dists = cursor.fetchall()
    
    # Get System Districts for this Parent
    cursor.execute(f"SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = {p_sys_id}")
    sys_dists = cursor.fetchall()
    
    sys_lookup = [{'id': cid, 'norm': normalize_name(cname), 'raw': cname} for cid, cname in sys_dists]
    
    for d_id, d_name in mogi_dists:
        d_norm = normalize_name(d_name)
        match_id = None
        
        # Exact Match
        for item in sys_lookup:
            if d_norm == item['norm']:
                match_id = item['id']
                break
        
        # Helper for numbered districts (Quan 1, Quan 2...)
        if not match_id and d_norm.isdigit():
             for item in sys_lookup:
                 if item['norm'] == d_norm:
                     match_id = item['id']
                     break

        # Fuzzy Match
        if not match_id:
             for item in sys_lookup:
                 if len(d_norm) > 3 and (d_norm in item['norm'] or item['norm'] in d_norm):
                     match_id = item['id']
                     break
                     
        # Special alias map
        if not match_id:
             if d_norm == 'thu duc':
                 # Thu Duc City (new) vs District (old). System might have old District.
                 # Let's see if we find 'quan thu duc'
                 for item in sys_lookup:
                     if 'thu duc' in item['norm']:
                         match_id = item['id']
                         break
        
        if match_id:
            dist_updates.append((match_id, d_id))
            # print(f"    Matched: {d_name} -> {match_id}")
        else:
            dist_fails.append(f"{d_name} (in {p_name})")

if dist_updates:
    cursor.executemany("UPDATE location_mogi SET cafeland_id = %s WHERE id = %s", dist_updates)
    conn.commit()
    print(f"  -> Updated {len(dist_updates)} Districts for TPHCM.")

print(f"  -> Unmatched Districts: {len(dist_fails)}")
if dist_fails:
    print(f"     First 5: {dist_fails[:5]}")

print("Done.")
cursor.close()
conn.close()
