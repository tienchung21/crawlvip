import pymysql
import os
import sys
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
try:
    import Config
    DB_HOST = Config.MYSQL_HOST
    DB_USER = Config.MYSQL_USER
    DB_PASS = Config.MYSQL_PASSWORD
    DB_NAME = Config.MYSQL_DB
except ImportError:
    DB_HOST = 'localhost'
    DB_USER = 'root'
    DB_PASS = ''
    DB_NAME = 'craw_db'

from map_meeyland_city import normalize_name

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== MAPPING HOMEDY -> TRANSACTION_CITY_NEW -> TRANSACTION_CITY_MERGE ===")

    # Cache sys cities
    cursor.execute("SELECT city_id, city_title FROM transaction_city_new WHERE city_parent_id = 0 AND city_loai IN (1, 2) OR city_parent_id = 0")
    sys_cities = cursor.fetchall()
    sys_city_lookup = {normalize_name(c[1]): c[0] for c in sys_cities}
    
    # Cache sys districts
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city_new WHERE city_parent_id > 0")
    sys_dists = cursor.fetchall()
    sys_dist_lookup = {}
    global_sys_dict = {}
    for d_id, d_name, p_id in sys_dists:
        norm = normalize_name(d_name)
        if p_id not in sys_dist_lookup:
            sys_dist_lookup[p_id] = {}
        sys_dist_lookup[p_id][norm] = d_id
        global_sys_dict[norm] = d_id
        
    # Cache merge lookup
    cursor.execute("SELECT old_city_id, new_city_id FROM transaction_city_merge")
    merge_lookup = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Cache sys wards
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city_new WHERE city_parent_id > 0")
    sys_wards_db = cursor.fetchall()
    sys_ward_lookup = {}
    for w_id, w_name, p_id in sys_wards_db:
        norm = normalize_name(w_name)
        norm_short = re.sub(r'^(phuong|xa|thi tran|tt|p|x)\s+', '', norm).strip()
        if p_id not in sys_ward_lookup:
            sys_ward_lookup[p_id] = {}
        sys_ward_lookup[p_id][norm] = w_id
        if norm_short != norm:
            sys_ward_lookup[p_id][norm_short] = w_id
            
    # --- PROCESS CITIES ---
    cursor.execute("SELECT location_id, name FROM location_homedy WHERE level_type = 'city'")
    homedy_cities = cursor.fetchall()
    city_mapping = {}
    
    for loc_id, name in homedy_cities:
        norm = normalize_name(name)
        old_id = sys_city_lookup.get(norm)
        if not old_id:
            for sys_norm, sys_id in sys_city_lookup.items():
                if len(norm) > 3 and (norm in sys_norm or sys_norm in norm):
                    old_id = sys_id
                    break
        if old_id:
            new_id = merge_lookup.get(old_id, old_id)
            city_mapping[loc_id] = { 'old_id': old_id, 'new_id': new_id }

    print(f"Mapped {len(city_mapping)}/{len(homedy_cities)} cities.")
    
    # Update Cities
    updates = [(data['new_id'], loc_id) for loc_id, data in city_mapping.items()]
    cursor.executemany("UPDATE location_homedy SET cafeland_id = %s WHERE location_id = %s AND level_type = 'city'", updates)
    conn.commit()
    
    # --- PROCESS DISTRICTS ---
    cursor.execute("SELECT location_id, name, city_id FROM location_homedy WHERE level_type = 'district'")
    homedy_dists = cursor.fetchall()
    dist_mapping = {}
    
    aliases = {
        'bù đăng': 'bù ðăng',
        'bu dang': 'bù ðăng',
        'phú quý': 'phú quí',
        'ea kar': 'ea kra',
        "m'đrắk": "m'đrắt",
        'ia grai': 'la grai',
        'ia pa': 'la pa',
        'kông chro': 'krông chro',
        'mèo vạc': 'mèo vạt',
        'bạch long vĩ': 'bạch long vỹ',
        'giồng riềng': 'gồng giềng',
        "ia h'drai": 'la hdrai',
        'si ma cai': 'xi ma cai',
        'nậm nhùn': 'mường tè', 
        'tam đường': 'phong thổ'
    }
    
    for loc_id, name, city_id in homedy_dists:
        city_info = city_mapping.get(city_id)
        if not city_info: continue
        
        old_parent_id = city_info['old_id']
        norm = normalize_name(name)
        parent_dists = sys_dist_lookup.get(old_parent_id, {})
        old_id = parent_dists.get(norm)
        
        if not old_id and name.lower() in aliases:
            norm_alias = normalize_name(aliases[name.lower()])
            old_id = parent_dists.get(norm_alias)
        if not old_id and norm in aliases:
            norm_alias = normalize_name(aliases[norm])
            old_id = parent_dists.get(norm_alias)
            
        if not old_id:
            for sys_norm, sys_id in parent_dists.items():
                if len(norm) > 2 and (norm in sys_norm or sys_norm in norm):
                    old_id = sys_id
                    break
                    
        if not old_id:
            old_id = global_sys_dict.get(norm)
            
        if old_id:
            new_id = merge_lookup.get(old_id, old_id)
            dist_mapping[loc_id] = { 'old_id': old_id, 'new_id': new_id }

    print(f"Mapped {len(dist_mapping)}/{len(homedy_dists)} districts.")
    updates = [(data['new_id'], loc_id) for loc_id, data in dist_mapping.items()]
    cursor.executemany("UPDATE location_homedy SET cafeland_id = %s WHERE location_id = %s AND level_type = 'district'", updates)
    conn.commit()
    
    # --- PROCESS WARDS ---
    cursor.execute("SELECT location_id, name, district_id FROM location_homedy WHERE level_type = 'ward'")
    homedy_wards = cursor.fetchall()
    ward_mapping = {}
    
    for loc_id, name, district_id in homedy_wards:
        dist_info = dist_mapping.get(district_id)
        if not dist_info: continue
        
        old_parent_id = dist_info['old_id']
        norm = normalize_name(name)
        norm_short = re.sub(r'^(phuong|xa|thi tran|tt|p|x)\s+', '', norm).strip()
        
        parent_wards = sys_ward_lookup.get(old_parent_id, {})
        old_id = parent_wards.get(norm)
        
        if not old_id: old_id = parent_wards.get(norm_short)
        
        if not old_id:
            for sys_norm, sys_id in parent_wards.items():
                if len(norm_short) > 2 and (norm_short in sys_norm or sys_norm in norm_short):
                    old_id = sys_id
                    break
                    
        if old_id:
            new_id = merge_lookup.get(old_id, old_id)
            ward_mapping[loc_id] = { 'old_id': old_id, 'new_id': new_id }

    print(f"Mapped {len(ward_mapping)}/{len(homedy_wards)} wards.")
    updates = [(data['new_id'], loc_id) for loc_id, data in ward_mapping.items()]
    cursor.executemany("UPDATE location_homedy SET cafeland_id = %s WHERE location_id = %s AND level_type = 'ward'", updates)
    conn.commit()
    
    print("Done!")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run()
