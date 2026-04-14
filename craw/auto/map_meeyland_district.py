import pymysql
import re
import unicodedata
import os
import sys

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

from map_meeyland_city import normalize_name, build_meeyland_city_mapping

def build_meeyland_district_mapping(cursor):
    """
    Returns a dict mapping: { 'meey_id': { 'meey_name': str, 'parent_id': int, 'old_district_id': int, 'new_district_id': int } }
    """
    city_mapping = build_meeyland_city_mapping(cursor)
    
    cursor.execute("SELECT meey_id, name, city_meey_id FROM location_meeland WHERE level_type = 'district' OR (city_meey_id != meey_id AND district_meey_id = meey_id)")
    meey_dists = cursor.fetchall()
    
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city_new WHERE city_parent_id > 0")
    sys_dists = cursor.fetchall()
    
    sys_dist_lookup = {}
    for d_id, d_name, p_id in sys_dists:
        norm = normalize_name(d_name)
        if p_id not in sys_dist_lookup:
            sys_dist_lookup[p_id] = {}
        sys_dist_lookup[p_id][norm] = d_id
        
    cursor.execute("SELECT old_city_id, new_city_id FROM transaction_city_merge")
    merge_lookup = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Optional global sys dict lookup if not found in parent
    global_sys_dict = {}
    for d_id, d_name, p_id in sys_dists:
        norm = normalize_name(d_name)
        global_sys_dict[norm] = d_id

    mapping_dict = {}
    
    for meey_id, name, city_meey_id in meey_dists:
        city_info = city_mapping.get(city_meey_id)
        if not city_info:
            continue
            
        old_parent_id = city_info['old_city_id']
        norm = normalize_name(name)
        parent_dists = sys_dist_lookup.get(old_parent_id, {})
        old_id = parent_dists.get(norm)
        
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
                    
        # Final fallback - global district lookup (for system border changes where district is in another city)
        if not old_id:
            old_id = global_sys_dict.get(norm)
        
        if old_id:
            # Check merge
            new_id = merge_lookup.get(old_id, old_id)
            mapping_dict[meey_id] = {
                'meey_name': name,
                'parent_id': old_parent_id,
                'old_district_id': old_id,
                'new_district_id': new_id
            }
            
    return mapping_dict

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    print("=== MAPPING MEEYLAND DISTRICTS -> TRANSACTION_CITY_NEW -> TRANSACTION_CITY_MERGE ===")
    
    total_db = cursor.execute("SELECT meey_id FROM location_meeland WHERE level_type = 'district' OR (city_meey_id != meey_id AND district_meey_id = meey_id)")
    mapping = build_meeyland_district_mapping(cursor)
    
    print(f"Successfully mapped {len(mapping)}/{total_db} districts.")
    print("\nSample Mappings:")
    print(f"{'Meey_District_ID':<25} | {'Dist Name':<20} | {'Old_ID':<8} | {'New_ID':<8}")
    print("-" * 75)
    
    count = 0
    for meey_id, data in mapping.items():
        print(f"{meey_id:<25} | {data['meey_name']:<20} | {data['old_district_id']:<8} | {data['new_district_id']:<8}")
        count += 1
        if count >= 15:
            break
            
    conn.close()

if __name__ == "__main__":
    run()
