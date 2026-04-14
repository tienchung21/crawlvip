import pymysql
import re
import unicodedata
import os
import sys

# Add parent directory to path to import Config
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
            
    if name in ['tphcm', 'tp hcm', 'ho chi minh']:
        name = 'ho chi minh'
    elif name == 'brvt' or 'ba ria' in name:
        name = 'ba ria vung tau'
    
    return name.strip()

def build_meeyland_city_mapping(cursor):
    """
    Returns a dict mapping: { 'meey_id': new_city_id }
    """
    # 1. Get Meeyland cities
    cursor.execute("SELECT meey_id, name FROM location_meeland WHERE level_type = 'city' OR level_type = '' OR level_type IS NULL OR city_meey_id = meey_id")
    if cursor.rowcount == 0:
        cursor.execute("SELECT meey_id, name FROM location_meeland WHERE district_meey_id IS NULL OR district_meey_id = ''")
    meey_cities = cursor.fetchall()
    
    # 2. Get transaction_city_new
    cursor.execute("SELECT city_id, city_title FROM transaction_city_new WHERE city_parent_id = 0 AND city_loai IN (1, 2) OR city_parent_id = 0")
    sys_cities = cursor.fetchall()
    sys_city_lookup = {normalize_name(c[1]): c[0] for c in sys_cities}
    
    # 3. Get transaction_city_merge mapped ids
    cursor.execute("SELECT old_city_id, new_city_id FROM transaction_city_merge")
    merge_lookup = {row[0]: row[1] for row in cursor.fetchall()}
    
    mapping_dict = {}
    
    for meey_id, name in meey_cities:
        norm = normalize_name(name)
        old_id = sys_city_lookup.get(norm)
        
        # Fuzzy match
        if not old_id:
            for sys_norm, sys_id in sys_city_lookup.items():
                if len(norm) > 3 and (norm in sys_norm or sys_norm in norm):
                    old_id = sys_id
                    break
                    
        if old_id:
            new_id = merge_lookup.get(old_id)
            if new_id:
                mapping_dict[meey_id] = {
                    'meey_name': name,
                    'old_city_id': old_id,
                    'new_city_id': new_id
                }
    
    return mapping_dict

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== MAPPING MEEYLAND -> TRANSACTION_CITY_NEW -> TRANSACTION_CITY_MERGE ===")
    
    mapping = build_meeyland_city_mapping(cursor)
    
    print(f"\nSuccessfully mapped {len(mapping)} cities.")
    print(f"{'Meeyland_ID':<25} | {'City Name':<25} | {'Old_ID':<8} | {'New_ID':<8}")
    print("-" * 75)
    for meey_id, data in mapping.items():
        print(f"{meey_id:<25} | {data['meey_name']:<25} | {data['old_city_id']:<8} | {data['new_city_id']:<8}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run()
