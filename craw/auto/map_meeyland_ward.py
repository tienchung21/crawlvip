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

from map_meeyland_city import normalize_name
from map_meeyland_district import build_meeyland_district_mapping

def build_meeyland_ward_mapping(cursor):
    """
    Returns a dict mapping: { 'meey_id': { 'meey_name': str, 'parent_district_id': int, 'old_ward_id': int, 'new_ward_id': int } }
    """
    district_mapping = build_meeyland_district_mapping(cursor)
    
    cursor.execute("SELECT meey_id, name, district_meey_id FROM location_meeland WHERE level_type = 'ward'")
    meey_wards = cursor.fetchall()
    
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city_new WHERE city_parent_id > 0")
    sys_wards = cursor.fetchall()
    
    sys_ward_lookup = {}
    for w_id, w_name, p_id in sys_wards:
        norm = normalize_name(w_name)
        # Also strip prefixes aggressively for wards (phường, xã, thị trấn)
        norm_short = re.sub(r'^(phuong|xa|thi tran|tt|p|x)\s+', '', norm).strip()
        
        if p_id not in sys_ward_lookup:
            sys_ward_lookup[p_id] = {}
            
        sys_ward_lookup[p_id][norm] = w_id
        if norm_short != norm:
            sys_ward_lookup[p_id][norm_short] = w_id
            
    cursor.execute("SELECT old_city_id, new_city_id FROM transaction_city_merge")
    merge_lookup = {row[0]: row[1] for row in cursor.fetchall()}
    
    mapping_dict = {}
    
    for meey_id, name, district_meey_id in meey_wards:
        dist_info = district_mapping.get(district_meey_id)
        if not dist_info:
            continue
            
        old_parent_id = dist_info['old_district_id']
        norm = normalize_name(name)
        norm_short = re.sub(r'^(phuong|xa|thi tran|tt|p|x)\s+', '', norm).strip()
        
        parent_wards = sys_ward_lookup.get(old_parent_id, {})
        
        # 1. Exact match
        old_id = parent_wards.get(norm)
        if not old_id:
            old_id = parent_wards.get(norm_short)
            
        # 2. Fuzzy match
        if not old_id:
            for sys_norm, sys_id in parent_wards.items():
                if len(norm_short) > 2 and (norm_short in sys_norm or sys_norm in norm_short):
                    old_id = sys_id
                    break
        
        if old_id:
            new_id = merge_lookup.get(old_id, old_id)
            mapping_dict[meey_id] = {
                'meey_name': name,
                'parent_id': old_parent_id,
                'old_ward_id': old_id,
                'new_ward_id': new_id
            }
            
    return mapping_dict

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    print("=== MAPPING MEEYLAND WARDS -> TRANSACTION_CITY_NEW -> TRANSACTION_CITY_MERGE ===")
    
    cursor.execute("SELECT COUNT(*) FROM location_meeland WHERE level_type = 'ward'")
    total_db = cursor.fetchone()[0]
    
    mapping = build_meeyland_ward_mapping(cursor)
    
    print(f"Successfully mapped {len(mapping)}/{total_db} wards.")
    print("\nSample Mappings:")
    print(f"{'Meey_Ward_ID':<25} | {'Ward Name':<20} | {'Old_ID':<8} | {'New_ID':<8}")
    print("-" * 75)
    
    count = 0
    for meey_id, data in mapping.items():
        print(f"{meey_id:<25} | {data['meey_name']:<20} | {data['old_ward_id']:<8} | {data['new_ward_id']:<8}")
        count += 1
        if count >= 15:
            break
            
    conn.close()

if __name__ == "__main__":
    run()
