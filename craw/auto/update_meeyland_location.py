import pymysql
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

from map_meeyland_city import build_meeyland_city_mapping
from map_meeyland_district import build_meeyland_district_mapping
from map_meeyland_ward import build_meeyland_ward_mapping

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== UPDATING LOCATION_MEELAND WITH NEW CAFELAND_ID ===")
    
    # 1. Update Cities
    print("1. Updating Cities...")
    city_map = build_meeyland_city_mapping(cursor)
    city_updates = [(data['new_city_id'], meey_id) for meey_id, data in city_map.items()]
    cursor.executemany("UPDATE location_meeland SET cafeland_id = %s WHERE meey_id = %s", city_updates)
    conn.commit()
    print(f"Updated {len(city_updates)} cities.")
    
    # 2. Update Districts
    print("2. Updating Districts...")
    dict_map = build_meeyland_district_mapping(cursor)
    dist_updates = [(data['new_district_id'], meey_id) for meey_id, data in dict_map.items()]
    cursor.executemany("UPDATE location_meeland SET cafeland_id = %s WHERE meey_id = %s", dist_updates)
    conn.commit()
    print(f"Updated {len(dist_updates)} districts.")
    
    # 3. Update Wards
    print("3. Updating Wards...")
    ward_map = build_meeyland_ward_mapping(cursor)
    ward_updates = [(data['new_ward_id'], meey_id) for meey_id, data in ward_map.items()]
    cursor.executemany("UPDATE location_meeland SET cafeland_id = %s WHERE meey_id = %s", ward_updates)
    conn.commit()
    print(f"Updated {len(ward_updates)} wards.")
    
    print("Finished updating 'cafeland_id' in location_meeland.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run()
