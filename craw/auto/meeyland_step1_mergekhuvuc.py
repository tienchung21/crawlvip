import pymysql
import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5000, help="Number of records to process")
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print(f"=== meeyland_step1_mergekhuvuc.py ===")
    
    # Load locations into memory to avoid table scans on updates
    cursor.execute("SELECT level_type, code, cafeland_id, district_meey_id, meey_id FROM location_meeland WHERE cafeland_id IS NOT NULL")
    locations = cursor.fetchall()
    
    cities = {}
    districts = {}
    wards = {}
    
    for loc in locations:
        if loc['level_type'] == 'city' and loc['code']:
            cities[str(loc['code'])] = loc['cafeland_id']
        elif loc['level_type'] == 'district' and loc['code']:
            districts[str(loc['code'])] = { 'cafeland_id': loc['cafeland_id'], 'meey_id': loc['meey_id'] }
        elif loc['level_type'] == 'ward' and loc['code']:
            wards[f"{str(loc['code'])}_{str(loc['district_meey_id'])}"] = loc['cafeland_id']

    # Fetch batch to update
    sql = "SELECT id, src_province_id, src_district_id, src_ward_id FROM data_clean_v1 WHERE domain = 'meeyland.com' AND process_status = 0 LIMIT %s"
    start = time.time()
    cursor.execute(sql, (args.limit,))
    rows = cursor.fetchall()
    
    if not rows:
        print("No rows to process.")
        return

    updates = []
    
    for r in rows:
        c_code = str(r['src_province_id']) if r['src_province_id'] else ""
        d_code = str(r['src_district_id']) if r['src_district_id'] else ""
        w_code = str(r['src_ward_id']) if r['src_ward_id'] else ""
        
        cf_prov = cities.get(c_code)
        cf_dist = None
        d_meey = None
        if d_code in districts:
            cf_dist = districts[d_code]['cafeland_id']
            d_meey = districts[d_code]['meey_id']
            
        cf_ward = None
        if w_code and d_meey:
            cf_ward = wards.get(f"{w_code}_{d_meey}")
            
        updates.append((cf_prov, cf_dist, cf_ward, r['id']))

    update_query = "UPDATE data_clean_v1 SET cf_province_id = %s, cf_district_id = %s, cf_ward_id = %s WHERE id = %s"
    cursor.executemany(update_query, updates)
    conn.commit()

    cursor.execute("""
        UPDATE data_clean_v1 
        SET process_status = 1, last_script = 'meeyland_step1_mergekhuvuc.py'
        WHERE domain = 'meeyland.com' 
          AND process_status = 0
          AND cf_province_id IS NOT NULL 
          AND cf_district_id IS NOT NULL
    """)
    updated = cursor.rowcount
    conn.commit()
    
    print(f"-> Merged location and set status=1 for {updated} meeyland records in {time.time()-start:.2f}s.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
