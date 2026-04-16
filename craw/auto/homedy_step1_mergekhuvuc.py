import pymysql
import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5000, help="Number of records to process")
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print(f"=== homedy_step1_mergekhuvuc.py ===")
    
    sql = "SELECT id, src_province_id, src_district_id, src_ward_id FROM data_clean_v1 WHERE domain = 'homedy.com' AND process_status = 0 LIMIT %s"
    start = time.time()
    cursor.execute(sql, (args.limit,))
    rows = cursor.fetchall()
    
    if not rows:
        print("No rows to process.")
        return

    update_query = """
        UPDATE data_clean_v1 
        SET cf_province_id = %s, cf_district_id = %s, cf_ward_id = %s,
            process_status = %s, last_script = 'homedy_step1_mergekhuvuc.py'
        WHERE id = %s
    """
    
    update_data = []
    for r in rows:
        # Check if province and district are not null
        status = 1 if r['src_province_id'] and r['src_district_id'] else -1
        update_data.append((
            r['src_province_id'],
            r['src_district_id'],
            r['src_ward_id'],
            status,
            r['id']
        ))
            
    if update_data:
        cursor.executemany(update_query, update_data)
    conn.commit()
    
    print(f"-> Merged location and set status=1 for {len(update_data)} homedy records in {time.time()-start:.2f}s.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
