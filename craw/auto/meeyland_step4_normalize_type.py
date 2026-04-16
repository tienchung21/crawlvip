import pymysql
import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print("=== meeyland_step4_normalize_type.py ===")
    
    sql = "SELECT id, src_category_id, src_type FROM data_clean_v1 WHERE domain = 'meeyland.com' AND process_status = 3 ORDER BY id LIMIT %s"
    start = time.time()
    cursor.execute(sql, (args.limit,))
    rows = cursor.fetchall()
    
    if not rows:
        print("No rows to process.")
        return

    update_query = "UPDATE data_clean_v1 SET std_trans_type = %s, std_category = %s, process_status = %s, last_script = 'meeyland_step4_normalize_type.py' WHERE id = %s"
    update_data = []
    
    for r in rows:
        trans_type = r['src_type']
        category = r['src_category_id']
        
        # Assume missing trans_type or category is an error
        if trans_type and category:
            update_data.append((trans_type, category, 4, r['id']))
        else:
            update_data.append((trans_type, category, -4, r['id']))
            
    if update_data:
        cursor.executemany(update_query, update_data)
    conn.commit()
    
    print(f"-> Normalized type and updated status (4 or -4) for {len(update_data)} meeyland records in {time.time()-start:.2f}s.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
