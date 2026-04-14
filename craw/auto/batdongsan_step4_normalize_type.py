
import pymysql
import time

BATCH_SIZE = 5000
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def main():
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    script_name = "batdongsan_step4_normalize_type.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    total_updated = 0
    
    while True:
        # Get Batch (Status = 3 from Step 3)
        # We process Batdongsan domain
        # Join with scraped_details_flat to get the URL
        sql_get = f"""
            SELECT d.id, d.src_category_id, s.url
            FROM data_clean_v1 d
            JOIN scraped_details_flat s ON d.ad_id = s.matin
            WHERE d.domain = 'batdongsan.com.vn'
              AND s.domain = 'batdongsan.com.vn'
              AND d.process_status = 3 
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_get)
        rows = cursor.fetchall()
        
        if not rows:
            break
            
        batch_count = 0
        for row in rows:
            record_id = row.get('id')
            raw_cat = row.get('src_category_id')
            url = row.get('url', '').lower() if row.get('url') else ''
            
            # 1. Std Category: Direct Copy (User Request)
            std_category = raw_cat
            
            # 2. Std Trans Type: Infer from URL
            std_trans_type = None
            if '/ban-' in url:
                std_trans_type = 's'
            elif '/cho-thue-' in url:
                std_trans_type = 'u'
            
            # Update AND Set Process Status = 4
            sql_update = """
                UPDATE data_clean_v1 
                SET std_category = %s, std_trans_type = %s, process_status = 4
                WHERE id = %s
            """
            cursor.execute(sql_update, (std_category, std_trans_type, record_id))
            batch_count += 1
                
        conn.commit()
        total_updated += batch_count
        print(f"  Batch: +{batch_count} rows (Total: {total_updated})")
        
        if len(rows) < BATCH_SIZE:
            break

    print(f"-> Normalized Type/Category for {total_updated} rows.")
    print("=== Finished ===")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
