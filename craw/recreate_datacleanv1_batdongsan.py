
import pymysql
import sys
import time

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    print("Connecting to DB...")
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    # 0. Cleanup Garbage from previous run (if any)
    # Since we accidentally inserted non-BDS rows which likely got CAST('18/...' AS UNSIGNED) -> 18.
    # We remove rows where domain='batdongsan.com.vn' AND orig_list_time < 20200101 (Just to be safe).
    # Real data is 2026...
    print("--- STEP 0: CLEANUP GARBAGE ---")
    try:
        sql_cleanup = "DELETE FROM data_clean_v1 WHERE domain='batdongsan.com.vn' AND orig_list_time > 0 AND orig_list_time < 20200101"
        cursor.execute(sql_cleanup)
        print(f"Cleaned up {cursor.rowcount} garbage rows.")
        conn.commit()
    except Exception as e:
        print(f"Cleanup Error: {e}")
    
    # 1. Insert Ignore (Strictly Batdongsan Domain)
    print("--- STEP 1: INSERT IGNORE ---")
    sql_insert = """
        INSERT IGNORE INTO data_clean_v1 (
            ad_id, 
            src_province_id, src_district_id, src_ward_id,
            src_size, src_price, src_category_id,
            orig_list_time, 
            std_date,
            domain, process_status
        )
        SELECT 
            matin, 
            city_code, district_id, ward_id,
            LEFT(REPLACE(REPLACE(dientich, '\n', ''), '\r', ''), 50), 
            LEFT(REPLACE(REPLACE(khoanggia, '\n', ''), '\r', ''), 50), 
            loaihinh,
            CAST(ngaydang AS UNSIGNED), -- Simple Copy (Confirmed Numeric for BDS)
            NULL,
            'batdongsan.com.vn', 
            0
        FROM scraped_details_flat
        WHERE matin IS NOT NULL AND matin != ''
          AND domain = 'batdongsan.com.vn' -- CRITICAL FILTER
    """
    
    try:
        cursor.execute(sql_insert)
        print(f"Inserted: {cursor.rowcount} new rows.")
        conn.commit()
    except Exception as e:
        print(f"Insert Error: {e}")
        conn.rollback()

    # 2. Update Existing (Simple Copy)
    print("\n--- STEP 2: UPDATE EXISTING (Simple Copy) ---")
    total_updated = 0
    batch_size = 5000
    
    while True:
        sql_batch = """
            UPDATE data_clean_v1 d
            JOIN scraped_details_flat s ON d.ad_id = s.matin
            SET d.orig_list_time = CAST(s.ngaydang AS UNSIGNED)
            WHERE d.domain = 'batdongsan.com.vn' 
              AND s.domain = 'batdongsan.com.vn' -- Ensure Source is also BDS
              AND (d.orig_list_time IS NULL OR d.orig_list_time = 0)
            LIMIT %s
        """
        try:
            cursor.execute(sql_batch, (batch_size,))
            count = cursor.rowcount
            conn.commit()
            total_updated += count
            print(f"Batch updated: {count} rows. Total: {total_updated}")
            if count < batch_size:
                break
            time.sleep(1)
        except Exception as e:
            print(f"Batch Update Error: {e}")
            conn.rollback()
            break

    conn.close()

if __name__ == '__main__':
    run()
