import pymysql
import sys

# Constants
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def merge_new_data():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== MERGE CAFELAND ID INTO DATA_CLEAN ===")

    # 1. Ensure column exists (idempotent check)
    try:
        cursor.execute("DESCRIBE data_clean cafeland_id")
        if cursor.rowcount == 0:
            print("Column 'cafeland_id' missing. Adding it...")
            cursor.execute("ALTER TABLE data_clean ADD COLUMN cafeland_id BIGINT DEFAULT NULL")
            conn.commit()
    except Exception as e:
        # Fallback if describe fails oddly, try adding
        try:
             cursor.execute("ALTER TABLE data_clean ADD COLUMN cafeland_id BIGINT DEFAULT NULL")
             conn.commit()
             print("Added column cafeland_id.")
        except:
             pass # Likely exists

    # 2. Count rows before update (Check for NULLs)
    cursor.execute("SELECT COUNT(*) FROM data_clean WHERE cafeland_id IS NULL")
    count_null = cursor.fetchone()[0]
    print(f"Found {count_null} rows with NULL cafeland_id.")
    
    if count_null == 0:
        print("No new data to merge. Exiting.")
        cursor.close()
        conn.close()
        return

    # 3. Exec Update
    # Update only rows that are currently NULL to be efficient for incremental runs
    sql = """
    UPDATE data_clean d
    JOIN location_detail l ON 
        d.ward = l.ward_id AND 
        d.area_v2 = l.area_id AND 
        d.region_v2 = l.region_id
    SET d.cafeland_id = l.cafeland_id
    WHERE l.level = 3 
      AND l.cafeland_id IS NOT NULL 
      AND d.cafeland_id IS NULL
    """

    print("Executing increment update...")
    cursor.execute(sql)
    rows_affected = cursor.rowcount
    conn.commit()

    print(f"SUCCESS: Updated {rows_affected} new rows.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    merge_new_data()
