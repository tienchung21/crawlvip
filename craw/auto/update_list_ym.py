import pymysql

# Config
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== UPDATING LIST_YM IN DATA_CLEAN ===")
    
    # 0. Create Indexes for Performance
    print("Checking indexes for performance...")
    try:
        cursor.execute("CREATE INDEX idx_orig_list_time ON data_clean(orig_list_time)")
        print("Created index idx_orig_list_time")
    except:
        pass
        
    try:
        cursor.execute("CREATE INDEX idx_list_time ON data_clean(list_time)")
        print("Created index idx_list_time")
    except:
        pass
    conn.commit()

    # 1. Update from orig_list_time (Priority 1)
    # Using FROM_UNIXTIME to convert bigint timestamp to YYYY-MM
    # NOTE: Timestamps are in MILLISECONDS, so divide by 1000
    print("Updating list_ym from orig_list_time (where available)...")
    sql_orig = """
    UPDATE data_clean
    SET list_ym = DATE_FORMAT(FROM_UNIXTIME(orig_list_time/1000), '%Y-%m')
    WHERE orig_list_time IS NOT NULL 
      AND orig_list_time > 0
      AND (list_ym IS NULL OR list_ym != DATE_FORMAT(FROM_UNIXTIME(orig_list_time/1000), '%Y-%m'))
    """
    cursor.execute(sql_orig)
    rows_orig = cursor.rowcount
    print(f"Updated {rows_orig} rows from orig_list_time.")

    # 2. Update from list_time (Fallback)
    # Only if orig_list_time is null/invalid
    print("Updating list_ym from list_time (fallback)...")
    sql_fallback = """
    UPDATE data_clean
    SET list_ym = DATE_FORMAT(FROM_UNIXTIME(list_time/1000), '%Y-%m')
    WHERE (orig_list_time IS NULL OR orig_list_time = 0)
      AND list_time IS NOT NULL 
      AND list_time > 0
      AND (list_ym IS NULL OR list_ym != DATE_FORMAT(FROM_UNIXTIME(list_time/1000), '%Y-%m'))
    """
    cursor.execute(sql_fallback)
    rows_fallback = cursor.rowcount
    print(f"Updated {rows_fallback} rows from list_time.")
    
    conn.commit()
    print("Done.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run()