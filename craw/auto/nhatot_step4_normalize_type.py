import pymysql
import time

BATCH_SIZE = 5000

def main():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='craw_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    script_name = "nhatot_step4_normalize_type.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    print("--- Phase 1: Copying category/type ---")
    total_updated = 0
    while True:
        sql_update = f"""
            UPDATE data_clean_v1
            SET std_category = src_category_id,
                std_trans_type = src_type
            WHERE domain = 'nhatot'
              AND process_status = 3
              AND (std_category IS NULL OR std_trans_type IS NULL)
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_update)
        rows = cursor.rowcount
        conn.commit()
        total_updated += rows
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < BATCH_SIZE:
            break
    print(f"-> Copied Category/Type for {total_updated} rows.")

    # Finalize
    print("Finalizing step status...")
    sql_final = f"""
        UPDATE data_clean_v1 
        SET process_status = 4,
            last_script = '{script_name}'
        WHERE domain = 'nhatot'
          AND process_status = 3
    """
    cursor.execute(sql_final)
    conn.commit()
    print(f"-> Updated process_status = 4 for {cursor.rowcount} rows.")
    
    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
