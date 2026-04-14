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

    script_name = "mogi_step4_normalize_type.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    total_updated = 0
    while True:
        sql_update = f"""
            UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
            SET std_category = src_category_id,
                std_trans_type = CASE
                    WHEN LOWER(src_type) = 's' THEN 's'
                    WHEN LOWER(src_type) = 'u' THEN 'u'
                    WHEN LOWER(src_type) = 'mua' THEN 's'
                    WHEN LOWER(src_type) LIKE '%%ban%%' OR LOWER(src_type) LIKE '%%bán%%' THEN 's'
                    WHEN LOWER(src_type) = 'thuê' OR LOWER(src_type) = 'thue' THEN 'u'
                    WHEN LOWER(src_type) LIKE '%%thue%%' OR LOWER(src_type) LIKE '%%thuê%%' THEN 'u'
                    ELSE std_trans_type
                END
            WHERE domain = 'mogi'
              AND process_status = 3
              AND (
                std_category IS NULL OR std_category <> src_category_id
                OR std_trans_type IS NULL OR std_trans_type = ''
              )
            ORDER BY id
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_update)
        rows = cursor.rowcount
        conn.commit()
        total_updated += rows
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < BATCH_SIZE:
            break

    print(f"-> Normalized Category/Type for {total_updated} rows.")

    print("Finalizing step status...")
    cursor.execute(
        f"""
        UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
        SET process_status = 4, last_script = '{script_name}'
        WHERE domain = 'mogi'
          AND process_status = 3
          AND std_category IS NOT NULL
          AND std_trans_type IS NOT NULL
        """
    )
    conn.commit()
    print(f"-> Updated process_status = 4 for {cursor.rowcount} rows.")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
