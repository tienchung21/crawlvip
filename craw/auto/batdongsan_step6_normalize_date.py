import pymysql
import time

BATCH_SIZE = 5000
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "craw_db"
DOMAIN = "batdongsan.com.vn"


def main():
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    cursor = conn.cursor()

    script_name = "batdongsan_step6_normalize_date.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    # Keep parity with other domains.
    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN transfer_time BIGINT NULL")
        conn.commit()
        print("Added column transfer_time to data_clean_v1")
    except Exception:
        pass

    total_updated = 0

    # 1) Compute std_date in batches for Step 5 rows.
    # Priority:
    #   - orig_list_time in YYYYMMDD (from source date mapping)
    #   - fallback update_time as unix timestamp (seconds / milliseconds)
    while True:
        now_ts = int(time.time())
        sql_update = f"""
            UPDATE data_clean_v1
            SET
              std_date = COALESCE(
                STR_TO_DATE(CAST(orig_list_time AS CHAR), '%%Y%%m%%d'),
                DATE(
                  FROM_UNIXTIME(
                    CASE
                      WHEN update_time IS NOT NULL AND update_time > 1000000000000 THEN update_time/1000
                      WHEN update_time IS NOT NULL AND update_time > 0 THEN update_time
                      ELSE NULL
                    END
                  )
                )
              ),
              transfer_time = COALESCE(transfer_time, %s),
              last_script = %s
            WHERE domain=%s
              AND process_status=5
              AND std_date IS NULL
              AND (
                (orig_list_time IS NOT NULL AND orig_list_time > 0)
                OR (update_time IS NOT NULL AND update_time > 0)
              )
            ORDER BY id
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_update, (now_ts, script_name, DOMAIN))
        rows = cursor.rowcount
        conn.commit()
        if rows == 0:
            break
        total_updated += rows
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < BATCH_SIZE:
            break

    # 2) Finalize only rows having valid std_date.
    cursor.execute(
        """
        UPDATE data_clean_v1
        SET process_status=6,
            last_script=%s,
            transfer_time=COALESCE(transfer_time, %s),
            median_flag=COALESCE(median_flag, 0)
        WHERE domain=%s
          AND process_status=5
          AND std_date IS NOT NULL
        """,
        (script_name, int(time.time()), DOMAIN),
    )
    conn.commit()
    finalized = cursor.rowcount

    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM data_clean_v1
        WHERE domain=%s AND process_status=5 AND std_date IS NULL
        """,
        (DOMAIN,),
    )
    stuck = cursor.fetchone() or {}

    print(f"-> Normalized Date rows updated: {total_updated}")
    print(f"-> Finalized process_status 5->6 where std_date is NOT NULL: {finalized}")
    print(f"Skipped (status stays 5 due to missing/unparseable date): {stuck.get('cnt', 0)}")
    print(f"=== Finished in {time.time() - start_time:.2f}s ===")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
