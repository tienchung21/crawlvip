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

    script_name = "mogi_step6_normalize_date.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN transfer_time BIGINT NULL")
        conn.commit()
        print("Added column transfer_time to data_clean_v1")
    except Exception:
        pass

    # Bulk SQL is much faster than per-row Python timestamp parsing.
    # orig_list_time from mogi convert is typically UNIX seconds.
    # Also support milliseconds if any older rows exist.
    total_updated = 0
    while True:
        now_ts = int(time.time())
        sql_update = f"""
            UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
            SET
              std_date = DATE(
                FROM_UNIXTIME(
                  CASE
                    WHEN orig_list_time IS NOT NULL AND orig_list_time > 1000000000000 THEN orig_list_time/1000
                    WHEN orig_list_time IS NOT NULL AND orig_list_time > 0 THEN orig_list_time
                    WHEN update_time IS NOT NULL AND update_time > 1000000000000 THEN update_time/1000
                    WHEN update_time IS NOT NULL AND update_time > 0 THEN update_time
                    ELSE NULL
                  END
                )
              ),
              transfer_time = COALESCE(transfer_time, %s),
              last_script = %s
            WHERE domain='mogi'
              AND process_status=5
              AND std_date IS NULL
              AND (
                (orig_list_time IS NOT NULL AND orig_list_time > 0)
                OR (update_time IS NOT NULL AND update_time > 0)
              )
            ORDER BY id
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_update, (now_ts, script_name))
        rows = cursor.rowcount
        conn.commit()
        if rows == 0:
            break
        total_updated += rows
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < BATCH_SIZE:
            break

    # Finalize status=5 rows only if std_date was computed (avoid marking incomplete as done).
    cursor.execute(
        f"""
        UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
        SET process_status=6,
            last_script=%s,
            transfer_time=COALESCE(transfer_time, %s),
            median_flag=COALESCE(median_flag, 0)
        WHERE domain='mogi'
          AND process_status=5
          AND std_date IS NOT NULL
        """,
        (script_name, int(time.time())),
    )
    conn.commit()
    finalized = cursor.rowcount

    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM data_clean_v1
        WHERE domain='mogi' AND process_status=5 AND std_date IS NULL
        """
    )
    stuck = cursor.fetchone() or {}

    print(f"-> Normalized Date rows updated: {total_updated} (std_date set or refreshed).")
    print(f"-> Finalized process_status 5->6 where std_date is NOT NULL: {finalized}")
    print(f"Skipped (status stays 5 due to missing/unparseable date): {stuck.get('cnt', 0)}")
    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
