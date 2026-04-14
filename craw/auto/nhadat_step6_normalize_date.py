import argparse
import time

import pymysql


BATCH_SIZE_DEFAULT = 5000


def _ensure_transfer_time(cursor, conn):
    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN transfer_time BIGINT NULL")
        conn.commit()
        print("Added column transfer_time to data_clean_v1")
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Step 6 (Date) for data_clean_v1 domain nhadat")
    parser.add_argument("--domain", default="nhadat", help="Domain in data_clean_v1 (default: nhadat)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help="Batch size")
    parser.add_argument("--recompute", action="store_true", help="Recompute std_date even if already set")
    parser.add_argument("--dry-run", action="store_true", help="Print stats only, do not update DB")
    args = parser.parse_args()

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="craw_db",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    cursor = conn.cursor()

    script_name = "nhadat_step6_normalize_date.py"
    print(f"=== Running {script_name} (domain={args.domain}) ===")
    start_time = time.time()

    _ensure_transfer_time(cursor, conn)

    cursor.execute("SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s", (args.domain,))
    total_domain = int((cursor.fetchone() or {}).get("c") or 0)
    cursor.execute(
        "SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s AND process_status=5",
        (args.domain,),
    )
    total_target = int((cursor.fetchone() or {}).get("c") or 0)
    skipped_wrong_status = max(total_domain - total_target, 0)
    print(f"Skipped (wrong status): {skipped_wrong_status:,} / total_domain={total_domain:,}")

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_step5,
            SUM(CASE WHEN orig_list_time IS NULL OR orig_list_time=0 THEN 1 ELSE 0 END) AS missing_orig_list_time,
            SUM(CASE WHEN std_date IS NULL THEN 1 ELSE 0 END) AS missing_std_date
        FROM data_clean_v1
        WHERE domain=%s AND process_status=5
        """,
        (args.domain,),
    )
    stat = cursor.fetchone() or {}
    print(
        "Pending (process_status=5): {total_step5:,} | missing orig_list_time: {missing_orig_list_time:,} | missing std_date: {missing_std_date:,}".format(
            total_step5=int(stat.get("total_step5") or 0),
            missing_orig_list_time=int(stat.get("missing_orig_list_time") or 0),
            missing_std_date=int(stat.get("missing_std_date") or 0),
        )
    )

    if args.dry_run:
        print("Dry-run: no updates executed.")
        cursor.close()
        conn.close()
        return

    extra = "" if args.recompute else "AND std_date IS NULL"
    total_updated = 0
    now_ts = int(time.time())
    while True:
        cursor.execute(
            f"""
            UPDATE data_clean_v1
            SET
                std_date = STR_TO_DATE(CAST(orig_list_time AS CHAR), '%%Y%%m%%d'),
                transfer_time = COALESCE(transfer_time, %s),
                process_status = 6,
                last_script = %s,
                median_flag = COALESCE(median_flag, 0)
            WHERE domain=%s
              AND process_status=5
              AND orig_list_time IS NOT NULL AND orig_list_time <> 0
              AND STR_TO_DATE(CAST(orig_list_time AS CHAR), '%%Y%%m%%d') IS NOT NULL
              {extra}
            ORDER BY id
            LIMIT {int(args.batch_size)}
            """,
            (now_ts, script_name, args.domain),
        )
        rows = cursor.rowcount
        conn.commit()
        if rows == 0:
            break
        total_updated += rows
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < args.batch_size:
            break

    end_time = time.time()
    print(f"-> Normalized Date for {total_updated} rows (std_date set).")
    cursor.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN orig_list_time IS NULL OR orig_list_time=0 THEN 1 ELSE 0 END) AS missing_orig_list_time,
          SUM(CASE WHEN std_date IS NULL THEN 1 ELSE 0 END) AS missing_std_date
        FROM data_clean_v1
        WHERE domain=%s AND process_status=5
        """,
        (args.domain,),
    )
    rem = cursor.fetchone() or {}
    print(
        "Remaining in Step 5 (not finalized): {total:,} | missing orig_list_time: {missing_orig_list_time:,} | missing std_date: {missing_std_date:,}".format(
            total=int(rem.get("total") or 0),
            missing_orig_list_time=int(rem.get("missing_orig_list_time") or 0),
            missing_std_date=int(rem.get("missing_std_date") or 0),
        )
    )
    print(f"Skipped (wrong status): {skipped_wrong_status:,}")
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
