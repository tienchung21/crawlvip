import argparse
import time

import pymysql


def connect():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="craw_db",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Step 1 (Region) for data_clean_v1 domain='nhadat' (mark processed only)"
    )
    parser.add_argument("--domain", default="nhadat", help="Domain in data_clean_v1 (default: nhadat)")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for UPDATE LIMIT")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore missing cf_province_id, but still require cf_ward_id to be non-zero",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print stats only, do not update DB")
    args = parser.parse_args()

    script_name = "nhadat_step1_mergekhuvuc.py"
    print(f"=== Running {script_name} (domain={args.domain}) ===")
    start_time = time.time()

    conn = connect()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s", (args.domain,))
    total_domain = int((cursor.fetchone() or {}).get("c") or 0)
    cursor.execute(
        "SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s AND process_status=0",
        (args.domain,),
    )
    total_target = int((cursor.fetchone() or {}).get("c") or 0)
    skipped_wrong_status = max(total_domain - total_target, 0)
    print(f"Skipped (wrong status): {skipped_wrong_status:,} / total_domain={total_domain:,}")

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_pending,
            SUM(CASE WHEN cf_province_id IS NULL OR cf_province_id = 0 THEN 1 ELSE 0 END) AS missing_province,
            SUM(CASE WHEN cf_ward_id IS NULL OR cf_ward_id = 0 THEN 1 ELSE 0 END) AS missing_ward
        FROM data_clean_v1
        WHERE domain = %s AND process_status = 0
        """,
        (args.domain,),
    )
    stat = cursor.fetchone() or {}
    total_pending = int(stat.get("total_pending") or 0)
    missing_province = int(stat.get("missing_province") or 0)
    missing_ward = int(stat.get("missing_ward") or 0)

    print(f"Pending rows (process_status=0): {total_pending:,}")
    print(f"Missing cf_province_id: {missing_province:,}")
    print(f"Missing cf_ward_id: {missing_ward:,}")

    if args.dry_run:
        print("Dry-run: no updates executed.")
        cursor.close()
        conn.close()
        return

    # Always require valid ward mapping for Step 1 finalization.
    where_extra = " AND cf_ward_id IS NOT NULL AND cf_ward_id <> 0"
    if not args.force:
        where_extra += " AND cf_province_id IS NOT NULL AND cf_province_id <> 0"

    print("Finalizing step status (process_status 0 -> 1)...")
    total_updated = 0
    while True:
        sql = f"""
        UPDATE data_clean_v1
        SET process_status = 1,
            last_script = %s
        WHERE domain = %s
          AND process_status = 0
          {where_extra}
        ORDER BY id
        LIMIT {int(args.batch_size)}
        """
        cursor.execute(sql, (script_name, args.domain))
        rows = cursor.rowcount
        conn.commit()
        total_updated += rows
        if rows == 0:
            break
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < args.batch_size:
            break

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")
    print(f"Total rows finalized to Step 1: {total_updated:,}")
    print(f"Skipped (wrong status): {skipped_wrong_status:,}")
    print(f"Skipped (missing location in status=0): {max(total_pending - total_updated, 0):,}")
    if missing_ward > 0:
        print("Note: rows with cf_ward_id missing/0 are never finalized in Step 1.")
    if not args.force and missing_province > 0:
        print("Note: rows with cf_province_id missing/0 were skipped (use --force to override province check only).")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
