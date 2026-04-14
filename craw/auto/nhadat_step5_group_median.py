import argparse
import time

import pymysql


BATCH_SIZE_DEFAULT = 5000


def _ensure_column(cursor, conn):
    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN median_group TINYINT NULL")
        conn.commit()
        print("Added column median_group to data_clean_v1")
    except Exception:
        pass


def _parse_statuses(s: str) -> list[int]:
    out: list[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out or [4]


def main():
    parser = argparse.ArgumentParser(description="Step 5 (Median group) for data_clean_v1 domain nhadat")
    parser.add_argument("--domain", default="nhadat", help="Domain in data_clean_v1 (default: nhadat)")
    parser.add_argument(
        "--statuses",
        default="4",
        help="Process statuses to (re)compute median_group for (default: 4)",
    )
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help="Batch size")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between batches (default 0)")
    parser.add_argument("--log-file", default="", help="Append logs to this file path (optional)")
    parser.add_argument(
        "--recompute",
        action="store_true",
        help="Recompute median_group even if already set (default: only fill when NULL)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print stats only, do not update DB")
    args = parser.parse_args()
    statuses = _parse_statuses(args.statuses)
    statuses_sql = ", ".join(str(int(x)) for x in statuses)

    def log(msg: str) -> None:
        print(msg, flush=True)
        if args.log_file:
            with open(args.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")

    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="",
        database="craw_db",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    cursor = conn.cursor()

    script_name = "nhadat_step5_group_median.py"
    log(f"=== Running {script_name} (domain={args.domain}) ===")
    start_time = time.time()

    _ensure_column(cursor, conn)

    cursor.execute("SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s", (args.domain,))
    total_domain = int((cursor.fetchone() or {}).get("c") or 0)
    cursor.execute(
        f"SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s AND process_status IN ({statuses_sql})",
        (args.domain,),
    )
    total_target = int((cursor.fetchone() or {}).get("c") or 0)
    skipped_wrong_status = max(total_domain - total_target, 0)

    cursor.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN std_trans_type IS NULL OR std_trans_type='' THEN 1 ELSE 0 END) AS missing_std_trans_type,
            SUM(CASE WHEN std_category IS NULL OR std_category='' THEN 1 ELSE 0 END) AS missing_std_category,
            SUM(CASE WHEN median_group IS NULL THEN 1 ELSE 0 END) AS missing_median_group
        FROM data_clean_v1
        WHERE domain=%s AND process_status IN ({statuses_sql})
        """,
        (args.domain,),
    )
    stat = cursor.fetchone() or {}
    log(
        "Target rows (process_status in {statuses}): {total:,} | missing std_trans_type: {missing_std_trans_type:,} | missing std_category: {missing_std_category:,} | missing median_group: {missing_median_group:,}".format(
            statuses=statuses_sql,
            total=int(stat.get("total") or 0),
            missing_std_trans_type=int(stat.get("missing_std_trans_type") or 0),
            missing_std_category=int(stat.get("missing_std_category") or 0),
            missing_median_group=int(stat.get("missing_median_group") or 0),
        )
    )
    log(f"Skipped (wrong status): {skipped_wrong_status:,} / total_domain={total_domain:,}")

    if args.dry_run:
        log("Dry-run: no updates executed.")
        cursor.close()
        conn.close()
        return

    # Mapping per user:
    # - type 'u' -> group 4
    # - type 's' -> by std_category (ID as string):
    #   group 1: 1,2,3
    #   group 2: 5,56
    #   group 3: 8,10,11
    extra_where = "" if args.recompute else "AND median_group IS NULL"

    # Only target rows that will actually change, otherwise cursor.rowcount can be < LIMIT
    # and we would stop early even though there are more updatable rows later.
    update_target_where = """
      AND (
        std_trans_type = 'u'
        OR (std_trans_type = 's' AND std_category IN ('1','2','3','5','56','8','10','11'))
      )
    """

    total_updated = 0
    while True:
        cursor.execute(
            f"""
            UPDATE data_clean_v1
            SET median_group = CASE
                WHEN std_trans_type = 'u' THEN 4
                WHEN std_trans_type = 's' AND std_category IN ('1','2','3') THEN 1
                WHEN std_trans_type = 's' AND std_category IN ('5','56') THEN 2
                WHEN std_trans_type = 's' AND std_category IN ('8','10','11') THEN 3
                ELSE median_group
            END
            WHERE domain=%s
              AND process_status IN ({statuses_sql})
              {extra_where}
              {update_target_where}
            ORDER BY id
            LIMIT {int(args.batch_size)}
            """,
            (args.domain,),
        )
        rows = cursor.rowcount
        conn.commit()
        if rows == 0:
            break
        total_updated += rows
        log(f"  Batch: +{rows} rows (Total: {total_updated})")
        if args.sleep:
            time.sleep(args.sleep)

    log(f"-> Updated median_group for {total_updated} rows.")

    # Summary
    cursor.execute(
        f"""
        SELECT median_group, COUNT(*) AS total
        FROM data_clean_v1
        WHERE domain=%s AND process_status IN ({statuses_sql})
        GROUP BY median_group
        ORDER BY median_group
        """,
        (args.domain,),
    )
    log(f"=== SUMMARY (process_status in {statuses_sql}) ===")
    for r in cursor.fetchall():
        log(f"Group {r['median_group']}: {r['total']} rows")

    # Finalize only rows that actually completed Step 5.
    # Completion rule: median_group is NOT NULL.
    log("Finalizing step status (process_status 4 -> 5, only completed rows)...")
    total_finalized = 0
    while True:
        cursor.execute(
            f"""
            UPDATE data_clean_v1
            SET process_status=5, last_script=%s
            WHERE domain=%s
              AND process_status=4
              AND median_group IS NOT NULL
            ORDER BY id
            LIMIT {int(args.batch_size)}
            """,
            (script_name, args.domain),
        )
        rows = cursor.rowcount
        conn.commit()
        if rows == 0:
            break
        total_finalized += rows
        log(f"  Batch: +{rows} rows (Total: {total_finalized})")
        if args.sleep:
            time.sleep(args.sleep)
    log(f"-> Updated process_status = 5 for {total_finalized} rows.")

    cursor.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN median_group IS NULL THEN 1 ELSE 0 END) AS missing_median_group
        FROM data_clean_v1
        WHERE domain=%s AND process_status=4
        """,
        (args.domain,),
    )
    rem = cursor.fetchone() or {}
    log(
        "Remaining in Step 4 (not finalized): {total:,} | missing median_group: {missing_median_group:,}".format(
            total=int(rem.get("total") or 0),
            missing_median_group=int(rem.get("missing_median_group") or 0),
        )
    )

    end_time = time.time()
    log(f"Skipped (wrong status): {skipped_wrong_status:,}")
    log(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
