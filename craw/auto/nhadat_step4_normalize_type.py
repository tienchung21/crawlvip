import argparse
import time

import pymysql


BATCH_SIZE_DEFAULT = 5000


def _parse_statuses(s: str) -> list[int]:
    out: list[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out or [3]


def main():
    parser = argparse.ArgumentParser(description="Step 4 (Type/Category) for data_clean_v1 domain nhadat")
    parser.add_argument("--domain", default="nhadat", help="Domain in data_clean_v1 (default: nhadat)")
    parser.add_argument(
        "--statuses",
        default="3",
        help="Process statuses to (re)compute std_trans_type/std_category for (default: 3). Use 3,4 to repair.",
    )
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help="Batch size")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between batches (default 0)")
    parser.add_argument("--log-file", default="", help="Append logs to this file path (optional)")
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

    script_name = "nhadat_step4_normalize_type.py"
    log(f"=== Running {script_name} (domain={args.domain}) ===")
    start_time = time.time()

    cursor.execute("SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s", (args.domain,))
    total_domain = int((cursor.fetchone() or {}).get("c") or 0)
    cursor.execute(
        f"SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s AND process_status IN ({statuses_sql})",
        (args.domain,),
    )
    total_target = int((cursor.fetchone() or {}).get("c") or 0)
    skipped_wrong_status = max(total_domain - total_target, 0)

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN src_type IS NULL OR src_type='' THEN 1 ELSE 0 END) AS missing_src_type,
            SUM(CASE WHEN src_category_id IS NULL OR src_category_id='' THEN 1 ELSE 0 END) AS missing_src_category,
            SUM(CASE WHEN std_trans_type IS NULL OR std_trans_type='' THEN 1 ELSE 0 END) AS missing_std_trans_type,
            SUM(CASE WHEN std_category IS NULL OR std_category='' THEN 1 ELSE 0 END) AS missing_std_category
        FROM data_clean_v1
        WHERE domain=%s AND process_status IN ("""
        + statuses_sql
        + """)
        """,
        (args.domain,),
    )
    stat = cursor.fetchone() or {}
    log(
        "Target rows (process_status in {statuses}): {total:,} | missing src_type: {missing_src_type:,} | missing src_category_id: {missing_src_category:,} | missing std_trans_type: {missing_std_trans_type:,} | missing std_category: {missing_std_category:,}".format(
            statuses=statuses_sql,
            total=int(stat.get("total") or 0),
            missing_src_type=int(stat.get("missing_src_type") or 0),
            missing_src_category=int(stat.get("missing_src_category") or 0),
            missing_std_trans_type=int(stat.get("missing_std_trans_type") or 0),
            missing_std_category=int(stat.get("missing_std_category") or 0),
        )
    )
    log(f"Skipped (wrong status): {skipped_wrong_status:,} / total_domain={total_domain:,}")

    if args.dry_run:
        log("Dry-run: no updates executed.")
        cursor.close()
        conn.close()
        return

    log("--- Phase 1: Normalizing trans_type + copying category ---")
    total_updated = 0
    while True:
        cursor.execute(
            f"""
            UPDATE data_clean_v1
            SET
                std_category = src_category_id,
                std_trans_type = CASE
                    WHEN src_type IN ('1','2',1,2) THEN 's'
                    WHEN src_type IN ('3',3) THEN 'u'
                    ELSE std_trans_type
                END
            WHERE domain=%s
              AND process_status IN ({statuses_sql})
              AND (
                std_category IS NULL OR std_category <> src_category_id
                OR std_trans_type IS NULL OR std_trans_type = ''
              )
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

    log(f"-> Updated {total_updated} rows.")

    # Finalize only rows that actually completed Step 4.
    # Completion rule: std_trans_type + std_category not NULL/empty.
    log("Finalizing step status (process_status 3 -> 4, only completed rows)...")
    total_finalized = 0
    while True:
        cursor.execute(
            f"""
            UPDATE data_clean_v1
            SET process_status=4, last_script=%s
            WHERE domain=%s
              AND process_status=3
              AND std_trans_type IS NOT NULL AND std_trans_type <> ''
              AND std_category IS NOT NULL AND std_category <> ''
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
    log(f"-> Updated process_status = 4 for {total_finalized} rows.")

    cursor.execute(
        """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN std_trans_type IS NULL OR std_trans_type='' THEN 1 ELSE 0 END) AS missing_std_trans_type,
          SUM(CASE WHEN std_category IS NULL OR std_category='' THEN 1 ELSE 0 END) AS missing_std_category
        FROM data_clean_v1
        WHERE domain=%s AND process_status=3
        """,
        (args.domain,),
    )
    rem = cursor.fetchone() or {}
    log(
        "Remaining in Step 3 (not finalized): {total:,} | missing std_trans_type: {missing_std_trans_type:,} | missing std_category: {missing_std_category:,}".format(
            total=int(rem.get("total") or 0),
            missing_std_trans_type=int(rem.get("missing_std_trans_type") or 0),
            missing_std_category=int(rem.get("missing_std_category") or 0),
        )
    )

    end_time = time.time()
    log(f"Skipped (wrong status): {skipped_wrong_status:,}")
    log(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
