import argparse
import time

import pymysql


BATCH_SIZE_DEFAULT = 5000


def main():
    parser = argparse.ArgumentParser(description="Step 2 (Price) for data_clean_v1 domain nhadat")
    parser.add_argument("--domain", default="nhadat", help="Domain in data_clean_v1 (default: nhadat)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help="Batch size")
    parser.add_argument(
        "--enable-delete",
        action="store_true",
        help="(Deprecated) Enable deleting low-price rows (default: off). Prefer handling in Step 3 logic instead.",
    )
    parser.add_argument(
        "--min-price",
        type=int,
        default=500000,
        help="(Deprecated) Min price threshold used only when --enable-delete is set",
    )
    parser.add_argument(
        "--drop-src-types",
        default="1,2",
        help="(Deprecated) Comma-separated src_type values used only when --enable-delete is set",
    )
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

    script_name = "nhadat_step2_normalize_price.py"
    print(f"=== Running {script_name} (domain={args.domain}) ===")
    start_time = time.time()

    cursor.execute("SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s", (args.domain,))
    total_domain = int((cursor.fetchone() or {}).get("c") or 0)
    cursor.execute(
        "SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s AND process_status=1",
        (args.domain,),
    )
    total_target = int((cursor.fetchone() or {}).get("c") or 0)
    skipped_wrong_status = max(total_domain - total_target, 0)
    print(f"Skipped (wrong status): {skipped_wrong_status:,} / total_domain={total_domain:,}")

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_step1,
            SUM(CASE WHEN src_price IS NULL OR src_price = '' THEN 1 ELSE 0 END) AS missing_src_price,
            SUM(CASE WHEN price_vnd IS NULL THEN 1 ELSE 0 END) AS missing_price_vnd
        FROM data_clean_v1
        WHERE domain=%s AND process_status=1
        """,
        (args.domain,),
    )
    stat = cursor.fetchone() or {}
    print(
        "Pending (process_status=1): {total_step1:,} | missing src_price: {missing_src_price:,} | missing price_vnd: {missing_price_vnd:,}".format(
            total_step1=int(stat.get("total_step1") or 0),
            missing_src_price=int(stat.get("missing_src_price") or 0),
            missing_price_vnd=int(stat.get("missing_price_vnd") or 0),
        )
    )

    if args.dry_run:
        print("Dry-run: no updates executed.")
        cursor.close()
        conn.close()
        return

    # Deletion logic removed from normal Step 2 flow per user request.
    # Keep flags for backward compatibility; require explicit opt-in.
    if args.enable_delete:
        drop_types = [t.strip() for t in str(args.drop_src_types).split(",") if t.strip() != ""]
        if drop_types and args.min_price is not None:
            print(f"[DEPRECATED] Dropping rows where src_type in {drop_types} AND src_price < {args.min_price:,} ...")
            total_deleted = 0
            placeholders = ", ".join(["%s"] * len(drop_types))
            while True:
                cursor.execute(
                    f"""
                    DELETE FROM data_clean_v1
                    WHERE domain=%s
                      AND process_status IN (1,2)
                      AND src_type IN ({placeholders})
                      AND src_price REGEXP '^[0-9]+$'
                      AND CAST(src_price AS UNSIGNED) < %s
                    LIMIT {int(args.batch_size)}
                    """,
                    [args.domain, *drop_types, int(args.min_price)],
                )
                deleted = cursor.rowcount
                conn.commit()
                if deleted == 0:
                    break
                total_deleted += deleted
                print(f"  Batch delete: -{deleted} rows (Total: {total_deleted})")
                if deleted < args.batch_size:
                    break
            print(f"-> Deleted: {total_deleted} rows.")

    # Nhadat: src_price is already numeric (API gives VND number).
    # Fast-path: bulk SQL update in batches.
    total_updated = 0
    while True:
        cursor.execute(
            f"""
            UPDATE data_clean_v1
            SET price_vnd = CAST(src_price AS UNSIGNED)
            WHERE domain=%s
              AND process_status=1
              AND price_vnd IS NULL
              AND src_price REGEXP '^[0-9]+$'
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
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < args.batch_size:
            break

    print(f"-> Copied src_price -> price_vnd for {total_updated} rows.")

    # Finalize only rows that have price_vnd after Step 2.
    print("Finalizing step status (process_status 1 -> 2, only price_vnd NOT NULL)...")
    total_finalized = 0
    while True:
        cursor.execute(
            f"""
            UPDATE data_clean_v1
            SET process_status=2, last_script=%s
            WHERE domain=%s
              AND process_status=1
              AND price_vnd IS NOT NULL
              AND price_vnd > 0
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
        print(f"  Batch finalize: +{rows} rows (Total: {total_finalized})")
        if rows < args.batch_size:
            break
    print(f"-> Updated process_status = 2 for {total_finalized} rows.")

    cursor.execute(
        """
        SELECT COUNT(*) AS c
        FROM data_clean_v1
        WHERE domain=%s AND process_status=1
        """,
        (args.domain,),
    )
    remaining_step1 = int((cursor.fetchone() or {}).get("c") or 0)
    print(f"Skipped (missing/invalid price in status=1): {remaining_step1:,}")

    end_time = time.time()
    print(f"Skipped (wrong status): {skipped_wrong_status:,}")
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
