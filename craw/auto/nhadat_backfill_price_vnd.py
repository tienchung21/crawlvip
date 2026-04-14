import argparse
import time

import pymysql


PRICE_M2_MIN_VND = 500_000
PRICE_TOTAL_MIN_VND_FOR_M2_ADJUST = 500_000_000


def main():
    parser = argparse.ArgumentParser(description="Backfill nhadat_data.price_vnd from price/area for area_unit='m2'")
    parser.add_argument("--batch-size", type=int, default=20000, help="Batch size for batched UPDATE LIMIT")
    parser.add_argument("--min-total", type=int, default=PRICE_TOTAL_MIN_VND_FOR_M2_ADJUST, help="Min total price to adjust")
    parser.add_argument("--min-m2", type=int, default=PRICE_M2_MIN_VND, help="Min computed VND/m2 to accept")
    parser.add_argument("--dry-run", action="store_true", help="Only print how many rows would be updated")
    args = parser.parse_args()

    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="",
        database="craw_db",
        port=3306,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    cur = conn.cursor()

    # Ensure column exists.
    try:
        cur.execute("ALTER TABLE nhadat_data ADD COLUMN price_vnd BIGINT NULL")
    except Exception:
        pass

    # Preview counts.
    cur.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN area_unit='m2' AND price >= %s THEN 1 ELSE 0 END) AS eligible_m2,
            SUM(CASE WHEN price_vnd IS NULL THEN 1 ELSE 0 END) AS missing_price_vnd
        FROM nhadat_data
        """,
        (int(args.min_total),),
    )
    stat = cur.fetchone() or {}
    print(
        "nhadat_data total={total:,} eligible_m2(price>=min_total)={eligible_m2:,} missing price_vnd={missing_price_vnd:,}".format(
            total=int(stat.get("total") or 0),
            eligible_m2=int(stat.get("eligible_m2") or 0),
            missing_price_vnd=int(stat.get("missing_price_vnd") or 0),
        )
    )

    # The core update rule.
    # For eligible m2 rows, compute price/area, accept only if computed >= min_m2 else keep original price.
    update_sql = f"""
        UPDATE nhadat_data
        SET price_vnd = CASE
            WHEN area_unit='m2'
             AND price >= %s
             AND area IS NOT NULL AND area <> ''
             AND area REGEXP '^[0-9]+([\\\\.,][0-9]+)?$'
             AND CAST(REPLACE(area, ',', '.') AS DECIMAL(18,4)) > 0
            THEN
                CASE
                    WHEN (price / CAST(REPLACE(area, ',', '.') AS DECIMAL(18,4))) >= %s
                    THEN CAST(price / CAST(REPLACE(area, ',', '.') AS DECIMAL(18,4)) AS UNSIGNED)
                    ELSE price
                END
            ELSE price
        END
        WHERE price_vnd IS NULL
        ORDER BY realestate_id
        LIMIT {int(args.batch_size)}
    """

    if args.dry_run:
        # Dry-run: approximate "would change" as missing price_vnd.
        print("Dry-run: use without --dry-run to execute updates in batches.")
        return

    total = 0
    start = time.time()
    while True:
        cur.execute(update_sql, (int(args.min_total), int(args.min_m2)))
        n = cur.rowcount
        if n == 0:
            break
        total += n
        print(f"  batch updated: +{n} (total={total})")
        if n < args.batch_size:
            break
    dur = time.time() - start
    print(f"Done. Updated {total} rows in {dur:.2f}s.")


if __name__ == "__main__":
    main()

