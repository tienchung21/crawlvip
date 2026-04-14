import argparse
import time
from typing import List

import pymysql


# data_clean_v1.price_m2 is DECIMAL(18,2) => max integer part has 16 digits.
MAX_DECIMAL18_INT = 9_999_999_999_999_999


def _parse_int_list(s: str) -> List[int]:
    out: List[int] = []
    for part in str(s).split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return sorted(set(out))


def main():
    parser = argparse.ArgumentParser(description="Step 3 (Size + price_m2) for data_clean_v1 domain nhadat")
    parser.add_argument("--domain", default="nhadat", help="Domain in data_clean_v1 (default: nhadat)")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for UPDATE LIMIT")
    parser.add_argument(
        "--statuses",
        default="2",
        help="process_status values to process (default: 2). Use 2,3 to repair/recompute.",
    )
    parser.add_argument(
        "--min-price-vnd",
        type=int,
        default=50_000,
        help="Do not finalize to Step 3 if price_vnd < this (default 50000)",
    )
    parser.add_argument("--recompute", action="store_true", help="Recompute even if price_m2 already set")
    parser.add_argument(
        "--m2-adjust-min-total",
        type=int,
        default=500_000_000,
        help="If unit='m2' and price_vnd>=this, treat price_vnd as total price and use price_vnd/std_area",
    )
    parser.add_argument(
        "--max-price-m2",
        type=int,
        default=0,
        help="Optional outlier cap. If >0 and computed price_m2 > this, set price_m2=NULL. Default off (0).",
    )
    parser.add_argument(
        "--units",
        default="m2,md,thang",
        help="Units to compute price_m2 for (default: m2,md,thang)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print stats only, do not update DB")
    args = parser.parse_args()

    statuses = _parse_int_list(args.statuses)
    if not statuses:
        raise SystemExit("--statuses is empty")
    status_sql = ",".join(str(x) for x in statuses)

    units = [u.strip().lower() for u in str(args.units).split(",") if u.strip()]
    if not units:
        raise SystemExit("--units is empty")

    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="",
        database="craw_db",
        port=3306,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    cur = conn.cursor()

    script_name = "nhadat_step3_normalize_size.py"
    print(f"=== Running {script_name} (domain={args.domain}, statuses={statuses}) ===")
    start_time = time.time()

    cur.execute("SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s", (args.domain,))
    total_domain = int((cur.fetchone() or {}).get("c") or 0)
    cur.execute(
        f"SELECT COUNT(*) AS c FROM data_clean_v1 WHERE domain=%s AND process_status IN ({status_sql})",
        (args.domain,),
    )
    total_target = int((cur.fetchone() or {}).get("c") or 0)
    skipped_wrong_status = max(total_domain - total_target, 0)
    print(f"Skipped (wrong status): {skipped_wrong_status:,} / total_domain={total_domain:,}")

    cur.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN std_area IS NULL OR std_area=0 THEN 1 ELSE 0 END) AS missing_area,
            SUM(CASE WHEN price_vnd IS NULL OR price_vnd=0 THEN 1 ELSE 0 END) AS missing_price,
            SUM(CASE WHEN price_m2 IS NULL THEN 1 ELSE 0 END) AS missing_price_m2
        FROM data_clean_v1
        WHERE domain=%s
          AND process_status IN ({status_sql})
        """,
        (args.domain,),
    )
    stat = cur.fetchone() or {}
    print(
        "Rows={total:,} missing_area={missing_area:,} missing_price={missing_price:,} missing_price_m2={missing_price_m2:,}".format(
            total=int(stat.get("total") or 0),
            missing_area=int(stat.get("missing_area") or 0),
            missing_price=int(stat.get("missing_price") or 0),
            missing_price_m2=int(stat.get("missing_price_m2") or 0),
        )
    )

    if args.dry_run:
        return

    # 1) Ensure std_area from src_size (numeric strings).
    cur.execute(
        f"""
        UPDATE data_clean_v1
        SET std_area = CAST(REPLACE(src_size, ',', '.') AS DECIMAL(18,4))
        WHERE domain=%s
          AND process_status IN ({status_sql})
          AND (std_area IS NULL OR std_area = 0)
          AND src_size IS NOT NULL AND src_size <> ''
          AND src_size REGEXP '^[0-9]+([\\\\.,][0-9]+)?$'
        """,
        (args.domain,),
    )
    total_area = cur.rowcount
    conn.commit()
    print(f"  Phase1 std_area updated: {total_area}")

    # 2) Compute price_m2.
    # Rule:
    # - unit='m2':
    #   if price_vnd < min_total: keep as-is (treat as already VND/m2)
    #   else: price_vnd/std_area
    # - unit in (md, thang): price_vnd/std_area
    # - If computed > DECIMAL(18,2) limit -> set NULL (skip)
    units_ph = ",".join(["%s"] * len(units))
    recompute_clause = "" if args.recompute else "AND price_m2 IS NULL"

    cur.execute(
        f"""
        UPDATE data_clean_v1
        SET price_m2 = CASE
            WHEN unit = 'm2' THEN
                CASE
                    WHEN price_vnd IS NULL OR price_vnd <= 0 THEN NULL
                    ELSE
                        CASE
	                            WHEN price_vnd < %s THEN
	                                CASE
	                                    WHEN price_vnd > {MAX_DECIMAL18_INT} THEN NULL
	                                    ELSE price_vnd
	                                END
	                            ELSE
	                                CASE
	                                    WHEN std_area IS NULL OR std_area <= 0 THEN NULL
	                                    WHEN (price_vnd / std_area) > {MAX_DECIMAL18_INT}
	                                    THEN NULL
	                                    ELSE (price_vnd / std_area)
	                                END
                        END
                END
	            WHEN unit IN ('md','thang') THEN
	                CASE
	                    WHEN price_vnd IS NULL OR price_vnd <= 0 OR std_area IS NULL OR std_area <= 0 THEN NULL
	                    WHEN (price_vnd / std_area) > {MAX_DECIMAL18_INT} THEN NULL
	                    ELSE (price_vnd / std_area)
	                END
            ELSE NULL
        END
        WHERE domain=%s
          AND process_status IN ({status_sql})
          {recompute_clause}
          AND unit IN ({units_ph})
        """,
        (
            int(args.m2_adjust_min_total),
            args.domain,
            *units,
        ),
    )
    total_m2 = cur.rowcount
    conn.commit()
    print(f"  Phase2 price_m2 updated: {total_m2}")

    # Optional outlier cap (off by default).
    if int(args.max_price_m2) > 0:
        cur.execute(
            f"""
            UPDATE data_clean_v1
            SET price_m2 = NULL
            WHERE domain=%s
              AND process_status IN ({status_sql})
              AND unit IN ({units_ph})
              AND price_m2 IS NOT NULL
              AND price_m2 > %s
            """,
            (args.domain, *units, int(args.max_price_m2)),
        )
        dropped = cur.rowcount
        conn.commit()
        if dropped:
            print(f"  Phase2.5 outlier cap: set price_m2=NULL for {dropped} rows (> {int(args.max_price_m2):,}).")

    # 3) Finalize: bump status 2 -> 3 only for rows that actually completed Step 3.
    # Completion rule:
    # - price_vnd >= min_price_vnd
    # - price_m2 is NOT NULL (computed)
    print("Finalizing step status (process_status 2 -> 3, only completed rows)...")
    total_finalized = 0
    while True:
        cur.execute(
            f"""
            UPDATE data_clean_v1
            SET process_status=3, last_script=%s
            WHERE domain=%s
              AND process_status=2
              AND price_vnd IS NOT NULL
              AND price_vnd >= %s
              AND price_m2 IS NOT NULL
            ORDER BY id
            LIMIT {int(args.batch_size)}
            """,
            (script_name, args.domain, int(args.min_price_vnd)),
        )
        rows = cur.rowcount
        conn.commit()
        if rows == 0:
            break
        total_finalized += rows
        print(f"  Batch finalize: +{rows} rows (Total: {total_finalized})")
        if rows < args.batch_size:
            break

    # Report remaining rows still in step 2 (incomplete).
    cur.execute(
        """
        SELECT
          SUM(CASE WHEN price_vnd IS NULL OR price_vnd=0 THEN 1 ELSE 0 END) AS missing_price,
          SUM(CASE WHEN price_vnd IS NOT NULL AND price_vnd < %s THEN 1 ELSE 0 END) AS too_low_price,
          SUM(CASE WHEN price_m2 IS NULL THEN 1 ELSE 0 END) AS missing_price_m2,
          COUNT(*) AS total
        FROM data_clean_v1
        WHERE domain=%s AND process_status=2
        """,
        (int(args.min_price_vnd), args.domain),
    )
    rem = cur.fetchone() or {}
    print(
        "Remaining in Step 2 (not finalized): {total:,} | missing_price_vnd: {missing_price:,} | price_vnd < {minvnd:,}: {too_low_price:,} | missing_price_m2: {missing_price_m2:,}".format(
            total=int(rem.get("total") or 0),
            missing_price=int(rem.get("missing_price") or 0),
            too_low_price=int(rem.get("too_low_price") or 0),
            missing_price_m2=int(rem.get("missing_price_m2") or 0),
            minvnd=int(args.min_price_vnd),
        )
    )

    end_time = time.time()
    print(f"-> Filled std_area: {total_area}")
    print(f"-> Updated price_m2 rows: {total_m2}")
    print(f"-> Finalized status 2->3: {total_finalized}")
    print(f"Skipped (wrong status): {skipped_wrong_status:,}")
    print(f"=== Finished in {end_time - start_time:.2f}s ===")


if __name__ == "__main__":
    main()
