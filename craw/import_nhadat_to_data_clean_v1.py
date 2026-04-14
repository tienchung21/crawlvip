import argparse
from datetime import datetime
import time
from typing import Any, Dict, List, Tuple

import pymysql


def get_connection():
    return pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="",
        database="craw_db",
        port=3306,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def _make_ad_id(realestate_id: Any) -> str:
    return f"nhadat_{realestate_id}"


def _has_column(conn, table: str, col: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=DATABASE()
              AND TABLE_NAME=%s
              AND COLUMN_NAME=%s
            """,
            (table, col),
        )
        row = cur.fetchone()
        if isinstance(row, dict):
            return int(row.get("c") or 0) > 0
        return int(row[0]) > 0


def fetch_rows(conn, limit: int, order: str) -> List[Dict[str, Any]]:
    return fetch_rows2(conn, limit=limit, order=order, include_converted=False)


def fetch_rows2(conn, limit: int, order: str, include_converted: bool) -> List[Dict[str, Any]]:
    order_sql = "DESC" if order.lower() == "desc" else "ASC"
    where_converted = "" if include_converted else "WHERE COALESCE(converted,0)=0"
    has_price_vnd = _has_column(conn, "nhadat_data", "price_vnd")
    price_expr = "price_vnd" if has_price_vnd else "price"
    sql = f"""
        SELECT
            realestate_id, trade_type, category_id, category_name,
            price, {price_expr} AS price_vnd, project_id, orig_list_time,
            city_id, ward_id, street_id, area, area_unit,
            fetched_at
        FROM nhadat_data
        {where_converted}
        ORDER BY realestate_id {order_sql}
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


def build_insert_values(rows: List[Dict[str, Any]]) -> List[Tuple]:
    out: List[Tuple] = []
    for r in rows:
        realestate_id = r.get("realestate_id")
        ad_id = _make_ad_id(realestate_id)

        # A: required core fields
        domain = "nhadat"
        last_script = "import_nhadat_data"
        process_status = 0

        # B: location - save straight to CF columns
        cf_province_id = r.get("city_id")
        cf_ward_id = r.get("ward_id")  # keep 0 if API returns 0
        cf_street_id = r.get("street_id")
        project_id = r.get("project_id")
        if project_id in (0, "0"):
            project_id = None

        # C: raw fields
        # User request: src_size = area only (no unit in the string).
        src_size = str(r["area"]) if r.get("area") not in (None, "") else None
        unit = str(r["area_unit"]) if r.get("area_unit") not in (None, "") else None
        # Prefer normalized price_vnd if available (may be computed from price/area for m2).
        effective_price = r.get("price_vnd")
        if effective_price is None:
            effective_price = r.get("price")
        src_price = str(effective_price) if effective_price is not None else None
        src_category_id = str(r["category_id"]) if r.get("category_id") is not None else None
        src_type = str(r["trade_type"]) if r.get("trade_type") is not None else None

        # Derived fields (minimal)
        price_vnd = effective_price
        # User request: do not fill std_area / std_date.
        std_area = None

        # User request: orig_list_time = orig_list_time.
        # NOTE: data_clean_v1.orig_list_time is BIGINT, while nhadat_data.orig_list_time is dd/mm/yyyy.
        # Convert to epoch milliseconds at 00:00:00 local time so it's numeric and still represents the same date.
        orig_list_time_raw = r.get("orig_list_time")

        out.append(
            (
                ad_id,
                cf_province_id,
                cf_ward_id,
                cf_street_id,
                project_id,
                src_size,
                unit,
                src_price,
                src_category_id,
                src_type,
                std_area,
                price_vnd,
                orig_list_time_raw,
                domain,
                last_script,
                process_status,
            )
        )
    return out


def insert_into_data_clean_v1(conn, values: List[Tuple]) -> int:
    if not values:
        return 0
    sql = """
        INSERT IGNORE INTO data_clean_v1 (
            ad_id,
            cf_province_id,
            cf_ward_id,
            cf_street_id,
            project_id,
            src_size,
            unit,
            src_price,
            src_category_id,
            src_type,
            std_area,
            price_vnd,
            std_date,
            orig_list_time,
            domain,
            last_script,
            process_status
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            NULL,
            CAST(DATE_FORMAT(STR_TO_DATE(%s, '%%d/%%m/%%Y'), '%%Y%%m%%d') AS UNSIGNED),
            %s,
            %s,
            %s
        )
    """
    with conn.cursor() as cur:
        cur.executemany(sql, values)
        return cur.rowcount


def upsert_into_data_clean_v1(conn, values: List[Tuple]) -> int:
    if not values:
        return 0
    # Only backfill fields that are safe to update for existing nhadat rows.
    # We do NOT touch process_status so Step pipeline remains consistent.
    sql = """
        INSERT INTO data_clean_v1 (
            ad_id,
            cf_province_id,
            cf_ward_id,
            cf_street_id,
            project_id,
            src_size,
            unit,
            src_price,
            src_category_id,
            src_type,
            std_area,
            price_vnd,
            std_date,
            orig_list_time,
            domain,
            last_script,
            process_status
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            NULL,
            CAST(DATE_FORMAT(STR_TO_DATE(%s, '%%d/%%m/%%Y'), '%%Y%%m%%d') AS UNSIGNED),
            %s,
            %s,
            %s
        )
        ON DUPLICATE KEY UPDATE
            cf_province_id = VALUES(cf_province_id),
            cf_ward_id = VALUES(cf_ward_id),
            cf_street_id = VALUES(cf_street_id),
            project_id = VALUES(project_id),
            src_size = VALUES(src_size),
            unit = VALUES(unit),
            src_price = VALUES(src_price),
            src_category_id = VALUES(src_category_id),
            src_type = VALUES(src_type),
            price_vnd = VALUES(price_vnd),
            orig_list_time = VALUES(orig_list_time),
            last_script = VALUES(last_script)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, values)
        return cur.rowcount


def mark_converted(conn, realestate_ids: List[int]) -> int:
    if not realestate_ids:
        return 0
    # Chunk to avoid very long SQL with huge IN lists.
    total = 0
    chunk_size = 1000
    with conn.cursor() as cur:
        for i in range(0, len(realestate_ids), chunk_size):
            chunk = realestate_ids[i : i + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))
            cur.execute(f"UPDATE nhadat_data SET converted=1 WHERE realestate_id IN ({placeholders})", chunk)
            total += cur.rowcount
    return total


def preview(values: List[Tuple]) -> None:
    cols = [
        "ad_id",
        "cf_province_id",
        "cf_ward_id",
        "cf_street_id",
        "project_id",
        "src_size",
        "unit",
        "src_price",
        "src_category_id",
        "src_type",
        "std_area",
        "price_vnd",
        "orig_list_time(dd/mm/yyyy)",
        "domain",
        "last_script",
        "process_status",
    ]
    print("Preview mapping (first rows):")
    print(" | ".join(cols))
    for v in values:
        print(" | ".join("" if x is None else str(x) for x in v))


def _count_remaining(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM nhadat_data WHERE COALESCE(converted,0)=0")
        row = cur.fetchone()
        if isinstance(row, dict):
            return int(row.get("c") or 0)
        return int(row[0])


def main():
    parser = argparse.ArgumentParser(description="Import nhadat_data -> data_clean_v1 (A,B,C minimal)")
    parser.add_argument("--limit", type=int, default=5, help="Number of rows to import (single batch)")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size when using --loop (default 5000)")
    parser.add_argument("--order", choices=["asc", "desc"], default="asc", help="Order by realestate_id")
    parser.add_argument("--dry-run", action="store_true", help="Do not write DB (overrides auto mode)")
    parser.add_argument("--apply", action="store_true", help="Actually insert into data_clean_v1")
    parser.add_argument(
        "--upsert",
        action="store_true",
        help="Insert or update existing row (fills unit/cf fields/src fields/orig_list_time) by ad_id",
    )
    parser.add_argument("--loop", action="store_true", help="Run in batches until nhadat_data.converted=0 is exhausted")
    parser.add_argument("--max-batches", type=int, default=0, help="Safety cap for --loop (0=unlimited)")
    parser.add_argument("--no-preview", action="store_true", help="Do not print preview rows")
    parser.add_argument("--log-file", default="", help="Append logs to this file path (optional)")
    parser.add_argument("--include-converted", action="store_true", help="Include rows already marked converted=1")
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help="Update existing data_clean_v1 rows for domain='nhadat' to orig_list_time=YYYYMMDD from nhadat_data",
    )
    parser.add_argument(
        "--update-existing-unit",
        action="store_true",
        help="Update existing data_clean_v1 rows for domain='nhadat' to unit=nhadat_data.area_unit by ad_id",
    )
    parser.add_argument(
        "--update-existing-project-id",
        action="store_true",
        help="Update existing data_clean_v1 rows for domain='nhadat' to project_id=nhadat_data.project_id by ad_id",
    )
    args = parser.parse_args()

    # Auto mode: if user runs this script without specifying any action flags,
    # assume they want to import everything (apply + upsert + loop).
    any_action = (
        args.apply
        or args.loop
        or args.upsert
        or args.update_existing
        or args.update_existing_unit
        or args.update_existing_project_id
        or args.include_converted
        or args.no_preview
        or bool(args.log_file)
    )
    if (not any_action) and (not args.dry_run):
        args.apply = True
        args.upsert = True
        args.loop = True
        args.no_preview = True
        args.log_file = "craw/auto/nhadat_convert.log"

    conn = get_connection()

    def log(msg: str) -> None:
        print(msg, flush=True)
        if args.log_file:
            with open(args.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")

    if args.dry_run:
        args.apply = False
        args.loop = False

    if args.loop and not args.apply:
        raise SystemExit("--loop requires --apply")

    if not args.loop:
        rows = fetch_rows2(conn, limit=args.limit, order=args.order, include_converted=args.include_converted)
        values = build_insert_values(rows)
        if not args.no_preview:
            preview(values)

        if (
            not args.apply
            and not args.update_existing
            and not args.update_existing_unit
            and not args.update_existing_project_id
        ):
            log("\nDry-run only. Use --apply to insert.")
            return

        inserted = 0
        if args.apply:
            if args.upsert:
                inserted = upsert_into_data_clean_v1(conn, values)
                log(f"\nUpsert rows (INSERT ... ON DUPLICATE KEY UPDATE): {inserted}")
            else:
                inserted = insert_into_data_clean_v1(conn, values)
                log(f"\nInserted rows (INSERT IGNORE): {inserted}")
            # Mark converted for processed rows, regardless of INSERT IGNORE outcome (may already exist in data_clean_v1).
            realestate_ids = [int(r["realestate_id"]) for r in rows if r.get("realestate_id") is not None]
            marked = mark_converted(conn, realestate_ids)
            log(f"Marked converted=1 in nhadat_data: {marked}")

        updated = 0
        if args.update_existing:
            # Convert existing ms-style orig_list_time to YYYYMMDD using authoritative nhadat_data.orig_list_time (dd/mm/yyyy).
            # ad_id format: "nhadat_<realestate_id>"
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE data_clean_v1 d
                    JOIN nhadat_data n
                      ON n.realestate_id = CAST(SUBSTRING(d.ad_id, 8) AS UNSIGNED)
                    SET d.orig_list_time = CAST(DATE_FORMAT(STR_TO_DATE(n.orig_list_time, '%d/%m/%Y'), '%Y%m%d') AS UNSIGNED)
                    WHERE d.domain='nhadat'
                      AND d.ad_id LIKE 'nhadat_%'
                      AND (d.orig_list_time IS NULL OR d.orig_list_time = 0 OR d.orig_list_time > 1000000000000)
                    """
                )
                updated = cur.rowcount
            log(f"\nUpdated existing rows (orig_list_time -> YYYYMMDD): {updated}")

        updated_unit = 0
        if args.update_existing_unit:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE data_clean_v1 d
                    JOIN nhadat_data n
                      ON n.realestate_id = CAST(SUBSTRING(d.ad_id, 8) AS UNSIGNED)
                    SET d.unit = NULLIF(n.area_unit, '')
                    WHERE d.domain='nhadat'
                      AND d.ad_id LIKE 'nhadat_%'
                      AND (d.unit IS NULL OR d.unit = '')
                    """
                )
                updated_unit = cur.rowcount
            log(f"\nUpdated existing rows (unit <- nhadat_data.area_unit): {updated_unit}")

        updated_project = 0
        if args.update_existing_project_id:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE data_clean_v1 d
                    JOIN nhadat_data n
                      ON n.realestate_id = CAST(SUBSTRING(d.ad_id, 8) AS UNSIGNED)
                    SET d.project_id = NULLIF(n.project_id, 0)
                    WHERE d.domain='nhadat'
                      AND d.ad_id LIKE 'nhadat_%'
                      AND (d.project_id IS NULL OR d.project_id = 0)
                      AND n.project_id IS NOT NULL
                      AND n.project_id != 0
                    """
                )
                updated_project = cur.rowcount
            log(f"\nUpdated existing rows (project_id <- nhadat_data.project_id): {updated_project}")

        # Show back what we inserted/exists
        if values:
            ad_ids = [v[0] for v in values]
            with conn.cursor() as cur:
                placeholders = ", ".join(["%s"] * len(ad_ids))
                cur.execute(
                    f"""
                    SELECT ad_id, cf_province_id, cf_ward_id, cf_street_id, project_id, src_size, unit, src_price, src_category_id, src_type,
                           std_area, price_vnd, std_date, orig_list_time, domain, process_status
                    FROM data_clean_v1
                    WHERE ad_id IN ({placeholders})
                    ORDER BY ad_id
                    """,
                    ad_ids,
                )
                got = cur.fetchall()
            log("\nRows in data_clean_v1:")
            for r in got:
                log(
                    f"{r['ad_id']} | prov={r['cf_province_id']} ward={r['cf_ward_id']} street={r['cf_street_id']} | "
                    f"project_id={r.get('project_id')} | "
                    f"size={r['src_size']}{('/'+r['unit']) if r.get('unit') else ''} price={r['price_vnd']} | "
                    f"orig_list_time={r['orig_list_time']} | domain={r['domain']}"
                )
        return

    # --loop mode: run batches until converted=0 exhausted
    batch = 0
    total_upsert = 0
    total_marked = 0
    start_all = time.time()
    while True:
        if args.max_batches and batch >= args.max_batches:
            log(f"Reached --max-batches={args.max_batches}. Stopping.")
            break
        batch += 1

        remaining_before = _count_remaining(conn)
        if remaining_before <= 0:
            log("No remaining nhadat_data rows with converted=0. Done.")
            break

        log(f"\n=== BATCH {batch} | remaining(converted=0) before: {remaining_before:,} | batch_size={args.batch_size} ===")
        rows = fetch_rows2(conn, limit=args.batch_size, order=args.order, include_converted=False)
        if not rows:
            log("No rows fetched. Done.")
            break

        values = build_insert_values(rows)
        if not args.no_preview:
            preview(values[:5])

        if args.upsert:
            up = upsert_into_data_clean_v1(conn, values)
            log(f"Upsert rowcount: {up}")
            total_upsert += up
        else:
            ins = insert_into_data_clean_v1(conn, values)
            log(f"Insert rowcount: {ins}")
            total_upsert += ins

        realestate_ids = [int(r["realestate_id"]) for r in rows if r.get("realestate_id") is not None]
        marked = mark_converted(conn, realestate_ids)
        total_marked += marked
        remaining_after = _count_remaining(conn)
        log(f"Marked converted=1: {marked} | remaining(converted=0) after: {remaining_after:,}")

        # Stop if we didn't reduce remaining (safety against infinite loops)
        if remaining_after >= remaining_before and len(rows) < args.batch_size:
            log("Remaining did not decrease and fetched less than batch_size. Stopping to avoid loop.")
            break

    dur = time.time() - start_all
    log(f"\n=== LOOP DONE === batches={batch} total_db_rowcount={total_upsert} total_marked={total_marked} duration={dur:.2f}s")


if __name__ == "__main__":
    main()
