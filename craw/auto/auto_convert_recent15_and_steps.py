import argparse
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pymysql


DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASS = ""
DB_NAME = "craw_db"
BATCH_SIZE_DEFAULT = 5000
DEFAULT_DAYS = 15

ROOT = Path(__file__).resolve().parents[2]


PIPELINES: Dict[str, List[str]] = {
    "batdongsan": [
        "craw/auto/batdongsan_step1_mergekhuvuc.py",
        "craw/auto/batdongsan_step2_normalize_price.py",
        "craw/auto/batdongsan_step3_normalize_size.py",
        "craw/auto/batdongsan_step4_normalize_type.py",
        "craw/auto/batdongsan_step5_group_median.py",
        "craw/auto/batdongsan_step6_normalize_date.py",
        "craw/auto/step7_apply_land_price.py --domain batdongsan",
    ],
    "mogi": [
        "craw/auto/mogi_step1_mergekhuvuc_v2.py",
        "craw/auto/mogi_step2_normalize_price.py",
        "craw/auto/mogi_step3_normalize_size.py",
        "craw/auto/mogi_step4_normalize_type.py",
        "craw/auto/mogi_step5_group_median.py",
        "craw/auto/mogi_step6_normalize_date.py",
        "craw/auto/step7_apply_land_price.py --domain mogi",
    ],
    "nhatot": [
        "craw/auto/nhatot_step1_mergekhuvuc_v2.py",
        "craw/auto/nhatot_step2_normalize_price.py",
        "craw/auto/nhatot_step3_normalize_size.py",
        "craw/auto/nhatot_step4_normalize_type.py",
        "craw/auto/nhatot_step5_group_median.py",
        "craw/auto/nhatot_step6_normalize_date.py",
        "craw/auto/step7_apply_land_price.py --domain nhatot",
    ],
    "nhadat": [
        "craw/auto/nhadat_step1_mergekhuvuc.py",
        "craw/auto/nhadat_step2_normalize_price.py",
        "craw/auto/nhadat_step3_normalize_size.py",
        "craw/auto/nhadat_step4_normalize_type.py",
        "craw/auto/nhadat_step5_group_median.py",
        "craw/auto/nhadat_step6_normalize_date.py",
        "craw/auto/step7_apply_land_price.py --domain nhadat",
    ],
}

DOMAIN_VALUE_MAP = {
    "batdongsan": "batdongsan.com.vn",
    "mogi": "mogi",
    "nhatot": "nhatot",
    "nhadat": "nhadat",
}


def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def placeholders(n: int) -> str:
    return ",".join(["%s"] * n)


def has_column(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        row = cur.fetchone()
        return int((row or {}).get("c") or 0) > 0


def convert_batdongsan_recent(conn, cutoff: datetime, batch_size: int) -> Dict[str, int]:
    """
    Logic aligned with recreate_datacleanv1_batdongsan.py, but only recent 15-day source rows.
    "Recent" for BDS is based on collected_links.updated_at (status=done), because
    scraped_details_flat.created_at may reflect listing last_update, not crawl insert time.
    """
    stats = {"batches": 0, "selected": 0, "inserted": 0, "updated": 0, "marked": 0}
    # Keep BDS batches conservative to reduce lock pressure on large tables.
    safe_batch_size = min(batch_size, 50)
    write_chunk_size = 10
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    with conn.cursor() as cur:
        while True:
            cur.execute(
                f"""
                SELECT s.id
                FROM scraped_details_flat s
                WHERE s.domain='batdongsan.com.vn'
                  AND COALESCE(s.cleanv1_converted,0)=0
                  AND s.matin IS NOT NULL AND TRIM(s.matin) <> ''
                  AND EXISTS (
                      SELECT 1
                      FROM collected_links c
                      WHERE c.domain='batdongsan.com.vn'
                        AND c.prj_id = s.link_id
                        AND c.status='done'
                        AND c.updated_at >= %s
                  )
                ORDER BY s.id
                LIMIT %s
                """,
                (cutoff_str, safe_batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            ids = [r["id"] for r in rows]
            stats["batches"] += 1
            stats["selected"] += len(ids)
            # Write in smaller chunks to avoid lock-table overflow (1206).
            start = 0
            while start < len(ids):
                sub_ids = ids[start : start + write_chunk_size]
                ph = placeholders(len(sub_ids))
                try:
                    cur.execute(
                        f"""
                        INSERT IGNORE INTO data_clean_v1 (
                            ad_id,
                            src_province_id, src_district_id, src_ward_id,
                            src_size, src_price, src_category_id, src_type,
                            orig_list_time, update_time,
                            url, domain, process_status, last_script
                        )
                        SELECT
                            s.matin,
                            CAST(s.city_code AS CHAR),
                            CAST(s.district_id AS CHAR),
                            CAST(s.ward_id AS CHAR),
                            LEFT(REPLACE(REPLACE(s.dientich, '\\n', ''), '\\r', ''), 50),
                            LEFT(REPLACE(REPLACE(s.khoanggia, '\\n', ''), '\\r', ''), 50),
                            LEFT(TRIM(s.loaihinh), 50),
                            LEFT(TRIM(s.trade_type), 50),
                            CASE
                              WHEN s.ngaydang REGEXP '^[0-9]{8}$' THEN CAST(s.ngaydang AS UNSIGNED)
                              WHEN s.ngaydang REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                                THEN CAST(DATE_FORMAT(STR_TO_DATE(s.ngaydang, '%%d/%%m/%%Y'), '%%Y%%m%%d') AS UNSIGNED)
                              ELSE NULL
                            END,
                            UNIX_TIMESTAMP(s.created_at),
                            s.url,
                            'batdongsan.com.vn',
                            0,
                            'auto_convert_recent15_and_steps.py'
                        FROM scraped_details_flat s
                        WHERE s.id IN ({ph})
                        """,
                        sub_ids,
                    )
                    stats["inserted"] += cur.rowcount

                    # Skip heavy UPDATE..JOIN against data_clean_v1 to avoid lock-table overflow.
                    # For recent unconverted rows, INSERT IGNORE is sufficient; existing duplicates
                    # will be processed in later domain-specific maintenance jobs if needed.
                    cur.execute(
                        f"""
                        UPDATE scraped_details_flat
                        SET cleanv1_converted=1,
                            cleanv1_converted_at=COALESCE(cleanv1_converted_at, NOW())
                        WHERE id IN ({ph})
                          AND domain='batdongsan.com.vn'
                          AND COALESCE(cleanv1_converted,0)=0
                        """,
                        sub_ids,
                    )
                    stats["marked"] += cur.rowcount
                    conn.commit()
                    # Small breathing room to reduce lock pressure on busy DB.
                    time.sleep(0.05)
                    start += len(sub_ids)
                except pymysql.err.OperationalError as e:
                    conn.rollback()
                    if e.args and e.args[0] == 1206 and write_chunk_size > 10:
                        write_chunk_size = max(10, write_chunk_size // 2)
                        print(
                            f"[BDS] hit lock-table overflow (1206), "
                            f"reduce write_chunk_size -> {write_chunk_size} and retry."
                        )
                        continue
                    raise

            print(
                f"[BDS][Batch {stats['batches']}] selected={len(ids)} "
                f"inserted={stats['inserted']} updated={stats['updated']} marked={stats['marked']}"
            )

    return stats


def convert_mogi_recent(conn, cutoff: datetime, batch_size: int) -> Dict[str, int]:
    """Logic aligned with recreate_datacleanv1_mogi.py, for recent rows in scraped_details_flat."""
    stats = {"batches": 0, "selected": 0, "inserted": 0, "marked": 0, "repaired": 0}
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    with conn.cursor() as cur:
        while True:
            cur.execute(
                """
                SELECT id
                FROM scraped_details_flat
                WHERE domain='mogi'
                  AND COALESCE(flag,0)=0
                  AND created_at >= %s
                ORDER BY id
                LIMIT %s
                """,
                (cutoff_str, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            ids = [r["id"] for r in rows]
            stats["batches"] += 1
            stats["selected"] += len(ids)
            ph = placeholders(len(ids))
            transfer_ts = int(time.time())

            cur.execute(
                f"""
                INSERT INTO data_clean_v1 (
                    ad_id,
                    src_province_id, src_district_id, src_ward_id,
                    src_size, src_price,
                    src_category_id, src_type,
                    orig_list_time, update_time,
                    transfer_time,
                    cf_street_id,
                    url, domain,
                    process_status, last_script
                )
                SELECT
                    COALESCE(NULLIF(TRIM(matin), ''), CAST(id AS CHAR)),
                    COALESCE(CAST(mogi_city_id AS CHAR), city_ext),
                    COALESCE(CAST(mogi_district_id AS CHAR), district_ext),
                    COALESCE(CAST(mogi_ward_id AS CHAR), ward_ext),
                    COALESCE(dientich, dientichsudung),
                    khoanggia,
                    COALESCE(loaihinh, loaibds),
                    trade_type,
                    COALESCE(
                      UNIX_TIMESTAMP(STR_TO_DATE(TRIM(ngaydang), '%%d/%%m/%%Y')),
                      UNIX_TIMESTAMP(STR_TO_DATE(TRIM(ngaydang), '%%Y-%%m-%%d'))
                    ),
                    UNIX_TIMESTAMP(created_at),
                    %s,
                    mogi_street_id,
                    url,
                    domain,
                    0,
                    'auto_convert_recent15_and_steps.py'
                FROM scraped_details_flat
                WHERE id IN ({ph})
                ON DUPLICATE KEY UPDATE
                    orig_list_time = COALESCE(VALUES(orig_list_time), orig_list_time),
                    update_time = COALESCE(VALUES(update_time), update_time),
                    transfer_time = COALESCE(transfer_time, VALUES(transfer_time)),
                    url = COALESCE(VALUES(url), data_clean_v1.url),
                    domain = COALESCE(data_clean_v1.domain, VALUES(domain)),
                    last_script = 'auto_convert_recent15_and_steps.py'
                """,
                [transfer_ts, *ids],
            )
            stats["inserted"] += cur.rowcount

            cur.execute(f"UPDATE scraped_details_flat SET flag=1 WHERE id IN ({ph})", ids)
            stats["marked"] += cur.rowcount

            # Repair only rows from this batch to avoid full-table lock pressure.
            cur.execute(
                f"""
                UPDATE data_clean_v1 d
                JOIN scraped_details_flat s
                  ON COALESCE(NULLIF(TRIM(s.matin),''), CAST(s.id AS CHAR)) COLLATE utf8mb4_unicode_ci = d.ad_id
                 AND s.domain = 'mogi'
                SET
                    d.orig_list_time = COALESCE(
                        UNIX_TIMESTAMP(STR_TO_DATE(TRIM(s.ngaydang), '%%d/%%m/%%Y')),
                        UNIX_TIMESTAMP(STR_TO_DATE(TRIM(s.ngaydang), '%%Y-%%m-%%d'))
                    ),
                    d.update_time = UNIX_TIMESTAMP(s.created_at)
                WHERE s.id IN ({ph})
                  AND d.domain = 'mogi'
                  AND (d.orig_list_time IS NULL OR d.orig_list_time = 0)
                  AND (d.update_time IS NULL OR d.update_time = 0)
                  AND s.ngaydang IS NOT NULL AND TRIM(s.ngaydang) <> ''
                """,
                ids,
            )
            stats["repaired"] += cur.rowcount
            conn.commit()

            print(
                f"[MOGI][Batch {stats['batches']}] selected={len(ids)} "
                f"inserted={stats['inserted']} marked={stats['marked']}"
            )

    return stats


def convert_nhatot_recent(conn, cutoff: datetime, batch_size: int) -> Dict[str, int]:
    """Logic aligned with recreate_data_clean_v2.py, only recent ad_listing_detail rows."""
    stats = {"batches": 0, "selected": 0, "inserted": 0, "marked": 0}
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    with conn.cursor() as cur:
        while True:
            cur.execute(
                """
                SELECT DISTINCT d.list_id
                FROM ad_listing_detail d
                WHERE COALESCE(d.cleanv1_converted,0)=0
                  AND FROM_UNIXTIME(d.time_crawl/1000) >= %s
                ORDER BY d.list_id
                LIMIT %s
                """,
                (cutoff_str, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            list_ids = [r["list_id"] for r in rows]
            stats["batches"] += 1
            stats["selected"] += len(list_ids)
            ph = placeholders(len(list_ids))
            transfer_ts = int(time.time())

            cur.execute(
                f"""
                INSERT IGNORE INTO data_clean_v1 (
                    ad_id,
                    src_province_id, src_district_id, src_ward_id,
                    src_size, src_price,
                    src_category_id, src_type,
                    orig_list_time, update_time,
                    url, domain,
                    transfer_time,
                    process_status, last_script
                )
                SELECT
                    d.list_id,
                    d.region_v2, d.area_v2, d.ward,
                    d.size, d.price_string,
                    d.category, d.type,
                    d.orig_list_time, d.list_time,
                    CONCAT('https://www.nhatot.com/', d.list_id, '.htm'),
                    'nhatot',
                    %s,
                    0,
                    'auto_convert_recent15_and_steps.py'
                FROM ad_listing_detail d
                WHERE d.list_id IN ({ph})
                """,
                [transfer_ts, *list_ids],
            )
            stats["inserted"] += cur.rowcount

            cur.execute(
                f"""
                UPDATE ad_listing_detail
                SET
                    cleanv1_converted=1,
                    cleanv1_converted_at=COALESCE(cleanv1_converted_at, NOW())
                WHERE list_id IN ({ph})
                  AND COALESCE(cleanv1_converted,0)=0
                """,
                list_ids,
            )
            stats["marked"] += cur.rowcount
            conn.commit()

            print(
                f"[NHATOT][Batch {stats['batches']}] selected={len(list_ids)} "
                f"inserted={stats['inserted']} marked={stats['marked']}"
            )

    return stats


def convert_nhadat_recent(conn, cutoff: datetime, batch_size: int) -> Dict[str, int]:
    """
    Convert recent nhadat_data rows to data_clean_v1 and mark nhadat_data.converted=1.
    Logic aligned with import_nhadat_to_data_clean_v1.py, scoped to recent fetched_at.
    """
    stats = {"batches": 0, "selected": 0, "inserted": 0, "updated": 0, "marked": 0}
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    has_price_vnd = has_column(conn, "nhadat_data", "price_vnd")
    price_expr = "n.price_vnd" if has_price_vnd else "n.price"

    with conn.cursor() as cur:
        while True:
            cur.execute(
                """
                SELECT n.realestate_id
                FROM nhadat_data n
                WHERE COALESCE(n.converted,0)=0
                  AND n.fetched_at >= %s
                ORDER BY n.realestate_id
                LIMIT %s
                """,
                (cutoff_str, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            ids = [r["realestate_id"] for r in rows]
            stats["batches"] += 1
            stats["selected"] += len(ids)
            ph = placeholders(len(ids))

            cur.execute(
                f"""
                INSERT IGNORE INTO data_clean_v1 (
                    ad_id,
                    cf_province_id, cf_ward_id, cf_street_id,
                    project_id,
                    src_size, unit, src_price,
                    src_category_id, src_type,
                    std_area, price_vnd, std_date, orig_list_time,
                    domain, last_script, process_status
                )
                SELECT
                    CONCAT('nhadat_', n.realestate_id),
                    n.city_id,
                    n.ward_id,
                    n.street_id,
                    NULLIF(n.project_id, 0),
                    CASE WHEN n.area IS NULL OR TRIM(CAST(n.area AS CHAR)) = '' THEN NULL ELSE CAST(n.area AS CHAR) END,
                    CASE WHEN n.area_unit IS NULL OR TRIM(CAST(n.area_unit AS CHAR)) = '' THEN NULL ELSE CAST(n.area_unit AS CHAR) END,
                    CASE
                      WHEN COALESCE({price_expr}, n.price) IS NULL THEN NULL
                      ELSE CAST(COALESCE({price_expr}, n.price) AS CHAR)
                    END,
                    CASE WHEN n.category_id IS NULL THEN NULL ELSE CAST(n.category_id AS CHAR) END,
                    CASE WHEN n.trade_type IS NULL THEN NULL ELSE CAST(n.trade_type AS CHAR) END,
                    NULL,
                    COALESCE({price_expr}, n.price),
                    NULL,
                    CAST(DATE_FORMAT(STR_TO_DATE(n.orig_list_time, '%%d/%%m/%%Y'), '%%Y%%m%%d') AS UNSIGNED),
                    'nhadat',
                    'auto_convert_recent15_and_steps.py',
                    0
                FROM nhadat_data n
                WHERE n.realestate_id IN ({ph})
                """,
                ids,
            )
            stats["inserted"] += cur.rowcount

            cur.execute(
                f"""
                UPDATE data_clean_v1 d
                JOIN nhadat_data n
                  ON d.ad_id = CONCAT('nhadat_', n.realestate_id)
                 AND d.domain = 'nhadat'
                SET
                    d.cf_province_id = n.city_id,
                    d.cf_ward_id = n.ward_id,
                    d.cf_street_id = n.street_id,
                    d.project_id = NULLIF(n.project_id, 0),
                    d.src_size = CASE
                        WHEN n.area IS NULL OR TRIM(CAST(n.area AS CHAR)) = '' THEN NULL
                        ELSE CAST(n.area AS CHAR)
                    END,
                    d.unit = CASE
                        WHEN n.area_unit IS NULL OR TRIM(CAST(n.area_unit AS CHAR)) = '' THEN NULL
                        ELSE CAST(n.area_unit AS CHAR)
                    END,
                    d.src_price = CASE
                        WHEN COALESCE({price_expr}, n.price) IS NULL THEN NULL
                        ELSE CAST(COALESCE({price_expr}, n.price) AS CHAR)
                    END,
                    d.src_category_id = CASE WHEN n.category_id IS NULL THEN NULL ELSE CAST(n.category_id AS CHAR) END,
                    d.src_type = CASE WHEN n.trade_type IS NULL THEN NULL ELSE CAST(n.trade_type AS CHAR) END,
                    d.price_vnd = COALESCE({price_expr}, n.price),
                    d.orig_list_time = COALESCE(
                        CAST(DATE_FORMAT(STR_TO_DATE(n.orig_list_time, '%%d/%%m/%%Y'), '%%Y%%m%%d') AS UNSIGNED),
                        d.orig_list_time
                    ),
                    d.last_script = 'auto_convert_recent15_and_steps.py'
                WHERE n.realestate_id IN ({ph})
                """,
                ids,
            )
            stats["updated"] += cur.rowcount

            cur.execute(
                f"""
                UPDATE nhadat_data
                SET converted = 1
                WHERE realestate_id IN ({ph})
                  AND COALESCE(converted,0)=0
                """,
                ids,
            )
            stats["marked"] += cur.rowcount
            conn.commit()

            print(
                f"[NHADAT][Batch {stats['batches']}] selected={len(ids)} "
                f"inserted={stats['inserted']} updated={stats['updated']} marked={stats['marked']}"
            )

    return stats


def run_steps(domain_key: str) -> None:
    steps = PIPELINES[domain_key]
    for step in steps:
        step_parts = shlex.split(step)
        script_path = ROOT / step_parts[0]
        cmd = [sys.executable, "-u", str(script_path), *step_parts[1:]]
        print(f"[{domain_key.upper()}] RUN: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, cwd=str(ROOT))


def get_backlog_counts(conn, domains: List[str]) -> Dict[str, int]:
    """Count rows that still need normalization steps (process_status < 6) per domain."""
    out: Dict[str, int] = {d: 0 for d in domains}
    with conn.cursor() as cur:
        for d in domains:
            domain_val = DOMAIN_VALUE_MAP[d]
            cur.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM data_clean_v1
                WHERE domain = %s
                  AND COALESCE(process_status, 0) < 6
                """,
                (domain_val,),
            )
            row = cur.fetchone()
            out[d] = int(row["cnt"] if row and "cnt" in row else 0)
    return out


def run_once(days: int, batch_size: int, domains: List[str], skip_steps: bool, run_steps_even_if_no_new: bool) -> None:
    cutoff = datetime.now() - timedelta(days=days)
    print(f"=== AUTO CONVERT RECENT + STEPS ===")
    print(f"Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M:%S')} ({days} days)")
    print(f"Domains: {', '.join(domains)}")

    with get_conn() as conn:
        summary: Dict[str, Dict[str, int]] = {}
        failed_domains: Dict[str, str] = {}
        for d in domains:
            try:
                if d == "batdongsan":
                    summary[d] = convert_batdongsan_recent(conn, cutoff, batch_size)
                elif d == "mogi":
                    summary[d] = convert_mogi_recent(conn, cutoff, batch_size)
                elif d == "nhatot":
                    summary[d] = convert_nhatot_recent(conn, cutoff, batch_size)
                elif d == "nhadat":
                    summary[d] = convert_nhadat_recent(conn, cutoff, batch_size)
            except pymysql.err.OperationalError as e:
                conn.rollback()
                msg = str(e)
                failed_domains[d] = msg
                summary[d] = {"batches": 0, "selected": 0, "inserted": 0, "updated": 0, "marked": 0}
                if e.args and e.args[0] == 1206:
                    print(f"[{d.upper()}] convert failed (1206 lock-table overflow), skip this domain in current cycle.")
                else:
                    print(f"[{d.upper()}] convert failed: {msg}")
            except Exception as e:
                conn.rollback()
                msg = str(e)
                failed_domains[d] = msg
                summary[d] = {"batches": 0, "selected": 0, "inserted": 0, "updated": 0, "marked": 0}
                print(f"[{d.upper()}] convert failed: {msg}")

        backlog_counts = get_backlog_counts(conn, domains)

        print("\n=== CONVERT SUMMARY ===")
        for d in domains:
            s = summary[d]
            print(f"{d}: {s}")
        if failed_domains:
            print("\n=== CONVERT FAILURES ===")
            for d, m in failed_domains.items():
                print(f"{d}: {m}")
        print("\n=== BACKLOG (<6) ===")
        for d in domains:
            print(f"{d}: {backlog_counts[d]}")

    if skip_steps:
        print("Skip step pipelines (--skip-steps). Done.")
        return

    for d in domains:
        if d in failed_domains:
            print(f"[{d.upper()}] Skip step pipeline due to convert failure in current cycle.")
            continue
        marked = summary[d].get("marked", 0)
        backlog = backlog_counts.get(d, 0)
        if marked > 0 or backlog > 0 or run_steps_even_if_no_new:
            try:
                run_steps(d)
            except Exception as e:
                print(f"[{d.upper()}] Step pipeline failed: {e}", file=sys.stderr)
        else:
            print(f"[{d.upper()}] No newly marked rows; skip step pipeline.")

    print("=== DONE ===")


def main():
    parser = argparse.ArgumentParser(
        description="Convert recent (N days) unconverted rows to data_clean_v1 and run domain steps 1-7."
    )
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Recent window in days (default 15)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help="Batch size (default 5000)")
    parser.add_argument(
        "--domains",
        default="batdongsan,mogi,nhatot,nhadat",
        help="Comma list: batdongsan,mogi,nhatot,nhadat (default all)",
    )
    parser.add_argument(
        "--skip-steps",
        action="store_true",
        help="Only convert + set flags; do not run Step 1-7.",
    )
    parser.add_argument(
        "--run-steps-even-if-no-new",
        action="store_true",
        help="Run step pipeline even if no new rows were marked converted.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously with sleep interval between cycles.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=300,
        help="Sleep seconds between loop cycles when --loop is enabled (default 300).",
    )
    args = parser.parse_args()

    allowed = {"batdongsan", "mogi", "nhatot", "nhadat"}
    domains = [d.strip().lower() for d in args.domains.split(",") if d.strip()]
    domains = [d for d in domains if d in allowed]
    if not domains:
        raise SystemExit("No valid domains selected. Use --domains batdongsan,mogi,nhatot,nhadat")

    if not args.loop:
        run_once(
            days=args.days,
            batch_size=args.batch_size,
            domains=domains,
            skip_steps=args.skip_steps,
            run_steps_even_if_no_new=args.run_steps_even_if_no_new,
        )
        return

    cycle = 0
    print(f"Loop mode enabled. Interval: {args.interval_seconds}s")
    while True:
        cycle += 1
        started = datetime.now()
        print(f"\n===== CYCLE {cycle} START {started.strftime('%Y-%m-%d %H:%M:%S')} =====")
        try:
            run_once(
                days=args.days,
                batch_size=args.batch_size,
                domains=domains,
                skip_steps=args.skip_steps,
                run_steps_even_if_no_new=args.run_steps_even_if_no_new,
            )
        except Exception as e:
            print(f"[ERROR] Cycle {cycle} failed: {e}", file=sys.stderr)
        ended = datetime.now()
        print(f"===== CYCLE {cycle} END {ended.strftime('%Y-%m-%d %H:%M:%S')} =====")
        time.sleep(max(1, int(args.interval_seconds)))


if __name__ == "__main__":
    main()
