import argparse
import time

import pymysql

DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "craw_db"
DOMAIN = "batdongsan.com.vn"
SCRIPT_NAME = "recreate_datacleanv1_batdongsan.py"
BATCH_SIZE = 5000


def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def ensure_source_tracking(cursor):
    cursor.execute("SHOW COLUMNS FROM scraped_details_flat LIKE 'cleanv1_converted'")
    if not cursor.fetchone():
        cursor.execute(
            "ALTER TABLE scraped_details_flat "
            "ADD COLUMN cleanv1_converted TINYINT(1) NOT NULL DEFAULT 0"
        )
    cursor.execute("SHOW COLUMNS FROM scraped_details_flat LIKE 'cleanv1_converted_at'")
    if not cursor.fetchone():
        cursor.execute(
            "ALTER TABLE scraped_details_flat "
            "ADD COLUMN cleanv1_converted_at DATETIME NULL"
        )

    cursor.execute("SHOW INDEX FROM scraped_details_flat WHERE Key_name='idx_sdf_domain_conv'")
    if not cursor.fetchone():
        cursor.execute(
            "ALTER TABLE scraped_details_flat "
            "ADD INDEX idx_sdf_domain_conv (domain, cleanv1_converted)"
        )


def insert_new_rows(cursor, limit):
    sql = f"""
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
            LEFT(REPLACE(REPLACE(s.dientich, '\n', ''), '\r', ''), 50),
            LEFT(REPLACE(REPLACE(s.khoanggia, '\n', ''), '\r', ''), 50),
            LEFT(TRIM(s.loaihinh), 50),
            LEFT(TRIM(s.trade_type), 50),
            CAST(s.ngaydang AS UNSIGNED),
            UNIX_TIMESTAMP(s.created_at),
            s.url,
            %s,
            0,
            %s
        FROM scraped_details_flat s
        WHERE s.domain = %s
          AND COALESCE(s.cleanv1_converted, 0) = 0
          AND s.matin IS NOT NULL AND TRIM(s.matin) <> ''
        LIMIT {limit}
    """
    cursor.execute(sql, (DOMAIN, SCRIPT_NAME, DOMAIN))
    return cursor.rowcount


def update_existing_rows(cursor, limit):
    sql = f"""
        UPDATE data_clean_v1 d
        JOIN scraped_details_flat s
          ON d.ad_id = s.matin
         AND s.domain = %s
        SET
            d.src_province_id = CAST(s.city_code AS CHAR),
            d.src_district_id = CAST(s.district_id AS CHAR),
            d.src_ward_id = CAST(s.ward_id AS CHAR),
            d.src_size = LEFT(REPLACE(REPLACE(s.dientich, '\n', ''), '\r', ''), 50),
            d.src_price = LEFT(REPLACE(REPLACE(s.khoanggia, '\n', ''), '\r', ''), 50),
            d.src_category_id = LEFT(TRIM(s.loaihinh), 50),
            d.src_type = LEFT(TRIM(s.trade_type), 50),
            d.orig_list_time = CAST(s.ngaydang AS UNSIGNED),
            d.update_time = UNIX_TIMESTAMP(s.created_at),
            d.url = s.url,
            d.domain = %s,
            d.last_script = %s
        WHERE d.domain = %s
          AND COALESCE(s.cleanv1_converted, 0) = 0
        LIMIT {limit}
    """
    cursor.execute(sql, (DOMAIN, DOMAIN, SCRIPT_NAME, DOMAIN))
    return cursor.rowcount


def mark_source_converted(cursor, limit):
    sql = f"""
        UPDATE scraped_details_flat s
        JOIN data_clean_v1 d
          ON d.ad_id = s.matin
         AND d.domain = %s
        SET
            s.cleanv1_converted = 1,
            s.cleanv1_converted_at = NOW()
        WHERE s.domain = %s
          AND COALESCE(s.cleanv1_converted, 0) = 0
          AND s.matin IS NOT NULL AND TRIM(s.matin) <> ''
        LIMIT {limit}
    """
    cursor.execute(sql, (DOMAIN, DOMAIN))
    return cursor.rowcount


def run(loop: bool, limit: int, sleep_s: float):
    print(f"=== STEP 0 CONVERT -> data_clean_v1 ({DOMAIN}) ===")
    print("Mapping:")
    print("  ad_id            <- scraped_details_flat.matin")
    print("  src_province_id  <- scraped_details_flat.city_code")
    print("  src_district_id  <- scraped_details_flat.district_id")
    print("  src_ward_id      <- scraped_details_flat.ward_id")
    print("  src_size         <- scraped_details_flat.dientich")
    print("  src_price        <- scraped_details_flat.khoanggia")
    print("  src_category_id  <- scraped_details_flat.loaihinh")
    print("  src_type         <- scraped_details_flat.trade_type")
    print("  orig_list_time   <- CAST(scraped_details_flat.ngaydang AS UNSIGNED)")
    print("  update_time      <- UNIX_TIMESTAMP(scraped_details_flat.created_at)")

    conn = get_conn()
    cursor = conn.cursor()
    ensure_source_tracking(cursor)
    conn.commit()

    batch = 0
    total_insert = 0
    total_update = 0
    total_mark = 0

    while True:
        batch += 1
        inserted = insert_new_rows(cursor, limit)
        updated = update_existing_rows(cursor, limit)
        marked = mark_source_converted(cursor, limit)
        conn.commit()

        total_insert += inserted
        total_update += updated
        total_mark += marked

        print(
            f"[Batch {batch}] inserted={inserted}, updated={updated}, "
            f"marked_converted={marked} | totals: "
            f"ins={total_insert}, upd={total_update}, mark={total_mark}"
        )

        if not loop or marked < limit:
            break
        if sleep_s > 0:
            time.sleep(sleep_s)

    cursor.execute(
        """
        SELECT COUNT(*) AS pending
        FROM scraped_details_flat
        WHERE domain=%s AND COALESCE(cleanv1_converted,0)=0
        """,
        (DOMAIN,),
    )
    pending = (cursor.fetchone() or {}).get("pending", 0)
    print(f"Done. Pending source rows (cleanv1_converted=0): {pending}")
    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Convert Step 0 Batdongsan -> data_clean_v1")
    parser.add_argument("--limit", type=int, default=BATCH_SIZE, help="Batch size per loop")
    parser.add_argument("--loop", action="store_true", help="Run until no pending source rows")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between loop batches")
    args = parser.parse_args()

    run(loop=args.loop, limit=args.limit, sleep_s=args.sleep)


if __name__ == "__main__":
    main()
