#!/usr/bin/env python3
import argparse
import os
import time
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database


RETRYABLE_DB_ERROR_CODES = {1205, 1206, 1213, 2006, 2013}


def is_retryable_db_error(exc):
    code = None
    if getattr(exc, "args", None):
        try:
            code = int(exc.args[0])
        except (TypeError, ValueError, IndexError):
            code = None
    return code in RETRYABLE_DB_ERROR_CODES


def run_db_with_retry(conn, fn, op_name, max_attempts=5, base_sleep=1.0):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if not is_retryable_db_error(exc) or attempt >= max_attempts:
                raise
            last_exc = exc
            sleep_s = base_sleep * attempt
            print(
                f"[RETRY] {op_name} attempt={attempt}/{max_attempts} "
                f"code={getattr(exc, 'args', ['?'])[0]} sleep={sleep_s:.1f}s"
            )
            time.sleep(sleep_s)
    raise last_exc


def ensure_source_schema(conn):
    statements = [
        "ALTER TABLE ad_listing_detail ADD COLUMN data_no_full_converted TINYINT(1) NOT NULL DEFAULT 0",
        "ALTER TABLE ad_listing_detail ADD COLUMN data_no_full_converted_at DATETIME NULL",
        "ALTER TABLE ad_listing_detail ADD COLUMN data_no_full_skip_reason VARCHAR(32) NULL",
        "CREATE INDEX idx_ad_data_no_full_converted ON ad_listing_detail(data_no_full_converted)",
    ]
    def _run():
        with conn.cursor() as cur:
            cur.execute("SHOW COLUMNS FROM ad_listing_detail LIKE 'data_no_full_converted'")
            if not cur.fetchone():
                cur.execute(statements[0])
            cur.execute("SHOW COLUMNS FROM ad_listing_detail LIKE 'data_no_full_converted_at'")
            if not cur.fetchone():
                cur.execute(statements[1])
            cur.execute("SHOW COLUMNS FROM ad_listing_detail LIKE 'data_no_full_skip_reason'")
            if not cur.fetchone():
                cur.execute(statements[2])
            cur.execute("SHOW INDEX FROM ad_listing_detail WHERE Key_name='idx_ad_data_no_full_converted'")
            if not cur.fetchone():
                cur.execute(statements[3])
        conn.commit()

    run_db_with_retry(conn, _run, "ensure_source_schema")


def legal_mapping(category, doc):
    if doc is None:
        return (None, None)
    try:
        category = int(category) if category is not None else None
        doc = int(doc)
    except (TypeError, ValueError):
        return (None, None)

    if doc == 1:
        return ("Sổ hồng", 1)
    if doc == 2:
        return ("Đang hợp thức hóa", 5)
    if doc == 3 and category == 1030:
        return ("Giấy tờ hợp lệ", 4)
    if doc == 4:
        if category == 1010:
            return ("Hợp đồng", 7)
        if category in (1020, 1040):
            return ("Không xác định", 8)
    if doc == 5:
        if category == 1010:
            return ("Hợp đồng", 7)
        if category in (1020, 1040):
            return ("Giấy tờ hợp lệ", 4)
    if doc == 6:
        if category == 1010:
            return ("Sổ hồng", 1)
        if category in (1020, 1040):
            return ("Giấy tay", 3)
    return (None, None)


def type_mapping(row):
    category = row["category"]
    listing_type = row["type"]
    house_type = row["house_type"]
    apartment_type = row["apartment_type"]
    land_type = row["land_type"]
    commercial_type = row["commercial_type"]

    try:
        category = int(category) if category is not None else None
        house_type = int(house_type) if house_type is not None else None
        apartment_type = int(apartment_type) if apartment_type is not None else None
        land_type = int(land_type) if land_type is not None else None
        commercial_type = int(commercial_type) if commercial_type is not None else None
    except (TypeError, ValueError):
        return (None, None, None)

    if category == 1040:
        cat_id = 2
        if land_type == 1:
            return (cat_id, 11, "Bán đất thổ cư")
        if land_type == 2:
            return (cat_id, 8, "Bán đất nền dự án")
        if land_type in (3, 4):
            return (cat_id, 10, "Bán đất nông, lâm nghiệp")
        return (None, None, None)

    if listing_type == "s":
        cat_id = 1
        if category == 1020:
            if house_type in (1, 2):
                return (cat_id, 2, "Bán nhà riêng")
            if house_type == 3:
                return (cat_id, 3, "Biệt thự")
            if house_type == 4:
                return (cat_id, 1, "Bán nhà phố dự án")
        if category == 1010:
            if apartment_type == 2:
                return (cat_id, 56, "Bán căn hộ Mini, Dịch vụ")
            if apartment_type in (1, 3, 4, 5, 6):
                return (cat_id, 5, "Bán căn hộ chung cư")
        if category == 1030:
            if commercial_type == 1:
                return (cat_id, 13, "Nhà hàng - Khách sạn")
            if commercial_type == 2:
                return (cat_id, 14, "Nhà Kho - Xưởng")
            if commercial_type in (3, 4):
                return (cat_id, 13, "Nhà hàng - Khách sạn")
            return (None, None, None)
        return (None, None, None)

    if listing_type == "u":
        cat_id = 3
        if category == 1020:
            if house_type in (1, 4):
                return (cat_id, 1, "Nhà phố")
            if house_type == 2:
                return (cat_id, 2, "Nhà riêng")
            if house_type == 3:
                return (cat_id, 3, "Biệt thự")
        if category == 1010:
            if apartment_type in (1, 2, 3, 4, 5, 6):
                return (cat_id, 5, "Căn hộ chung cư")
        if category == 1030:
            if commercial_type == 4:
                return (cat_id, 6, "Văn phòng")
            if commercial_type == 3:
                return (cat_id, 12, "Mặt bằng")
            if commercial_type == 1:
                return (cat_id, 13, "Nhà hàng - Khách sạn")
            if commercial_type == 2:
                return (cat_id, 14, "Nhà Kho - Xưởng")
        if category == 1050:
            return (cat_id, 15, "Phòng trọ")
        return (None, None, None)

    return (None, None, None)


def get_candidate_ids(conn, batch_size):
    sql = """
        SELECT d.list_id
        FROM ad_listing_detail d
        WHERE COALESCE(d.data_no_full_converted,0) = 0
          AND d.list_id IS NOT NULL
          AND d.subject IS NOT NULL AND d.subject <> ''
          AND d.body IS NOT NULL AND d.body <> ''
          AND d.price IS NOT NULL
          AND d.size IS NOT NULL
        ORDER BY d.list_time DESC, d.list_id DESC
        LIMIT %s
    """
    def _run():
        with conn.cursor() as cur:
            cur.execute(sql, (batch_size,))
            rows = cur.fetchall()
            return [str((r["list_id"] if isinstance(r, dict) else r[0])) for r in rows]

    return run_db_with_retry(conn, _run, "get_candidate_ids")


def get_candidates(conn, source_post_ids):
    if not source_post_ids:
        return []
    placeholders = ",".join(["%s"] * len(source_post_ids))
    sql = f"""
        SELECT
            d.list_id AS source_post_id,
            d.ad_id AS id_img,
            d.subject AS title,
            d.address,
            FROM_UNIXTIME(COALESCE(d.orig_list_time, d.list_time) / 1000) AS posted_at,
            NULL AS img,
            d.price,
            d.size AS area,
            d.body AS description,
            d.type,
            d.direction AS house_direction,
            d.floors,
            d.toilets AS bathrooms,
            NULL AS road_width,
            NULL AS living_rooms,
            d.rooms AS bedrooms,
            d.latitude AS lat,
            d.longitude AS lng,
            NULL AS broker_name,
            NULL AS phone,
            'nhatot' AS source,
            NOW() AS time_converted_at,
            d.width,
            d.length,
            NULL AS city,
            NULL AS district,
            NULL AS ward,
            d.street_name AS street,
            tcm_province.new_city_id AS province_id,
            NULL AS district_id,
            tcm_ward.new_city_id AS ward_id,
            NULL AS street_id,
            d.pty_project_name AS project_name,
            NULL AS slug_name,
            'PENDING' AS images_status,
            d.property_legal_document,
            d.category,
            d.house_type,
            d.apartment_type,
            d.land_type,
            d.commercial_type
        FROM ad_listing_detail d
        LEFT JOIN location_detail ld_province
            ON ld_province.level = 1
           AND ld_province.region_id = d.region_v2
        LEFT JOIN transaction_city_merge tcm_province
            ON tcm_province.old_city_id = ld_province.cafeland_id
        LEFT JOIN location_detail ld_ward
            ON ld_ward.level = 3
           AND ld_ward.region_id = d.region_v2
           AND ld_ward.area_id = d.area_v2
           AND ld_ward.ward_id = d.ward
        LEFT JOIN transaction_city_merge tcm_ward
            ON tcm_ward.old_city_id = ld_ward.cafeland_id
        WHERE d.list_id IN ({placeholders})
        ORDER BY d.list_time DESC, d.list_id DESC
    """
    def _load_rows():
        with conn.cursor() as cur:
            cur.execute(sql, source_post_ids)
            return cur.fetchall()

    rows = run_db_with_retry(conn, _load_rows, "get_candidates")

    # Resolve first image per ad_id with indexed point lookups instead of a full derived scan
    detail_ids = [str(r["id_img"]) for r in rows if r.get("id_img") is not None]
    if detail_ids:
        img_placeholders = ",".join(["%s"] * len(detail_ids))
        img_sql = f"""
            SELECT s1.detail_id, s1.image_url
            FROM scraped_detail_images s1
            INNER JOIN (
                SELECT detail_id, MIN(idx) AS min_idx
                FROM scraped_detail_images
                WHERE detail_id IN ({img_placeholders})
                GROUP BY detail_id
            ) s2
              ON s1.detail_id = s2.detail_id
             AND s1.idx = s2.min_idx
        """
        def _load_images():
            with conn.cursor() as cur:
                cur.execute(img_sql, detail_ids)
                return cur.fetchall()

        image_rows = run_db_with_retry(conn, _load_images, "get_candidate_images")
        image_map = {
            str((img["detail_id"] if isinstance(img, dict) else img[0])): (img["image_url"] if isinstance(img, dict) else img[1])
            for img in image_rows
        }
        for row in rows:
            row["img"] = image_map.get(str(row.get("id_img")))

    return rows


def persist_batch(conn, rows, skipped_rows_by_reason):
    if not rows and not any(skipped_rows_by_reason.values()):
        return 0

    insert_sql = """
        INSERT INTO data_no_full (
            title, address, posted_at, img, price, area, description,
            property_type, type, house_direction, floors, bathrooms, road_width,
            living_rooms, bedrooms, legal_status, lat, `long`, broker_name, phone,
            source, time_converted_at, source_post_id, width, length, city, district,
            ward, street, province_id, district_id, ward_id, street_id, id_img,
            project_name, slug_name, images_status, stratum_id, cat_id, type_id, unit, project_id
        ) VALUES (
            %(title)s, %(address)s, %(posted_at)s, %(img)s, %(price)s, %(area)s, %(description)s,
            %(property_type)s, %(type)s, %(house_direction)s, %(floors)s, %(bathrooms)s, %(road_width)s,
            %(living_rooms)s, %(bedrooms)s, %(legal_status)s, %(lat)s, %(long)s, %(broker_name)s, %(phone)s,
            %(source)s, %(time_converted_at)s, %(source_post_id)s, %(width)s, %(length)s, %(city)s, %(district)s,
            %(ward)s, %(street)s, %(province_id)s, %(district_id)s, %(ward_id)s, %(street_id)s, %(id_img)s,
            %(project_name)s, %(slug_name)s, %(images_status)s, %(stratum_id)s, %(cat_id)s, %(type_id)s, %(unit)s, %(project_id)s
        )
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _run():
        inserted_count = 0
        with conn.cursor() as cur:
            if rows:
                cur.executemany(insert_sql, rows)
                inserted_count = cur.rowcount

            inserted_ad_ids = [str(r["_source_ad_id"]) for r in rows if r.get("_source_ad_id") is not None]
            if inserted_ad_ids:
                placeholders = ",".join(["%s"] * len(inserted_ad_ids))
                cur.execute(
                    f"""
                    UPDATE ad_listing_detail
                    SET data_no_full_converted = 1,
                        data_no_full_converted_at = %s,
                        data_no_full_skip_reason = NULL
                    WHERE ad_id IN ({placeholders})
                    """,
                    [now] + inserted_ad_ids,
                )

            for reason, ad_ids in skipped_rows_by_reason.items():
                if not ad_ids:
                    continue
                placeholders = ",".join(["%s"] * len(ad_ids))
                cur.execute(
                    f"""
                    UPDATE ad_listing_detail
                    SET data_no_full_converted = 1,
                        data_no_full_converted_at = %s,
                        data_no_full_skip_reason = %s
                    WHERE ad_id IN ({placeholders})
                    """,
                    [now, reason] + [str(x) for x in ad_ids],
                )
        conn.commit()
        return inserted_count

    return run_db_with_retry(conn, _run, "persist_batch")


def build_insertable_rows(raw_rows):
    insert_rows = []
    skipped = {
        "missing_type_map": 0,
        "missing_region_merge": 0,
        "missing_image": 0,
    }
    skipped_ids = {
        "skip_type": [],
        "skip_region": [],
        "skip_img": [],
    }

    for row in raw_rows:
        if not row.get("province_id") or not row.get("ward_id"):
            skipped["missing_region_merge"] += 1
            skipped_ids["skip_region"].append(str(row.get("id_img")))
            continue

        cat_id, type_id, property_type = type_mapping(row)
        if not cat_id or not type_id or not property_type:
            skipped["missing_type_map"] += 1
            skipped_ids["skip_type"].append(str(row.get("id_img")))
            continue

        legal_status, stratum_id = legal_mapping(row.get("category"), row.get("property_legal_document"))
        unit = "tháng" if row.get("type") == "u" else "m2"

        insert_rows.append({
            "title": row.get("title"),
            "address": row.get("address"),
            "posted_at": row.get("posted_at"),
            "img": row.get("img"),
            "price": row.get("price"),
            "area": row.get("area"),
            "description": row.get("description"),
            "property_type": property_type,
            "type": row.get("type"),
            "house_direction": row.get("house_direction"),
            "floors": row.get("floors"),
            "bathrooms": row.get("bathrooms"),
            "road_width": row.get("road_width"),
            "living_rooms": row.get("living_rooms"),
            "bedrooms": row.get("bedrooms"),
            "legal_status": legal_status,
            "lat": row.get("lat"),
            "long": row.get("lng"),
            "broker_name": row.get("broker_name"),
            "phone": row.get("phone"),
            "source": row.get("source"),
            "time_converted_at": row.get("time_converted_at"),
            "source_post_id": str(row.get("source_post_id")),
            "width": row.get("width"),
            "length": row.get("length"),
            "city": row.get("city"),
            "district": row.get("district"),
            "ward": row.get("ward"),
            "street": row.get("street"),
            "province_id": row.get("province_id"),
            "district_id": None,
            "ward_id": row.get("ward_id"),
            "street_id": None,
            "id_img": row.get("id_img"),
            "project_name": row.get("project_name") or None,
            "slug_name": None,
            "images_status": "PENDING",
            "stratum_id": stratum_id,
            "cat_id": cat_id,
            "type_id": type_id,
            "unit": unit,
            "project_id": 0,
            "_source_ad_id": row.get("id_img"),
        })

    return insert_rows, skipped, skipped_ids


def run(args):
    db = Database()
    conn = db.get_connection()
    try:
        ensure_source_schema(conn)
        batch_no = 0
        while True:
            batch_no += 1
            candidate_ids = get_candidate_ids(conn, args.batch_size)
            raw_rows = get_candidates(conn, candidate_ids)
            selected = len(raw_rows)
            if selected == 0:
                print(f"[BATCH {batch_no}] selected=0 inserted=0 skip_type=0 skip_region=0 skip_img=0")
                break

            rows_to_insert, skipped, skipped_ids = build_insertable_rows(raw_rows)
            inserted = persist_batch(conn, rows_to_insert, skipped_ids)

            print(
                f"[BATCH {batch_no}] selected={selected} inserted={inserted} "
                f"skip_type={skipped['missing_type_map']} "
                f"skip_region={skipped['missing_region_merge']} "
                f"skip_img={skipped['missing_image']}"
            )

            if not args.loop_until_done:
                break
            if selected == 0:
                break
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Convert Nhatot raw rows to data_no_full in batches")
    parser.add_argument("--batch-size", type=int, default=100, help="Rows to scan per batch")
    parser.add_argument("--loop-until-done", action="store_true", help="Keep batching until no candidates remain")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
