#!/usr/bin/env python3
"""
Listing Uploader - Upload tin đăng lên cafeland API
Chạy song song với ftp_image_processor.py

Workflow:
1. Lấy tin đăng có images_status = 'IMAGES_READY'
2. Lấy tất cả ftp_path của ảnh thuộc tin đó
3. POST lên API cafeland
4. Update images_status = 'LISTING_UPLOADED'
"""

import sys
import os
import time
import json
import argparse
import logging
import html
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import Database

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# API Config
API_URL = "https://api.cafeland.vn/api/website-service/api/app-sync/reals/post-data/"
STATIC_BASE_URL = "https://static2.cafeland.vn"
NULL_CONTACT_API_URL = "https://api.cafeland.vn/api/website-service/api/app-sync/reals/post-data/null-contact/"
ALLOWED_TABLES = {"data_full", "data_no_full"}
AREA_FILTER_TABLE = "upload_area_lt20"
_LOCATION_NAME_CACHE = None
RETRYABLE_DB_ERROR_CODES = {1205, 1206, 1213, 2006, 2013}


def _validate_table_name(table_name: str) -> str:
    table_name = (table_name or "data_full").strip()
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported table: {table_name}")
    return table_name


def get_api_url(api_mode: str) -> str:
    return NULL_CONTACT_API_URL if api_mode == "null-contact" else API_URL


def is_retryable_db_error(exc) -> bool:
    code = None
    if getattr(exc, "args", None):
        try:
            code = int(exc.args[0])
        except (TypeError, ValueError, IndexError):
            code = None
    return code in RETRYABLE_DB_ERROR_CODES


def run_db_with_retry(fn, op_name: str, max_attempts: int = 5, base_sleep: float = 1.0):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if not is_retryable_db_error(exc) or attempt >= max_attempts:
                raise
            last_exc = exc
            sleep_s = base_sleep * attempt
            logger.warning(
                "[DB-RETRY] %s attempt=%s/%s code=%s sleep=%.1fs",
                op_name,
                attempt,
                max_attempts,
                getattr(exc, "args", ["?"])[0],
                sleep_s,
            )
            time.sleep(sleep_s)
    raise last_exc


def load_location_name_cache():
    global _LOCATION_NAME_CACHE
    if _LOCATION_NAME_CACHE is not None:
        return _LOCATION_NAME_CACHE

    province_map = {}
    ward_map = {}
    def _run():
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT new_city_id, new_city_name, new_city_parent_id
                FROM transaction_city_merge
                """
            )
            for row in cursor.fetchall():
                cid = row.get("new_city_id")
                name = (row.get("new_city_name") or "").strip()
                parent_id = row.get("new_city_parent_id")
                if not cid or not name:
                    continue
                if int(parent_id or 0) == 0:
                    province_map[int(cid)] = name
                else:
                    ward_map[int(cid)] = name
        finally:
            cursor.close()
            conn.close()

    run_db_with_retry(_run, "load_location_name_cache")

    _LOCATION_NAME_CACHE = (province_map, ward_map)
    return _LOCATION_NAME_CACHE


def resolve_listing_location_parts(listing: dict):
    province_map, ward_map = load_location_name_cache()
    street = (listing.get('street') or '').strip()
    if street.isdigit():
        street = ''

    ward_name = (listing.get('ward') or '').strip()
    if not ward_name:
        ward_name = ward_map.get(int(listing.get('ward_id') or 0), '')

    city_name = (listing.get('city') or '').strip()
    if not city_name:
        city_name = province_map.get(int(listing.get('province_id') or 0), '')

    return street, ward_name, city_name


def infer_listing_type(listing: dict) -> str:
    raw = (listing.get('listing_type') or '').strip().lower()
    if raw in ('s', 'ban', 'mua', 'cần bán', 'can ban'):
        return 's'
    if raw in ('u', 'thue', 'thuê', 'cho thuê', 'cho thue'):
        return 'u'

    unit = (listing.get('unit') or '').strip().lower()
    if unit in ('tháng', 'thang'):
        return 'u'

    cat_id = int(listing.get('cat_id') or 0)
    if cat_id == 3:
        return 'u'
    return 's'


def clean_detail_text(value: str) -> str:
    text = value or ''
    text = text.replace('<br>', '\n').replace('<br/>', '\n').replace('<br />', '\n')
    text = re.sub(r'</?(div|p|span|strong|b|i|u)[^>]*>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html.unescape(text)
    text = text.replace('\r', '\n')
    text = re.sub(r'\n\s*\n+', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def ensure_area_filter_schema(db: Database):
    """Create/refresh whitelist table for uploadable province/ward pairs."""
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {AREA_FILTER_TABLE} (
                province_id SMALLINT UNSIGNED NOT NULL,
                ward_id SMALLINT UNSIGNED NOT NULL,
                province_name VARCHAR(128) NULL,
                ward_name VARCHAR(128) NULL,
                data_full_count INT NOT NULL DEFAULT 0,
                nhadat_count INT NOT NULL DEFAULT 0,
                total_count INT NOT NULL DEFAULT 0,
                PRIMARY KEY (province_id, ward_id),
                KEY idx_total_count (total_count)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def refresh_area_filter_table(db: Database):
    """Populate whitelist of province/ward pairs where data_full + nhadat_data < 20."""
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {AREA_FILTER_TABLE}")
        cursor.execute(f"""
            INSERT INTO {AREA_FILTER_TABLE}
                (province_id, ward_id, province_name, ward_name, data_full_count, nhadat_count, total_count)
            WITH province_names AS (
                SELECT new_city_id AS province_id, MIN(new_city_name) AS province_name
                FROM transaction_city_merge
                WHERE new_city_parent_id = 0
                GROUP BY new_city_id
            ),
            all_wards AS (
                SELECT new_city_parent_id AS province_id, new_city_id AS ward_id, MIN(new_city_name) AS ward_name
                FROM transaction_city_merge
                WHERE new_city_parent_id <> 0
                GROUP BY new_city_parent_id, new_city_id
            ),
            df AS (
                SELECT province_id, ward_id, COUNT(*) AS data_full_count
                FROM data_full
                GROUP BY province_id, ward_id
            ),
            nd AS (
                SELECT city_id AS province_id, ward_id, COUNT(*) AS nhadat_count
                FROM nhadat_data
                GROUP BY city_id, ward_id
            )
            SELECT
                aw.province_id,
                aw.ward_id,
                pn.province_name,
                aw.ward_name,
                COALESCE(df.data_full_count, 0) AS data_full_count,
                COALESCE(nd.nhadat_count, 0) AS nhadat_count,
                COALESCE(df.data_full_count, 0) + COALESCE(nd.nhadat_count, 0) AS total_count
            FROM all_wards aw
            LEFT JOIN province_names pn
                ON pn.province_id = aw.province_id
            LEFT JOIN df
                ON df.province_id = aw.province_id
               AND df.ward_id = aw.ward_id
            LEFT JOIN nd
                ON nd.province_id = aw.province_id
               AND nd.ward_id = aw.ward_id
            WHERE COALESCE(df.data_full_count, 0) + COALESCE(nd.nhadat_count, 0) < 20
        """)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def ensure_uploaded_at_schema(db: Database, table_name: str = "data_full"):
    """Ensure target table has uploaded_at column for daily upload stats."""
    table_name = _validate_table_name(table_name)
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'uploaded_at'")
        if not cursor.fetchone():
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN uploaded_at DATETIME NULL DEFAULT NULL")
            conn.commit()

        cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name='idx_data_full_uploaded_at'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE INDEX idx_data_full_uploaded_at ON {table_name} (uploaded_at)")
            conn.commit()
    finally:
        cursor.close()
        conn.close()


def normalize_house_direction(value):
    """Pass through Cafeland-compatible direction enum 1..8, else None.

    Current agreed rule:
    1=Đông, 2=Tây, 3=Nam, 4=Bắc, 5=Đông Bắc, 6=Đông Nam, 7=Tây Bắc, 8=Tây Nam
    """
    if value is None:
        return None
    try:
        ivalue = int(float(value))
    except (TypeError, ValueError):
        return None
    return ivalue if 1 <= ivalue <= 8 else None


def get_ready_listings(
    db: Database,
    limit: int = 50,
    dry_run: bool = False,
    order: str = "asc",
    table_name: str = "data_full",
    area_filter: bool = False,
    exclude_province_ids: list[int] | None = None,
    area_filter_table: str | None = None,
    area_filter_max_total: int | None = None,
) -> list:
    """Get listings with images_status = 'IMAGES_READY'.

    Normal mode: atomically marks rows IMAGES_READY -> UPLOADING, then selects UPLOADING.
    Dry-run mode: SELECT only, does not update DB status.
    """
    order_sql = "ASC" if str(order).lower() != "desc" else "DESC"
    table_name = _validate_table_name(table_name)
    exclude_province_ids = [int(x) for x in (exclude_province_ids or [])]
    def _run():
        conn = db.get_connection()
        cursor = conn.cursor()
        try:
            province_filter = ""
            province_filter_params = []
            if exclude_province_ids:
                placeholders = ",".join(["%s"] * len(exclude_province_ids))
                province_filter = f" AND df.province_id NOT IN ({placeholders})"
                province_filter_params.extend(exclude_province_ids)

            custom_area_join = ""
            custom_area_where = ""
            custom_area_params = []
            if area_filter_table:
                custom_area_join = f"""
                LEFT JOIN {area_filter_table} af
                    ON af.province_id = df.province_id
                   AND af.ward_id = df.ward_id
                """
                if area_filter_max_total is not None:
                    custom_area_where = " AND (af.total_count < %s OR df.source = 'vinhome')"
                    custom_area_params.append(int(area_filter_max_total))
                else:
                    custom_area_where = " AND (af.province_id IS NOT NULL OR df.source = 'vinhome')"

            if dry_run:
                where_status = "IMAGES_READY"
            else:
                join_area = custom_area_join or (f"""
                LEFT JOIN {AREA_FILTER_TABLE} af
                    ON af.province_id = df.province_id
                   AND af.ward_id = df.ward_id
                """ if area_filter else "")
                if not custom_area_where and area_filter:
                    area_where = " AND (af.province_id IS NOT NULL OR df.source = 'vinhome')"
                else:
                    area_where = custom_area_where

                cursor.execute(f"""
                    UPDATE {table_name} 
                    INNER JOIN (
                        SELECT df.id
                        FROM {table_name} df
                        {join_area}
                        WHERE df.images_status = 'IMAGES_READY'
                          AND df.uploaded_at IS NULL
                          AND (df.source <> 'vinhome' OR df.ward_id IS NOT NULL)
                          {area_where}
                          {province_filter}
                        ORDER BY df.id {order_sql}
                        LIMIT %s
                    ) picked ON picked.id = {table_name}.id
                    SET {table_name}.images_status = 'UPLOADING'
                """, tuple(custom_area_params + province_filter_params + [limit]))
                affected = cursor.rowcount
                conn.commit()
                if affected == 0:
                    return []
                where_status = "UPLOADING"

            join_area = custom_area_join or (f"""
                LEFT JOIN {AREA_FILTER_TABLE} af
                    ON af.province_id = df.province_id
                   AND af.ward_id = df.ward_id
            """ if area_filter else "")

            cursor.execute(f"""
            SELECT 
                df.id,
                df.title,
                df.description,
                df.price,
                df.area,
                df.address,
                df.street,
                COALESCE(tcm_province.new_city_name, df.city) as city,
                COALESCE(tcm_district.new_city_name, df.district) as district,
                COALESCE(tcm_ward.new_city_name, df.ward) as ward,
                df.lat,
                df.long as lng,
                df.id_img,
                df.slug_name,
                df.type as listing_type,
                df.property_type,
                df.floors,
                df.bedrooms,
                df.bathrooms,
                df.house_direction,
                df.road_width,
                df.living_rooms,
                df.province_id,
                df.district_id,
                df.ward_id,
                df.project_id,
                df.source,
                df.source_post_id,
                df.cat_id,
                df.type_id,
                df.stratum_id,
                df.phone,
                df.broker_name,
                df.width,
                df.length,
                df.unit
            FROM {table_name} df
            {join_area}
            LEFT JOIN transaction_city_merge tcm_province 
                ON df.province_id = tcm_province.new_city_id 
                AND tcm_province.action_type = 0
            LEFT JOIN transaction_city_merge tcm_district 
                ON df.district_id = tcm_district.new_city_id 
                AND tcm_district.action_type = 0
            LEFT JOIN transaction_city_merge tcm_ward 
                ON df.ward_id = tcm_ward.new_city_id 
                AND tcm_ward.action_type = 0
            WHERE df.images_status = '{where_status}'
              AND (df.source <> 'vinhome' OR df.ward_id IS NOT NULL)
              {custom_area_where}
              {province_filter}
            ORDER BY df.id {order_sql}
            LIMIT %s
            """, tuple(custom_area_params + province_filter_params + [limit]))
            return cursor.fetchall()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            cursor.close()
            conn.close()

    return run_db_with_retry(_run, f"get_ready_listings[{table_name}]")

def get_listing_by_id(db: Database, listing_id: int, dry_run: bool = False, table_name: str = "data_full") -> dict | None:
    """Fetch 1 listing by id.

    Normal mode: only allow IMAGES_READY and mark it to UPLOADING (atomic).
    Dry-run mode: SELECT only, does not update DB status.
    """
    table_name = _validate_table_name(table_name)
    def _run():
        conn = db.get_connection()
        cursor = conn.cursor()
        try:
            if dry_run:
                cursor.execute(
                f"""
                SELECT 
                    df.id,
                    df.title,
                    df.description,
                    df.price,
                    df.area,
                    df.address,
                    df.street,
                    COALESCE(tcm_province.new_city_name, df.city) as city,
                    COALESCE(tcm_district.new_city_name, df.district) as district,
                    COALESCE(tcm_ward.new_city_name, df.ward) as ward,
                    df.lat,
                    df.long as lng,
                    df.id_img,
                    df.slug_name,
                    df.type as listing_type,
                    df.property_type,
                    df.floors,
                    df.bedrooms,
                    df.bathrooms,
                    df.house_direction,
                    df.road_width,
                    df.living_rooms,
                    df.province_id,
                    df.district_id,
                    df.ward_id,
                    df.project_id,
                    df.source,
                    df.source_post_id,
                    df.cat_id,
                    df.type_id,
                    df.stratum_id,
                    df.phone,
                    df.broker_name,
                    df.width,
                    df.length,
                    df.unit
                FROM {table_name} df
                LEFT JOIN transaction_city_merge tcm_province 
                    ON df.province_id = tcm_province.new_city_id 
                    AND tcm_province.action_type = 0
                LEFT JOIN transaction_city_merge tcm_district 
                    ON df.district_id = tcm_district.new_city_id 
                    AND tcm_district.action_type = 0
                LEFT JOIN transaction_city_merge tcm_ward 
                    ON df.ward_id = tcm_ward.new_city_id 
                    AND tcm_ward.action_type = 0
                WHERE df.id = %s
                LIMIT 1
                """,
                (listing_id,),
            )
                row = cursor.fetchone()
                return row

            cursor.execute(
            f"""
            UPDATE {table_name}
            SET images_status='UPLOADING'
            WHERE id=%s AND images_status='IMAGES_READY'
            """,
            (listing_id,),
            )
            affected = cursor.rowcount
            conn.commit()
            if affected == 0:
                return None

            cursor.execute(
            f"""
            SELECT 
                df.id,
                df.title,
                df.description,
                df.price,
                df.area,
                df.address,
                df.street,
                COALESCE(tcm_province.new_city_name, df.city) as city,
                COALESCE(tcm_district.new_city_name, df.district) as district,
                COALESCE(tcm_ward.new_city_name, df.ward) as ward,
                df.lat,
                df.long as lng,
                df.id_img,
                df.slug_name,
                df.type as listing_type,
                df.property_type,
                df.floors,
                df.bedrooms,
                df.bathrooms,
                df.house_direction,
                df.road_width,
                df.living_rooms,
                df.province_id,
                df.district_id,
                df.ward_id,
                df.project_id,
                df.source,
                df.source_post_id,
                df.cat_id,
                df.type_id,
                df.stratum_id,
                df.phone,
                df.broker_name,
                df.width,
                df.length,
                df.unit
            FROM {table_name} df
            LEFT JOIN transaction_city_merge tcm_province 
                ON df.province_id = tcm_province.new_city_id 
                AND tcm_province.action_type = 0
            LEFT JOIN transaction_city_merge tcm_district 
                ON df.district_id = tcm_district.new_city_id 
                AND tcm_district.action_type = 0
            LEFT JOIN transaction_city_merge tcm_ward 
                ON df.ward_id = tcm_ward.new_city_id 
                AND tcm_ward.action_type = 0
            WHERE df.id = %s AND df.images_status='UPLOADING'
            LIMIT 1
            """,
            (listing_id,),
            )
            return cursor.fetchone()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            cursor.close()
            conn.close()

    return run_db_with_retry(_run, f"get_listing_by_id[{table_name}]")


def get_listing_images(db: Database, detail_id: int, table_name: str = "data_full") -> list:
    """Get all uploaded image paths for a listing.
    
    Only gets images that belong to this listing_id in the selected table.
    """
    table_name = _validate_table_name(table_name)
    def _run():
        conn = db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
            SELECT DISTINCT sdi.ftp_path 
            FROM scraped_detail_images sdi
            INNER JOIN {table_name} df ON sdi.detail_id = df.id_img
            WHERE sdi.detail_id = %s AND sdi.status = 'UPLOADED' AND sdi.ftp_path IS NOT NULL
            ORDER BY sdi.idx
            """, (detail_id,))
            rows = cursor.fetchall()
            images = []
            for row in rows:
                path = row['ftp_path'] if isinstance(row, dict) else row[0]
                full_url = f"{STATIC_BASE_URL}{path}"
                images.append(full_url)
            return images
        finally:
            cursor.close()
            conn.close()

    return run_db_with_retry(_run, f"get_listing_images[{table_name}]")


def update_listing_status(db: Database, listing_id: int, status: str, error: str = None, table_name: str = "data_full"):
    """Update listing status."""
    table_name = _validate_table_name(table_name)
    def _run():
        conn = db.get_connection()
        cursor = conn.cursor()
        try:
            if status in ("LISTING_UPLOADED", "DUPLICATE_SKIPPED"):
                cursor.execute(
                    f"UPDATE {table_name} SET images_status=%s, uploaded_at=NOW() WHERE id=%s",
                    (status, listing_id),
                )
            else:
                cursor.execute(
                    f"UPDATE {table_name} SET images_status=%s WHERE id=%s",
                    (status, listing_id),
                )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            cursor.close()
            conn.close()

    try:
        run_db_with_retry(_run, f"update_listing_status[{table_name}]")
    except Exception as e:
        logger.error(f"Failed to update listing status: {e}")

def is_duplicate_uploaded(db: Database, listing: dict, table_name: str = "data_full") -> bool:
    """Anti-duplicate: skip if same (source, source_post_id) has already been uploaded."""
    table_name = _validate_table_name(table_name)
    try:
        src = listing.get('source')
        src_post_id = listing.get('source_post_id')
        lid = listing.get('id')
        if not src or not src_post_id:
            return False
    except Exception:
        return False

    def _run():
        conn = db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
            f"""
            SELECT id
            FROM {table_name}
            WHERE source=%s AND source_post_id=%s
              AND images_status='LISTING_UPLOADED'
              AND id <> %s
            LIMIT 1
            """,
            (src, src_post_id, lid),
            )
            return cursor.fetchone() is not None
        finally:
            cursor.close()
            conn.close()

    return run_db_with_retry(_run, f"is_duplicate_uploaded[{table_name}]")

def upload_listing(listing: dict, images: list, dry_run: bool = False, api_mode: str = "normal") -> tuple:
    """Upload a single listing to API.
    
    Returns: (success: bool, listing_id: int, error: str or None)
    """
    from decimal import Decimal
    
    def json_serial(obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    listing_id = listing['id'] if isinstance(listing, dict) else listing[0]
    
    try:
        # --- PREPARE DATA ---
        # Map fields based on thong_tin_dong_bo_du_lieu.docx
        # Map fields based on thong_tin_dong_bo_du_lieu.docx
        
        # 1. Category and Type from DB
        cat_id = listing.get('cat_id') or 1
        type_id = listing.get('type_id') or 1
        # Override upload mapping: cat 3 + type 57 must be sent as type 12.
        if int(cat_id or 0) == 3 and int(type_id or 0) == 57:
            type_id = 12
            
        # 2. Detail Validation (Must be > 50 chars)
        # 2. Construct Detail
        original_detail = clean_detail_text(listing.get('description') or listing.get('mota') or '')
        
        prop_type = listing.get('property_type') or 'Nhà đất'
        street_name, ward_name, city_name = resolve_listing_location_parts(listing)
        location_parts = [part for part in (street_name, ward_name, city_name) if part]
        location_str = ", ".join(location_parts)
        
        price_val = int(listing.get('price') or 0)
        price_fmt = "{:,}".format(price_val).replace(",", ".")
        
        # Đơn vị giá: type 's' (bán) = VND, type 'u' (thuê) = /tháng
        listing_type = infer_listing_type(listing)
        if listing_type == 'u':
            price_unit = '/tháng'
        else:
            price_unit = 'VND'
        
        area_val = listing.get('area') or 0
        
        # Format: Type Location \n Desc \n Price \n Area
        detail = f"{prop_type} {location_str}\n{original_detail}\nGiá : {price_fmt} {price_unit}\nDiện tích: {area_val} m2"
        
        if len(detail) < 50:
             detail += " ." + ("." * (50 - len(detail)))

        # 3. Legal status (stratum_id) - now stored directly in data_full from ETL
        stratum_id = listing.get('stratum_id')

        payload_data = {
             'cat_id': cat_id,
             'type_id': type_id,
             'city_id': listing.get('province_id') or 1,
             'district_id': listing.get('district_id') or 0,
             'wards_id': listing.get('ward_id') or 0,
             'txtOtherWards': None,
             'wards_name': None,
             
             'street_house': 0,
             # Temporary: disable sending street_name to API
             'street_name': None,
             'txtOtherStreet': None,
             'number_house': "",
             
             'project_id': int(listing.get('project_id') or 0),
             'txtProjectOther': None,
             'project_name': None,
             
             'sl_location': (
                 [str(listing.get('lat')), str(listing.get('lng'))]
                 if listing.get('lat') is not None and listing.get('lng') is not None
                 else None
             ),
             
             'title': f"{listing.get('property_type') or ''}: {listing.get('title')}"[:80],
             'detail': detail,
             
             'price': int(listing.get('price')) if listing.get('price') else None,
             'currency': 'vnd',
             'unit_area': listing.get('unit') or 'm2',
             'area_used': listing.get('area') or listing.get('dientich'),
             'area_home_1': float(listing.get('width')) if listing.get('width') else None,
             'area_home_2': float(listing.get('length')) if listing.get('length') else None,
             
             'stratum_id': stratum_id,
             'house_direction': None,
             'road_home': None,
             'way_home': normalize_house_direction(listing.get('house_direction')),
             
             'storey': int(float(listing.get('floors') or 0)), 'sitting_room': 0, 
            'bathroom': int(float(listing.get('bathrooms') or 0)), 
            'rooms': int(float(listing.get('bedrooms') or 0)), 'other_room': 0,
             
             'list_feature_1': None, 'list_feature_2': None, 'list_feature_3': None,
             'list_feature_4': None, 'list_feature_5': None, 'list_feature_6': None,
             'list_feature_7': None, 'list_feature_8': None, 'list_feature_9': None,
             'list_feature_other': None,
             
             'sl_avatar': images[0] if images else "",
             'sl_avatar_360': None,
             # Keep gallery non-empty even for single-image listings.
             'sendupload': images[1:] if len(images) > 1 else (images[:1] if images else []),
             'sendupload_phap_ly': None,
             'show_pic_thumb_phap_ly': 0,
             'video_youtube': "",
             
             'contact_name': "" if api_mode == "null-contact" else (listing.get('broker_name') or "Chính chủ"),
             'contact_mobile': "" if api_mode == "null-contact" else (listing.get('phone') or "0909000000"),
             
             'location_extra1': None, 'location_extra2': None,
             'data_sgd': None, 'phan_tram_chia_se': None, 'thoi_gian_chia_se': None
        }
        
        headers = {
            'token': '$2y$10$0f/Frpwde3r0.th2lxB3Nuq7dGgZUhPMe4aoAC9Toz0how..g1rJ6',
            'secret': '8aHAzSUUJw'
        }
        
        if dry_run:
            logger.info(f"[DRY-RUN] Would POST listing {listing_id} with {len(images)} images")
            return (True, listing_id, None)
        
        # --- SEND REQUEST ---
        # Body: data_form = JSON String
        json_payload = json.dumps(payload_data, default=json_serial)
        logger.info(f"Payload for {listing_id}: {json_payload}")
        
        post_body = {
            'data_form': json_payload
        }
        
        response = requests.post(
            get_api_url(api_mode),
            data=post_body,
            headers=headers,
            timeout=30
        )
        
        # --- VERIFY RESPONSE ---
        if response.status_code == 200:
            try:
                resp_json = response.json()
                if resp_json.get('success') is True:
                     return (True, listing_id, None)
                
                # Failed
                msg = resp_json.get('message')
                errors = resp_json.get('errors')
                err_str = f"{msg} | {errors}" if errors else msg
                return (False, listing_id, f"API Error: {err_str}")
            except ValueError:
                pass

        return (False, listing_id, f"API HTTP {response.status_code}: {response.text[:200]}")
            
    except Exception as e:
        return (False, listing_id, str(e))

def should_retry(error: str) -> bool:
    if not error:
        return False
    e = error.lower()
    # Retry on network-ish / transient / rate limit errors.
    transient_markers = [
        "timed out",
        "timeout",
        "connection",
        "temporarily",
        "reset",
        "502",
        "503",
        "504",
        "429",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
    ]
    return any(m in e for m in transient_markers)

def process_one_listing(db: Database, listing: dict, dry_run: bool, retries: int, table_name: str = "data_full", api_mode: str = "normal") -> tuple:
    """Process 1 listing with retries. Returns (success, listing_id, error_or_none)."""
    listing_id = listing['id'] if isinstance(listing, dict) else listing[0]
    id_img = listing['id_img'] if isinstance(listing, dict) else listing[11]

    # Anti duplicate by source_post_id (if already uploaded)
    if not dry_run and isinstance(listing, dict) and is_duplicate_uploaded(db, listing, table_name=table_name):
        return (True, listing_id, "DUPLICATE_SKIPPED")

    images = get_listing_images(db, id_img, table_name=table_name)
    if not images:
        return (False, listing_id, "NO_IMAGES")

    attempt = 0
    last_err = None
    while attempt < max(1, retries):
        attempt += 1
        ok, lid, err = upload_listing(listing, images, dry_run, api_mode=api_mode)
        if ok:
            return (True, lid, None)
        last_err = err
        if attempt < retries and should_retry(err or ""):
            time.sleep(min(5.0, 0.5 * (2 ** (attempt - 1))))
            continue
        break
    return (False, listing_id, last_err or "UNKNOWN_ERROR")

def run_uploader(args):
    """Main processing loop."""
    logger.info("=== LISTING UPLOADER ===")
    logger.info(f"Table: {args.table}")
    logger.info(f"API URL: {get_api_url(args.api_mode)}")
    if args.exclude_province_ids:
        logger.info(f"Exclude provinces: {args.exclude_province_ids}")
    
    db = Database()
    ensure_uploaded_at_schema(db, table_name=args.table)
    if args.area_filter_lt20:
        ensure_area_filter_schema(db)
        refresh_area_filter_table(db)
        logger.info(f"Area filter enabled via {AREA_FILTER_TABLE}")
    stats = {'ok': 0, 'fail': 0}
    stats_lock = Lock()
    
    try:
        total_processed = 0
        
        while True:
            # If user targets one listing id, process once and exit.
            if getattr(args, "id", None):
                one = get_listing_by_id(db, int(args.id), dry_run=args.dry_run, table_name=args.table)
                if not one:
                    logger.info(f"Listing id={args.id} not ready (need images_status=IMAGES_READY) or not found.")
                    break
                listings = [one]
                batch_size = 1
            else:
            # Determine batch size respecting the limit
                batch_size = args.batch
                if args.limit > 0:
                    remaining = args.limit - total_processed
                    if remaining <= 0:
                        break
                    if remaining < batch_size:
                        batch_size = remaining

                listings = get_ready_listings(
                    db,
                    limit=batch_size,
                    dry_run=args.dry_run,
                    order=args.order,
                    table_name=args.table,
                    area_filter=args.area_filter_lt20,
                    exclude_province_ids=args.exclude_province_ids,
                    area_filter_table=args.area_filter_table,
                    area_filter_max_total=args.area_filter_max_total,
                )
            
            if not listings:
                if args.continuous:
                    logger.info("No listings ready, waiting 5s...")
                    time.sleep(5)
                    continue
                else:
                    logger.info("No more listings to process")
                    break
            
            logger.info(f"Processing batch: {len(listings)} listings")
            
            workers = max(1, int(args.workers))
            # Upload concurrently inside a batch
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = [
                    ex.submit(process_one_listing, db, listing, args.dry_run, int(args.retries), args.table, args.api_mode)
                    for listing in listings
                ]
                for fut in as_completed(futs):
                    success, lid, error = fut.result()
                    if success:
                        if not args.dry_run:
                            if error == "DUPLICATE_SKIPPED":
                                update_listing_status(db, lid, 'DUPLICATE_SKIPPED', table_name=args.table)
                                logger.info(f"⏭️ Listing {lid} skipped (duplicate source_post_id already uploaded)")
                            else:
                                update_listing_status(db, lid, 'LISTING_UPLOADED', table_name=args.table)
                                logger.info(f"✅ Listing {lid} uploaded")
                        with stats_lock:
                            stats['ok'] += 1
                    else:
                        if not args.dry_run:
                            if error == "NO_IMAGES":
                                update_listing_status(db, lid, 'NO_IMAGES', table_name=args.table)
                                logger.warning(f"⚠️ Listing {lid} has no images, skipping")
                            else:
                                update_listing_status(db, lid, 'UPLOAD_FAILED', table_name=args.table)
                        logger.error(f"❌ Listing {lid} failed: {error}")
                        with stats_lock:
                            stats['fail'] += 1
            
            total_processed += len(listings)
            if getattr(args, "id", None):
                break
            
            if args.limit > 0 and total_processed >= args.limit:
                logger.info(f"Reached limit of {args.limit} listings")
                break
            
            time.sleep(0.5)
        
        logger.info("=" * 50)
        logger.info(f"COMPLETED. Total: {total_processed}, OK: {stats['ok']}, FAIL: {stats['fail']}")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


def main():
    parser = argparse.ArgumentParser(description='Listing Uploader')
    parser.add_argument('--batch', type=int, default=50, help='Batch size')
    parser.add_argument('--limit', type=int, default=0, help='Max listings to process (0=unlimited)')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without posting')
    parser.add_argument('--continuous', action='store_true', help='Run continuously, wait for new listings')
    parser.add_argument('--id', type=int, default=0, help='Upload a specific data_full.id (requires IMAGES_READY)')
    parser.add_argument('--workers', type=int, default=15, help='Concurrent uploads per batch')
    parser.add_argument('--retries', type=int, default=3, help='Retry count per listing on transient errors')
    parser.add_argument('--order', type=str, default='asc', choices=['asc', 'desc'], help='Pick listings in asc/desc id order')
    parser.add_argument('--table', type=str, default='data_full', choices=['data_full', 'data_no_full'], help='Source listing table')
    parser.add_argument('--api-mode', type=str, default='normal', choices=['normal', 'null-contact'], help='Target API mode')
    parser.add_argument('--area-filter-lt20', action='store_true', help='Only upload province/ward pairs where data_full + nhadat_data < 20')
    parser.add_argument('--area-filter-table', type=str, default='', help='Optional custom area filter table to join by province_id + ward_id')
    parser.add_argument('--area-filter-max-total', type=int, default=None, help='Optional max total_count threshold when using --area-filter-table')
    parser.add_argument('--exclude-province-ids', type=str, default='', help='Comma-separated province_id list to exclude, e.g. 63,1')
    
    args = parser.parse_args()
    args.exclude_province_ids = [int(x.strip()) for x in args.exclude_province_ids.split(',') if x.strip()]
    args.area_filter_table = (args.area_filter_table or '').strip() or None
    run_uploader(args)


if __name__ == '__main__':
    main()
