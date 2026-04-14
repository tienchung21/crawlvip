import requests
import time
import json
import pymysql
import os
import sys
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, parse_qsl, urlencode, urlsplit, urlunsplit

# Configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'
BATCH_SIZE = 300
STOP_ON_DUP = 400  # Stop when 400 consecutive duplicate ad_id are seen
SKIP_TOTAL_THRESHOLD = 5000 
RECURSIVE_THRESHOLD = 2000 # If total > 2000, drill down to sub-levels
DELAY_SECONDS = 1.0
BASE_URL = "https://gateway.chotot.com/v1/public/ad-listing"
IMAGE_SYNC_RETRIES = 4
IMAGE_SYNC_RETRY_SLEEP = 1.5

# Keep only videos in ad_listing_detail. Image URLs are normalized into
# scraped_detail_images using detail_id = ad_id for Nhatot.
AD_SKIP_FIELDS_LIGHT = {
    "image",
    "images",
    "image_thumbnails",
    "thumbnail_image",
    "webp_image",
    "inspection_images",
    "special_display_images",
    "number_of_images",
    "avatar",
    "seller_info",
    "time_crawl",
    "raw_json",
}

MEDIA_FIELDS = [
    "body",
    "subject",
    "videos",
]

AD_ALL_COLUMNS = [
    "account_id", "account_name", "account_oid", "ad_features", "ad_id", "ad_labels",
    "area", "area_name", "area_v2", "avatar", "average_rating", "average_rating_for_seller",
    "body", "business_days", "category", "category_name", "contain_videos", "date", "fee_type",
    "full_name", "furnishing_sell", "house_type", "image", "image_thumbnails", "images",
    "inspection_images", "is_sticky", "is_zalo_show", "label_campaigns", "latitude", "list_id",
    "list_time", "location", "longitude", "number_of_images", "orig_list_time", "params", "price",
    "price_million_per_m2", "price_string", "property_legal_document", "protection_entitlement",
    "pty_characteristics", "pty_jupiter", "pty_map", "pty_map_modifier", "pty_project_name",
    "region", "region_name", "region_name_v3", "region_v2", "rooms", "seller_info", "shop", "size",
    "size_unit_string", "sold_ads", "special_display_images", "specific_service_offered", "state",
    "status", "street_name", "street_number", "streetnumber_display", "subject", "thumbnail_image",
    "total_rating", "total_rating_for_seller", "type", "videos", "ward", "ward_name", "ward_name_v3",
    "webp_image",
    "address", "apartment_feature", "apartment_type", "balconydirection", "block", "commercial_type",
    "company_ad", "deposit", "detail_address", "direction", "floornumber", "floors", "furnishing_rent",
    "has_video", "is_block_similar_ads_other_agent", "is_good_room", "is_main_street", "land_type",
    "length", "living_size", "location_id", "project_oid", "projectid", "projectimages",
    "property_status", "shop_alias", "size_unit", "special_display", "sticky_ad_type",
    "stickyad_feature", "toilets", "unique_street_id", "unitnumber", "unitnumber_display", "width",
    "time_crawl",
    "raw_json",
    "__source_file", "__source_o",
]

AD_JSON_FIELDS = {
    "ad_features", "ad_labels", "business_days", "fee_type", "image_thumbnails",
    "images", "inspection_images", "label_campaigns", "params", "pty_characteristics",
    "seller_info", "shop", "special_display_images", "specific_service_offered", "videos",
    "apartment_feature", "projectimages", "special_display", "stickyad_feature",
}

def get_connection():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

def _ad_normalize_value(key: str, val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, str) and val.strip().lower() == "default":
        return None
    if key in AD_JSON_FIELDS:
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    if isinstance(val, bool):
        return 1 if val else 0
    return val

def _ad_upsert_ads(conn, table: str, ads: List[Dict[str, Any]], source_file: str, source_o: Any = None, batch_size: int = 300, skip_fields: Optional[set] = None) -> int:
    if not ads:
        return 0

    cols_sql = ", ".join(f"`{c}`" for c in AD_ALL_COLUMNS)
    placeholders = ", ".join(["%s"] * len(AD_ALL_COLUMNS))
    sql = f"""
        INSERT IGNORE INTO `{table}` ({cols_sql})
        VALUES ({placeholders})
    """

    rows = []
    skip_fields = skip_fields or set()
    for ad in ads:
        row = {}
        for c in AD_ALL_COLUMNS:
            if c == "__source_file":
                row[c] = source_file
            elif c == "__source_o":
                row[c] = source_o
            elif c == "time_crawl":
                row[c] = int(time.time() * 1000)
            elif c in skip_fields:
                row[c] = None
            elif c == "raw_json":
                if "raw_json" in skip_fields:
                    row[c] = None
                else:
                    row[c] = json.dumps(ad, ensure_ascii=False, separators=(",", ":"))
            else:
                row[c] = _ad_normalize_value(c, ad.get(c))
        rows.append([row[c] for c in AD_ALL_COLUMNS])

    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            try:
                cur.executemany(sql, rows[i:i + batch_size])
                inserted += cur.rowcount
            except Exception as e:
                print(f"Error upserting batch: {e}")
        conn.commit()
    return inserted

def _ad_upsert_ads_update_existing(
    conn,
    table: str,
    ads: List[Dict[str, Any]],
    source_file: str,
    source_o: Any = None,
    batch_size: int = 300,
    skip_fields: Optional[set] = None,
    update_fields: Optional[List[str]] = None,
) -> int:
    if not ads:
        return 0

    cols_sql = ", ".join(f"`{c}`" for c in AD_ALL_COLUMNS)
    placeholders = ", ".join(["%s"] * len(AD_ALL_COLUMNS))
    update_fields = update_fields or []
    update_sql = ", ".join(
        f"`{c}`=IFNULL(VALUES(`{c}`), `{c}`)" for c in update_fields
    )
    meta_updates = "`__source_file`=VALUES(`__source_file`), `__source_o`=VALUES(`__source_o`), `time_crawl`=VALUES(`time_crawl`)"
    if update_sql:
        update_sql = f"{update_sql}, {meta_updates}"
    else:
        update_sql = meta_updates
    sql = f"""
        INSERT INTO `{table}` ({cols_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_sql}
    """

    rows = []
    skip_fields = skip_fields or set()
    for ad in ads:
        row = {}
        for c in AD_ALL_COLUMNS:
            if c == "__source_file":
                row[c] = source_file
            elif c == "__source_o":
                row[c] = source_o
            elif c == "time_crawl":
                row[c] = int(time.time() * 1000)
            elif c in skip_fields:
                row[c] = None
            elif c == "raw_json":
                if "raw_json" in skip_fields:
                    row[c] = None
                else:
                    row[c] = json.dumps(ad, ensure_ascii=False, separators=(",", ":"))
            else:
                row[c] = _ad_normalize_value(c, ad.get(c))
        rows.append([row[c] for c in AD_ALL_COLUMNS])

    affected = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            try:
                cur.executemany(sql, rows[i:i + batch_size])
                affected += cur.rowcount
            except Exception as e:
                print(f"Error upserting/updating batch: {e}")
        conn.commit()
    return affected

def _ad_fetch_existing_ids(conn, table: str, ad_ids: List[Any], batch_size: int = 1000) -> set:
    if not ad_ids:
        return set()
    existing = set()
    with conn.cursor() as cur:
        for i in range(0, len(ad_ids), batch_size):
            chunk = ad_ids[i:i + batch_size]
            if not chunk: continue
            placeholders = ", ".join(["%s"] * len(chunk))
            cur.execute(f"SELECT ad_id FROM `{table}` WHERE ad_id IN ({placeholders})", chunk)
            rows = cur.fetchall()
            for row in rows:
                existing.add(row['ad_id'])
    return existing

def _sync_scraped_detail_images(conn, ads: List[Dict[str, Any]], batch_size: int = 500) -> int:
    if not ads:
        return 0

    rows = []
    for ad in ads:
        detail_id = ad.get("ad_id")
        images = ad.get("images") or []
        if not detail_id or not isinstance(images, list):
            continue
        idx_counter = 0
        for img in images:
            if isinstance(img, str) and img.strip():
                rows.append((int(detail_id), img.strip(), idx_counter))
                idx_counter += 1

    if not rows:
        return 0

    sql = """
        INSERT INTO scraped_detail_images (detail_id, image_url, idx, status)
        SELECT %s, %s, %s, 'PENDING'
        FROM DUAL
        WHERE NOT EXISTS (
            SELECT 1
            FROM scraped_detail_images
            WHERE detail_id = %s
              AND image_url = %s
        )
    """
    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            params = [(detail_id, image_url, idx, detail_id, image_url) for detail_id, image_url, idx in chunk]
            for attempt in range(1, IMAGE_SYNC_RETRIES + 1):
                try:
                    cur.executemany(sql, params)
                    inserted += cur.rowcount
                    break
                except pymysql.err.OperationalError as e:
                    if e.args and e.args[0] == 1213 and attempt < IMAGE_SYNC_RETRIES:
                        conn.rollback()
                        wait_s = IMAGE_SYNC_RETRY_SLEEP * attempt
                        print(
                            f"Retry _sync_scraped_detail_images chunk due to deadlock "
                            f"(attempt {attempt}/{IMAGE_SYNC_RETRIES}, sleep {wait_s:.1f}s)"
                        )
                        time.sleep(wait_s)
                        continue
                    conn.rollback()
                    raise
        conn.commit()
    return inserted

def _ad_fetch_ads(url: str):
    headers = {
        "accept": "application/json;version=1",
        "origin": "https://www.nhatot.com",
        "referer": "https://www.nhatot.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        status = resp.status_code
        if status != 200:
            return [], None, status, False
        data = resp.json()
        return data.get("ads", []), data.get("total"), status, False
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return [], None, 999, False

def _fetch_regions(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT region_id, name FROM location_detail WHERE level=1 AND is_active=1 ORDER BY name")
        return cur.fetchall()

def _fetch_areas(conn, region_id):
    with conn.cursor() as cur:
        cur.execute("SELECT area_id, name FROM location_detail WHERE level=2 AND region_id=%s AND is_active=1 ORDER BY name", (region_id,))
        return cur.fetchall()

def _fetch_wards(conn, region_id, area_id):
    with conn.cursor() as cur:
        cur.execute("SELECT ward_id, name FROM location_detail WHERE level=3 AND region_id=%s AND area_id=%s AND is_active=1 ORDER BY name", (region_id, area_id))
        return cur.fetchall()

def _fetch_wards_by_region(conn, region_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ward_id, name, area_id
            FROM location_detail
            WHERE level=3 AND region_id=%s AND is_active=1
            ORDER BY area_id, name
            """,
            (region_id,),
        )
        return cur.fetchall()

def _get_db_count_scope(conn, region_id, area_id=None, ward_id=None):
    with conn.cursor() as cur:
        sql = "SELECT COUNT(*) as c FROM ad_listing_detail WHERE region_v2=%s"
        params = [region_id]
        if area_id:
            sql += " AND area_v2=%s"
            params.append(area_id)
        if ward_id:
            sql += " AND ward=%s"
            params.append(ward_id)
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        return row['c'] if row else 0

def crawl_scope(conn, region_id, area_id, ward_id, rname, aname, wname):
    """
    Generic crawler for a specific scope (Region, Area, or Ward).
    Returns True if crawled successfully, False if skipped.
    """
    scope_label = f"{rname}"
    if aname: scope_label += f" - {aname}"
    if wname: scope_label += f" - {wname}"

    # Build Check URL
    params = {
        "cg": 1000,
        "limit": 1,
        "o": 0,
        "region_v2": region_id,
        "key_param_included": "true",
        "include_expired_ads": "true"
    }
    if area_id: params["area_v2"] = area_id
    if ward_id: params["ward"] = ward_id

    check_url = f"{BASE_URL}?{urlencode(params)}"
    _, total_api, status, _ = _ad_fetch_ads(check_url)
    
    if status != 200:
        print(f"[{scope_label}] Failed to check total. Status {status}")
        return False
    if total_api is None: total_api = 0

    # Decision Logic
    # 1. If recursive check needed:
    #    If we are at Region level and Total > RECURSIVE_THRESHOLD -> Drill to Area
    #    If we are at Area level and Total > RECURSIVE_THRESHOLD -> Drill to Ward
    
    should_drill_down = False
    if ward_id is None: # Not at leaf level yet
        if total_api > RECURSIVE_THRESHOLD:
            should_drill_down = True
            print(f"[{scope_label}] Total {total_api} > {RECURSIVE_THRESHOLD}. Drilling down...")
            return "DRILL_DOWN"

    # Logic: If query returns < Threshold OR we are at leaf level (Ward), we crawl here.
    # UPDATED: Always crawl (at least 1 page) because DB holds historical data while API might only have 90 days.
    # Rely on STOP_ON_DUP to exit early.
    
    # 2. Check DB count (just for logging)
    db_count = _get_db_count_scope(conn, region_id, area_id, ward_id)
    print(f"[{scope_label}] API Total: {total_api}, DB Count: {db_count}")
    
    # 3. Crawl
    limit = 50
    offset = 0
    dup_streak = 0
    total_fetched = 0
    scope_new = 0
    
    print(f"  -> Crawling {scope_label}...")
    
    while True:
        params["limit"] = limit
        params["o"] = offset
        crawl_url = f"{BASE_URL}?{urlencode(params)}"
        
        ads, _, status, _ = _ad_fetch_ads(crawl_url)
        if status != 200 or not ads:
            break
        
        ad_ids = [ad.get('ad_id') for ad in ads]
        existing = _ad_fetch_existing_ids(conn, 'ad_listing_detail', ad_ids)
        
        upserted = _ad_upsert_ads(conn, 'ad_listing_detail', ads, "daily_api", offset, skip_fields=AD_SKIP_FIELDS_LIGHT)
        images_inserted = _sync_scraped_detail_images(conn, ads)
        
        batch_dup = 0
        for aid in ad_ids:
            if aid in existing:
                dup_streak += 1
                batch_dup += 1
            else:
                dup_streak = 0
        
        total_fetched += len(ads)
        scope_new += upserted
        if images_inserted:
            print(f"    -> synced scraped_detail_images: +{images_inserted}")
        
        if dup_streak >= STOP_ON_DUP:
            print(f"    -> Stopped {scope_label} due to {dup_streak} consecutive duplicates.")
            break
        
        offset += limit
        time.sleep(DELAY_SECONDS)
        
        # Safety break
        if offset > 10000:
           print(f"    -> Reached safety offset limit 10000.")
           break

    return {"fetched": total_fetched, "new": scope_new}

def crawl_nhatot():
    conn = get_connection()
    try:
        regions = _fetch_regions(conn)
        print(f"Found {len(regions)} regions.")
        
        g_fetched = 0
        g_new = 0
        
        for reg in regions:
            rid = reg['region_id']
            rname = reg['name']
            
            # Try Region level
            res_reg = crawl_scope(conn, rid, None, None, rname, None, None)
            
            if res_reg == "DRILL_DOWN":
                # User-requested behavior:
                # If region total is large, skip level-2 entirely and drill directly to level-3 wards.
                wards = _fetch_wards_by_region(conn, rid)
                print(f"  -> Direct ward drill for {rname}: {len(wards)} wards (skip level-2)")
                for ward in wards:
                    wid = ward['ward_id']
                    wname = ward['name']
                    aid = ward.get('area_id')
                    stats = crawl_scope(conn, rid, aid, wid, rname, f"area_{aid}" if aid else None, wname)
                    if isinstance(stats, dict):
                        g_fetched += stats.get("fetched", 0)
                        g_new += stats.get("new", 0)
            elif isinstance(res_reg, dict):
                 g_fetched += res_reg.get("fetched", 0)
                 g_new += res_reg.get("new", 0)

        print(f"=== SUMMARY ===")
        print(f"Total Fetched: {g_fetched}")
        print(f"Total New: {g_new}") 

                            
    finally:
        conn.close()

if __name__ == "__main__":
    print("=== Starting Nhatot Daily API Crawl (Recursive V2) ===")
    crawl_nhatot()
    print("=== Finished ===")
