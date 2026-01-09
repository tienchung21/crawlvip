import json
import requests

from craw.database import Database

JSON_FIELDS = {
    "ad_features", "ad_labels", "business_days", "fee_type", "image_thumbnails",
    "images", "inspection_images", "label_campaigns", "params", "pty_characteristics",
    "seller_info", "shop", "special_display_images", "specific_service_offered", "videos",
    "apartment_feature", "projectimages", "special_display", "stickyad_feature"
}

ALL_COLUMNS = [
    "account_id","account_name","account_oid","ad_features","ad_id","ad_labels",
    "area","area_name","area_v2","avatar","average_rating","average_rating_for_seller",
    "body","business_days","category","category_name","contain_videos","date","fee_type",
    "full_name","furnishing_sell","house_type","image","image_thumbnails","images",
    "inspection_images","is_sticky","is_zalo_show","label_campaigns","latitude","list_id",
    "list_time","location","longitude","number_of_images","orig_list_time","params","price",
    "price_million_per_m2","price_string","property_legal_document","protection_entitlement",
    "pty_characteristics","pty_jupiter","pty_map","pty_map_modifier","pty_project_name",
    "region","region_name","region_name_v3","region_v2","rooms","seller_info","shop","size",
    "size_unit_string","sold_ads","special_display_images","specific_service_offered","state",
    "status","street_name","street_number","streetnumber_display","subject","thumbnail_image",
    "total_rating","total_rating_for_seller","type","videos","ward","ward_name","ward_name_v3",
    "webp_image",
    "address","apartment_feature","apartment_type","balconydirection","block","commercial_type",
    "company_ad","deposit","detail_address","direction","floornumber","floors","furnishing_rent",
    "has_video","is_block_similar_ads_other_agent","is_good_room","is_main_street","land_type",
    "length","living_size","location_id","project_oid","projectid","projectimages",
    "property_status","shop_alias","size_unit","special_display","sticky_ad_type",
    "stickyad_feature","toilets","unique_street_id","unitnumber","unitnumber_display","width",
    "raw_json",
    "__source_file","__source_o"
]

def normalize_value(key, val):
    if val is None:
        return None
    if key in JSON_FIELDS:
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    if isinstance(val, bool):
        return 1 if val else 0
    return val

def ensure_raw_json_column(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE 'raw_json'")
        if not cur.fetchone():
            cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `raw_json` JSON NULL")
            conn.commit()

def ensure_extra_columns(conn, table):
    columns = {
        "address": "VARCHAR(255) NULL",
        "apartment_feature": "JSON NULL",
        "apartment_type": "INT NULL",
        "balconydirection": "INT NULL",
        "block": "VARCHAR(255) NULL",
        "commercial_type": "INT NULL",
        "company_ad": "TINYINT(1) NULL",
        "deposit": "BIGINT NULL",
        "detail_address": "VARCHAR(255) NULL",
        "direction": "INT NULL",
        "floornumber": "INT NULL",
        "floors": "INT NULL",
        "furnishing_rent": "INT NULL",
        "has_video": "TINYINT(1) NULL",
        "is_block_similar_ads_other_agent": "TINYINT(1) NULL",
        "is_good_room": "TINYINT(1) NULL",
        "is_main_street": "TINYINT(1) NULL",
        "land_type": "INT NULL",
        "length": "DOUBLE NULL",
        "living_size": "DOUBLE NULL",
        "location_id": "VARCHAR(64) NULL",
        "project_oid": "VARCHAR(64) NULL",
        "projectid": "BIGINT NULL",
        "projectimages": "JSON NULL",
        "property_status": "INT NULL",
        "shop_alias": "VARCHAR(64) NULL",
        "size_unit": "VARCHAR(32) NULL",
        "special_display": "JSON NULL",
        "sticky_ad_type": "INT NULL",
        "stickyad_feature": "JSON NULL",
        "toilets": "INT NULL",
        "unique_street_id": "VARCHAR(64) NULL",
        "unitnumber": "VARCHAR(64) NULL",
        "unitnumber_display": "VARCHAR(64) NULL",
        "width": "DOUBLE NULL",
    }

    with conn.cursor() as cur:
        for col, col_type in columns.items():
            cur.execute(f"SHOW COLUMNS FROM `{table}` LIKE %s", (col,))
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {col_type}")
        conn.commit()

def fetch_ads():
    url = "https://gateway.chotot.com/v1/public/ad-listing?cg=1020&key_param_included=true&video_count_included=true"
    headers = {
        "accept": "application/json;version=1",
        "origin": "https://www.nhatot.com",
        "referer": "https://www.nhatot.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("ads", [])

def upsert_ads(conn, table, ads, source_file, source_o=None, batch_size=300):
    if not ads:
        return 0

    cols_sql = ", ".join(f"`{c}`" for c in ALL_COLUMNS)
    placeholders = ", ".join(["%s"] * len(ALL_COLUMNS))
    update_cols = [c for c in ALL_COLUMNS if c != "ad_id"]
    update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols])

    sql = f"""
        INSERT INTO `{table}` ({cols_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_sql}
    """

    rows = []
    for ad in ads:
        row = {}
        for c in ALL_COLUMNS:
            if c == "__source_file":
                row[c] = source_file
            elif c == "__source_o":
                row[c] = source_o
            elif c == "raw_json":
                row[c] = json.dumps(ad, ensure_ascii=False, separators=(",", ":"))
            else:
                row[c] = normalize_value(c, ad.get(c))
        rows.append([row[c] for c in ALL_COLUMNS])

    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            cur.executemany(sql, rows[i:i + batch_size])
        conn.commit()
    return len(rows)

def main():
    ads = fetch_ads()
    print(f"Fetched ads: {len(ads)}")

    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    conn = db.get_connection()

    try:
        ensure_raw_json_column(conn, "ad_listing_detail")
        ensure_extra_columns(conn, "ad_listing_detail")
        inserted = upsert_ads(conn, "ad_listing_detail", ads, source_file="api", source_o=0)
        print(f"Upserted rows: {inserted}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
