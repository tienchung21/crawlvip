#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database


SOURCE_DOMAIN = "guland.vn"

PROPERTY_RULES = {
    ("Nhà riêng", "s"): {"property_type": "Bán nhà riêng", "cat_id": 1, "type_id": 2},
    ("Nhà riêng", "u"): {"property_type": "Cho thuê nhà riêng", "cat_id": 3, "type_id": 2},
    ("Đất", "s"): {"property_type": "Bán đất thổ cư", "cat_id": 2, "type_id": 11},
    ("Đất", "u"): {"property_type": "Cho thuê đất", "cat_id": 3, "type_id": 12},
    ("Căn hộ chung cư", "s"): {"property_type": "Bán căn hộ chung cư", "cat_id": 1, "type_id": 5},
    ("Căn hộ chung cư", "u"): {"property_type": "Cho thuê căn hộ chung cư", "cat_id": 3, "type_id": 5},
    ("Kho, nhà xưởng", "s"): {"property_type": "Bán kho, nhà xưởng", "cat_id": 1, "type_id": 14},
    ("Kho, nhà xưởng", "u"): {"property_type": "Cho thuê nhà kho - Xưởng", "cat_id": 3, "type_id": 14},
    ("Văn phòng", "s"): {"property_type": "Bán căn hộ Mini, Dịch vụ", "cat_id": 1, "type_id": 56},
    ("Văn phòng", "u"): {"property_type": "Cho thuê văn phòng", "cat_id": 3, "type_id": 6},
    ("Nhà trọ", "s"): {"property_type": "Bán căn hộ Mini, Dịch vụ", "cat_id": 1, "type_id": 56},
    ("Nhà trọ", "u"): {"property_type": "Cho thuê phòng trọ", "cat_id": 3, "type_id": 15},
    ("Phòng trọ", "s"): {"property_type": "Bán căn hộ Mini, Dịch vụ", "cat_id": 1, "type_id": 56},
    ("Phòng trọ", "u"): {"property_type": "Cho thuê phòng trọ", "cat_id": 3, "type_id": 15},
    ("Khách sạn", "s"): {"property_type": "Bán nhà hàng - Khách sạn", "cat_id": 1, "type_id": 13},
    ("Khách sạn", "u"): {"property_type": "Cho thuê nhà hàng - Khách sạn", "cat_id": 3, "type_id": 13},
    ("Mặt bằng kinh doanh", "s"): {"property_type": "Bán căn hộ Mini, Dịch vụ", "cat_id": 1, "type_id": 56},
    ("Mặt bằng kinh doanh", "u"): {"property_type": "Cho thuê mặt bằng", "cat_id": 3, "type_id": 12},
}


def clean_raw(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value or value == "---":
        return None
    return value


def normalize_text(value):
    value = clean_raw(value)
    if not value:
        return ""
    value = value.replace("đ", "d").replace("Đ", "D")
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = value.lower()
    # Expand administrative abbreviations before collapsing spaces.
    value = re.sub(r"\btt\.\s*", "thi tran ", value)
    value = re.sub(r"\btt\s+", "thi tran ", value)
    value = re.sub(r"\bp\.\s*", "phuong ", value)
    value = re.sub(r"\bp\s+", "phuong ", value)
    value = re.sub(r"\bx\.\s*", "xa ", value)
    value = re.sub(r"\bx\s+", "xa ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_province_name(value):
    value = normalize_text(value)
    if not value:
        return ""
    value = re.sub(r"^(tp\.?|thanh pho|tinh)\s+", "", value)
    return value.strip()


def normalize_ward_name(value):
    value = normalize_text(value)
    if not value:
        return ""
    value = re.sub(r"^(phuong|xa|thi tran)\s+", "", value)
    return value.strip()


def looks_like_ward(value):
    value = normalize_text(value)
    return bool(re.match(r"^(phuong|xa|thi tran)\s+", value))


def looks_like_district(value):
    value = normalize_text(value)
    return bool(re.match(r"^(quan|huyen|thi xa|thanh pho)\s+", value))


def parse_decimal(value):
    value = clean_raw(value)
    if not value:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)", value)
    if not m:
        return None
    try:
        return Decimal(m.group(1).replace(",", "."))
    except Exception:
        return None


def parse_area(value):
    value = clean_raw(value)
    if not value:
        return None
    m = re.search(r"(\d[\d\s.,]*)", value)
    if not m:
        return None
    num = re.sub(r"\s+", "", m.group(1))
    if "." in num and "," in num:
        if num.find(".") < num.find(","):
            num = num.replace(".", "").replace(",", ".")
        else:
            num = num.replace(",", "")
    elif "." in num:
        if re.fullmatch(r"\d{1,3}(?:\.\d{3})+(?:,\d+)?", num):
            num = num.replace(".", "").replace(",", ".")
    elif "," in num:
        if re.fullmatch(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?", num):
            num = num.replace(",", "")
        else:
            num = num.replace(",", ".")
    try:
        return Decimal(num)
    except Exception:
        return None


def parse_int(value):
    value = clean_raw(value)
    if not value:
        return None
    m = re.search(r"(\d+)", value)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def parse_price_to_vnd(value):
    value = clean_raw(value)
    if not value:
        return None
    text = normalize_text(value)
    m = re.search(r"(\d+(?:[.,]\d+)?)", text)
    if not m:
        return None
    num = Decimal(m.group(1).replace(",", "."))
    if "ty" in text:
        return int(num * Decimal("1000000000"))
    if "trieu" in text:
        return int(num * Decimal("1000000"))
    if "nghin" in text or "ngan" in text:
        return int(num * Decimal("1000"))
    return None


MAX_DB_PRICE_VND = 999999999999999999


def subtract_months(dt, months):
    year = dt.year
    month = dt.month - months
    while month <= 0:
        year -= 1
        month += 12
    day = min(dt.day, 28)
    return dt.replace(year=year, month=month, day=day)


def parse_posted_at(relative_text, crawled_at):
    relative_text = clean_raw(relative_text)
    if not relative_text or not crawled_at:
        return None
    text = normalize_text(relative_text)
    m = re.search(r"(\d+)\s+(giay|phut|gio|ngay|tuan|thang)", text)
    if not m:
        return crawled_at
    amount = int(m.group(1))
    unit = m.group(2)
    if unit == "giay":
        return crawled_at - timedelta(seconds=amount)
    if unit == "phut":
        return crawled_at - timedelta(minutes=amount)
    if unit == "gio":
        return crawled_at - timedelta(hours=amount)
    if unit == "ngay":
        return crawled_at - timedelta(days=amount)
    if unit == "tuan":
        return crawled_at - timedelta(days=7 * amount)
    if unit == "thang":
        return subtract_months(crawled_at, amount)
    return crawled_at


def build_location_maps(conn):
    province_new = {}
    province_old = {}
    child_new = {}
    child_old = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT new_city_id, new_city_parent_id, new_city_name, old_city_name, action_type
            FROM transaction_city_merge
            """
        )
        for row in cur.fetchall():
            new_city_id = int(row["new_city_id"])
            parent_id = int(row["new_city_parent_id"] or 0)
            action_type = int(row["action_type"] or 0)
            new_name = clean_raw(row.get("new_city_name"))
            old_name = clean_raw(row.get("old_city_name"))
            if parent_id == 0:
                if new_name:
                    key = normalize_province_name(new_name)
                    province_new.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
                if old_name:
                    key = normalize_province_name(old_name)
                    province_old.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
            else:
                if new_name:
                    key = (parent_id, normalize_ward_name(new_name))
                    child_new.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
                if old_name:
                    key = (parent_id, normalize_ward_name(old_name))
                    child_old.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
    return province_new, province_old, child_new, child_old


def build_project_merge_map(conn):
    project_map = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT guland_project_name, duan_id, duan_ten
            FROM duan_guland_duan_merge
            """
        )
        for row in cur.fetchall():
            raw_name = clean_raw(row.get("guland_project_name"))
            if not raw_name:
                continue
            # Keep the newest/highest duan_id if duplicate names exist in merge table.
            prev = project_map.get(raw_name)
            if not prev or int(row["duan_id"]) > int(prev["project_id"]):
                project_map[raw_name] = {
                    "project_id": int(row["duan_id"]),
                    "project_name": clean_raw(row.get("duan_ten")) or raw_name,
                }
    return project_map


def pick_best(candidates):
    if not candidates:
        return None
    return sorted(candidates, key=lambda x: (x[0] != 0, x[1]))[0]


def parse_location(diachi, province_new, province_old, child_new, child_old):
    raw = clean_raw(diachi)
    if not raw:
        return {"province_id": None, "ward_id": None, "street": None, "province_name_raw": None, "ward_name_raw": None, "address_case": None}
    is_new_address = "(mới)" in normalize_text(raw)
    cleaned = re.sub(r"\(\s*moi\s*\)", "", normalize_text(raw), flags=re.I)
    # normalize_text removed accents; keep raw parts for street.
    raw_cleaned = re.sub(r"\(\s*Mới\s*\)", "", raw, flags=re.I).strip()
    parts = [p.strip() for p in raw_cleaned.split(",") if p.strip()]
    if len(parts) < 2:
        return {"province_id": None, "ward_id": None, "street": None, "province_name_raw": None, "ward_name_raw": None, "address_case": None}

    province_name_raw = parts[-1]
    province_id = None
    ward_id = None
    ward_name_raw = None
    street = None
    address_case = None

    has_district = len(parts) >= 3 and looks_like_district(parts[-2])
    old_style = has_district or (len(parts) >= 4 and looks_like_district(parts[-2]))

    if is_new_address or not old_style:
        address_case = "new"
        province_key = normalize_province_name(province_name_raw)
        province_row = pick_best(province_new.get(province_key, []))
        if not province_row:
            province_row = pick_best(province_old.get(province_key, []))
        if province_row:
            province_id = province_row[1]
            ward_name_raw = parts[-2]
            ward_row = pick_best(child_new.get((province_id, normalize_ward_name(ward_name_raw)), []))
            if ward_row:
                ward_id = ward_row[1]
            elif ward_name_raw:
                ward_row = pick_best(child_old.get((province_id, normalize_ward_name(ward_name_raw)), []))
                if ward_row:
                    ward_id = ward_row[1]
            if len(parts) >= 3:
                street = ", ".join(parts[:-2]).strip() or None
    else:
        address_case = "old"
        province_row = pick_best(province_old.get(normalize_province_name(province_name_raw), []))
        if province_row:
            province_id = province_row[1]
            # Old full pattern: [street], ward_old, district_old, province_old
            if len(parts) >= 4 and looks_like_district(parts[-2]):
                ward_name_raw = parts[-3]
                if ward_name_raw:
                    ward_row = pick_best(child_old.get((province_id, normalize_ward_name(ward_name_raw)), []))
                    if ward_row:
                        ward_id = ward_row[1]
                street = ", ".join(parts[:-3]).strip() or None
            # Old short pattern with ward: [ward_old], district_old, province_old
            elif len(parts) == 3 and looks_like_district(parts[-2]):
                if looks_like_ward(parts[0]):
                    ward_name_raw = parts[0]
                    ward_row = pick_best(child_old.get((province_id, normalize_ward_name(ward_name_raw)), []))
                    if ward_row:
                        ward_id = ward_row[1]
                    street = None
                    address_case = "old_short_with_ward"
                else:
                    # Old district-only pattern: [street], district_old, province_old
                    ward_name_raw = None
                    street = clean_raw(parts[0])
                    ward_id = None
                    address_case = "old_district_only"

    return {
        "province_id": province_id,
        "ward_id": ward_id,
        "street": clean_raw(street),
        "province_name_raw": province_name_raw,
        "ward_name_raw": ward_name_raw,
        "address_case": address_case,
    }


def get_first_image(cur, detail_id):
    cur.execute(
        """
        SELECT image_url
        FROM scraped_detail_images
        WHERE detail_id = %s
        ORDER BY idx ASC, id ASC
        LIMIT 1
        """,
        (detail_id,),
    )
    row = cur.fetchone()
    return row["image_url"] if row else None


def map_rule(loaibds, trade_type):
    loaibds = clean_raw(loaibds)
    trade_type = clean_raw(trade_type)
    if not loaibds or not trade_type:
        return None
    return PROPERTY_RULES.get((loaibds, trade_type))


def infer_unit(cat_id):
    return "tháng" if cat_id == 3 else "md"


def build_payload(row, loc_maps, project_merge_map, cur):
    rule = map_rule(row.get("loaibds"), row.get("trade_type"))
    if not rule:
        return None, "skip_type"
    loc = parse_location(row.get("diachi"), *loc_maps)
    if not loc["province_id"] or not loc["ward_id"]:
        return None, "skip_region"

    created_at = row.get("created_at")
    raw_project_name = clean_raw(row.get("thuocduan"))
    merged_project = project_merge_map.get(raw_project_name) if raw_project_name else None

    price_vnd = parse_price_to_vnd(row.get("khoanggia"))
    if price_vnd is not None and (price_vnd <= 0 or price_vnd > MAX_DB_PRICE_VND):
        return None, "skip_price"

    payload = {
        "title": clean_raw(row.get("title")),
        "address": None,
        "posted_at": parse_posted_at(row.get("ngaydang"), created_at),
        "img": get_first_image(cur, row["id"]),
        "price": price_vnd,
        "area": parse_area(row.get("dientich")),
        "description": clean_raw(row.get("mota")),
        "property_type": rule["property_type"],
        "type": None,
        "house_direction": clean_raw(row.get("huongnha")),
        "floors": parse_int(row.get("sotang")),
        "bathrooms": None,
        "road_width": None,
        "living_rooms": None,
        "bedrooms": parse_int(row.get("sophongngu")),
        "legal_status": None,
        "lat": None,
        "long": None,
        "broker_name": clean_raw(row.get("tenmoigioi")),
        "phone": clean_raw(row.get("sodienthoai")),
        "source": SOURCE_DOMAIN,
        "time_converted_at": datetime.now(),
        "source_post_id": clean_raw(row.get("matin")),
        "width": parse_decimal(row.get("chieungang")),
        "length": None,
        "city": None,
        "district": None,
        "ward": None,
        "street": loc["street"],
        "province_id": loc["province_id"],
        "district_id": None,
        "ward_id": loc["ward_id"],
        "street_id": None,
        "id_img": row["id"],
        "project_name": (merged_project or {}).get("project_name") or raw_project_name,
        "slug_name": None,
        "images_status": None,
        "stratum_id": None,
        "cat_id": rule["cat_id"],
        "type_id": rule["type_id"],
        "unit": infer_unit(rule["cat_id"]),
        "project_id": (merged_project or {}).get("project_id"),
        "uploaded_at": None,
        "_debug": {
            "diachi": row.get("diachi"),
            "address_case": loc["address_case"],
            "province_name_raw": loc["province_name_raw"],
            "ward_name_raw": loc["ward_name_raw"],
            "province_id": loc["province_id"],
            "ward_id": loc["ward_id"],
        },
    }
    return payload, None


INSERT_COLUMNS = [
    "title", "address", "posted_at", "img", "price", "area", "description",
    "property_type", "type", "house_direction", "floors", "bathrooms",
    "road_width", "living_rooms", "bedrooms", "legal_status", "lat", "`long`",
    "broker_name", "phone", "source", "time_converted_at", "source_post_id",
    "width", "length", "city", "district", "ward", "street", "province_id",
    "district_id", "ward_id", "street_id", "id_img", "project_name",
    "slug_name", "images_status", "stratum_id", "cat_id", "type_id", "unit",
    "project_id", "uploaded_at",
]


def ensure_datafull_converted_column(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW COLUMNS FROM scraped_details_flat LIKE 'datafull_converted'")
        if not cur.fetchone():
            cur.execute(
                "ALTER TABLE scraped_details_flat "
                "ADD COLUMN datafull_converted TINYINT(1) NOT NULL DEFAULT 0"
            )
        cur.execute("SHOW COLUMNS FROM scraped_details_flat LIKE 'datafull_skip_reason'")
        if not cur.fetchone():
            cur.execute(
                "ALTER TABLE scraped_details_flat "
                "ADD COLUMN datafull_skip_reason VARCHAR(50) DEFAULT NULL"
            )
        cur.execute("SHOW COLUMNS FROM scraped_details_flat LIKE 'datafull_skip_at'")
        if not cur.fetchone():
            cur.execute(
                "ALTER TABLE scraped_details_flat "
                "ADD COLUMN datafull_skip_at DATETIME DEFAULT NULL"
            )
    conn.commit()


def fetch_rows(conn, limit, matin_list=None):
    sql = """
        SELECT *
        FROM scraped_details_flat
        WHERE domain = %s
          AND title IS NOT NULL
          AND mota IS NOT NULL
          AND khoanggia IS NOT NULL
          AND dientich IS NOT NULL
          AND matin IS NOT NULL
          AND COALESCE(datafull_converted, 0) = 0
    """
    params = [SOURCE_DOMAIN]
    if matin_list:
        sql += " AND matin IN ({})".format(",".join(["%s"] * len(matin_list)))
        params.extend(matin_list)
    else:
        sql += " AND datafull_skip_reason IS NULL"
    sql += " ORDER BY id DESC LIMIT %s"
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def already_exists(cur, source_post_id):
    cur.execute(
        """
        SELECT id
        FROM data_full
        WHERE source = %s AND source_post_id = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (SOURCE_DOMAIN, source_post_id),
    )
    row = cur.fetchone()
    return row["id"] if row else None


def mark_converted(cur, sdf_id):
    cur.execute(
        """
        UPDATE scraped_details_flat
        SET datafull_converted = 1,
            datafull_skip_reason = NULL,
            datafull_skip_at = NULL
        WHERE id = %s
        """,
        (sdf_id,),
    )


def mark_skipped(cur, sdf_id, skip_reason):
    cur.execute(
        """
        UPDATE scraped_details_flat
        SET datafull_skip_reason = %s,
            datafull_skip_at = NOW()
        WHERE id = %s
        """,
        (skip_reason, sdf_id),
    )


def insert_payload(cur, payload):
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    sql = f"INSERT INTO data_full ({', '.join(INSERT_COLUMNS)}) VALUES ({placeholders})"
    values = [payload.get(col.replace("`", "")) for col in INSERT_COLUMNS]
    cur.execute(sql, values)
    return cur.lastrowid


def print_preview(row, payload, skip_reason=None):
    print("=" * 100)
    print(f"detail_id={row['id']} matin={row.get('matin')} title={row.get('title')}")
    if skip_reason:
        print(f"skip_reason={skip_reason} | diachi={row.get('diachi')} | loaibds={row.get('loaibds')} | trade_type={row.get('trade_type')}")
        return
    debug = payload.pop("_debug", {})
    print(json.dumps(payload, ensure_ascii=False, default=str, indent=2))
    print(f"debug={json.dumps(debug, ensure_ascii=False)}")


def main():
    ap = argparse.ArgumentParser(description="Convert Guland -> data_full (preview or insert)")
    ap.add_argument("--preview-limit", type=int, default=3)
    ap.add_argument("--matin", help="Comma-separated matin list")
    ap.add_argument("--insert", action="store_true")
    args = ap.parse_args()

    matin_list = [x.strip() for x in args.matin.split(",") if x.strip()] if args.matin else None

    db = Database()
    conn = db.get_connection(True)
    ensure_datafull_converted_column(conn)
    loc_maps = build_location_maps(conn)
    project_merge_map = build_project_merge_map(conn)
    rows = fetch_rows(conn, args.preview_limit, matin_list=matin_list)

    with conn.cursor() as cur:
        for row in rows:
            payload, skip_reason = build_payload(row, loc_maps, project_merge_map, cur)
            print_preview(row, payload, skip_reason)
            if not args.insert:
                continue
            if skip_reason:
                mark_skipped(cur, row["id"], skip_reason)
                conn.commit()
                continue
            existing_id = already_exists(cur, row.get("matin"))
            if existing_id:
                print(f"already_exists_in_data_full={existing_id}")
                mark_converted(cur, row["id"])
                conn.commit()
                continue
            new_id = insert_payload(cur, payload)
            mark_converted(cur, row["id"])
            conn.commit()
            print(f"inserted_data_full_id={new_id}")
    conn.close()


if __name__ == "__main__":
    main()
