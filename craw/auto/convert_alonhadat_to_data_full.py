#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import unicodedata
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database


SOURCE_DOMAIN = "alonhadat.com.vn"

# Business rule approved in mapping file.
# Important: every trade_type='u' must map to cat_id=3.
PROPERTY_RULES = {
    ("Nhà mặt tiền", "s"): {"property_type": "Bán nhà riêng", "cat_id": 1, "type_id": 2},
    ("Nhà mặt tiền", "u"): {"property_type": "Cho thuê nhà riêng", "cat_id": 3, "type_id": 2},
    ("Nhà trong hẻm", "s"): {"property_type": "Bán nhà riêng", "cat_id": 1, "type_id": 2},
    ("Nhà trong hẻm", "u"): {"property_type": "Cho thuê nhà riêng", "cat_id": 3, "type_id": 2},
    ("Biệt thự, nhà liền kề", "s"): {"property_type": "Bán biệt thự", "cat_id": 1, "type_id": 3},
    ("Biệt thự, nhà liền kề", "u"): {"property_type": "Cho thuê biệt thự", "cat_id": 3, "type_id": 3},
    ("Căn hộ chung cư", "s"): {"property_type": "Bán căn hộ chung cư", "cat_id": 1, "type_id": 5},
    ("Căn hộ chung cư", "u"): {"property_type": "Cho thuê căn hộ chung cư", "cat_id": 3, "type_id": 5},
    ("Phòng trọ, nhà trọ", "s"): {"property_type": "Bán căn hộ Mini, Dịch vụ", "cat_id": 1, "type_id": 56},
    ("Phòng trọ, nhà trọ", "u"): {"property_type": "Cho thuê phòng trọ", "cat_id": 3, "type_id": 15},
    ("Văn phòng", "s"): {"property_type": "Bán căn hộ Mini, Dịch vụ", "cat_id": 1, "type_id": 56},
    ("Văn phòng", "u"): {"property_type": "Cho thuê văn phòng", "cat_id": 3, "type_id": 6},
    ("Kho, xưởng", "s"): {"property_type": "Bán kho, nhà xưởng", "cat_id": 1, "type_id": 14},
    ("Kho, xưởng", "u"): {"property_type": "Cho thuê nhà kho - Xưởng", "cat_id": 3, "type_id": 14},
    ("Nhà hàng, khách sạn", "s"): {"property_type": "Bán nhà hàng - Khách sạn", "cat_id": 1, "type_id": 13},
    ("Nhà hàng, khách sạn", "u"): {"property_type": "Cho thuê nhà hàng - Khách sạn", "cat_id": 3, "type_id": 13},
    ("Shop, kiot, quán", "s"): {"property_type": "Bán căn hộ Mini, Dịch vụ", "cat_id": 1, "type_id": 56},
    ("Shop, kiot, quán", "u"): {"property_type": "Cho thuê mặt bằng", "cat_id": 3, "type_id": 12},
    ("Trang trại", "s"): {"property_type": "Bán đất nông, lâm nghiệp", "cat_id": 2, "type_id": 10},
    ("Trang trại", "u"): {"property_type": "Cho thuê đất", "cat_id": 3, "type_id": 12},
    ("Mặt bằng", "s"): {"property_type": "Bán đất nền dự án", "cat_id": 2, "type_id": 8},
    ("Mặt bằng", "u"): {"property_type": "Cho thuê mặt bằng", "cat_id": 3, "type_id": 12},
    ("Đất thổ cư, đất ở", "s"): {"property_type": "Bán đất thổ cư", "cat_id": 2, "type_id": 11},
    ("Đất nền, liền kề, đất dự án", "s"): {"property_type": "Bán đất thổ cư", "cat_id": 2, "type_id": 11},
    ("Đất nông, lâm nghiệp", "s"): {"property_type": "Bán đất nông, lâm nghiệp", "cat_id": 2, "type_id": 10},
}

STRATUM_RULES = {
    "Sổ hồng/ Sổ đỏ": 1,
    "Giấy tờ hợp lệ": 4,
    "Giấy phép XD": 8,
}


def normalize_text(value):
    if value is None:
        return ""
    value = str(value).strip()
    if not value:
        return ""
    value = value.replace("đ", "d").replace("Đ", "D")
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = value.lower()
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_admin_name(value):
    value = normalize_text(value)
    if not value:
        return ""
    value = re.sub(r"^(phuong|xa|thi tran)\s+", "", value)
    value = re.sub(r"^(tp\.?|thanh pho|tinh)\s+", "", value)
    return value.strip()


def clean_raw(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value or value == "---":
        return None
    return value


def clean_project_name(value):
    value = clean_raw(value)
    if not value:
        return None
    value = re.sub(r"\s*\(\s*xem\s+chi\s+tiết\s+dự\s+án\s*\)\s*$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def parse_decimal(value):
    value = clean_raw(value)
    if not value:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)", value)
    if not m:
        return None
    num = m.group(1).replace(",", ".")
    try:
        return Decimal(num)
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

    def _first_num(segment):
        m = re.search(r"(\d+(?:[.,]\d+)?)", segment)
        if not m:
            return None
        return Decimal(m.group(1).replace(",", "."))

    total = Decimal("0")
    has = False

    if "ty" in text:
        left = text.split("ty", 1)[0]
        n = _first_num(left)
        if n is not None:
            total += n * Decimal("1000000000")
            has = True
        right = text.split("ty", 1)[1]
        if "trieu" in right:
            n = _first_num(right)
            if n is not None:
                total += n * Decimal("1000000")
                has = True
        return int(total) if has else None

    if "trieu" in text:
        n = _first_num(text)
        return int(n * Decimal("1000000")) if n is not None else None

    if "nghin" in text or "ngan" in text:
        n = _first_num(text)
        return int(n * Decimal("1000")) if n is not None else None

    return None


def parse_posted_at(value):
    value = clean_raw(value)
    if not value:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def slugify(value):
    base = normalize_text(value)
    if not base:
        return None
    base = re.sub(r"[^a-z0-9\s-]", "", base)
    base = re.sub(r"\s+", "-", base).strip("-")
    return base or None


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
                    key = normalize_admin_name(new_name)
                    province_new.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
                if old_name:
                    key = normalize_admin_name(old_name)
                    province_old.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
            else:
                if new_name:
                    key = (parent_id, normalize_admin_name(new_name))
                    child_new.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
                if old_name:
                    key = (parent_id, normalize_admin_name(old_name))
                    child_old.setdefault(key, []).append((action_type, new_city_id, new_name, old_name))
    return province_new, province_old, child_new, child_old


def pick_best(candidates):
    if not candidates:
        return None
    return sorted(candidates, key=lambda x: (x[0] != 0, x[1]))[0]


def parse_location(diachi, province_new, province_old, child_new, child_old):
    diachi = clean_raw(diachi)
    if not diachi:
        return {
            "province_id": None,
            "ward_id": None,
            "street": None,
            "city_name_raw": None,
            "ward_name_raw": None,
            "address_case": None,
        }

    parts = [p.strip() for p in diachi.split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return {
            "province_id": None,
            "ward_id": None,
            "street": None,
            "city_name_raw": None,
            "ward_name_raw": None,
            "address_case": None,
        }

    city_name_raw = parts[-1]
    city_key = normalize_admin_name(city_name_raw)
    province_row = pick_best(province_new.get(city_key, [])) or pick_best(province_old.get(city_key, []))
    province_id = province_row[1] if province_row else None

    ward_name_raw = parts[-2] if len(parts) >= 2 else None
    ward_key = normalize_admin_name(ward_name_raw) if ward_name_raw else None
    ward_row = None
    if province_id and ward_key:
        ward_row = pick_best(child_new.get((province_id, ward_key), []))
        if not ward_row:
            ward_row = pick_best(child_old.get((province_id, ward_key), []))
    ward_id = ward_row[1] if ward_row else None

    street = None
    if len(parts) >= 3:
        street = clean_raw(parts[0])

    return {
        "province_id": province_id,
        "ward_id": ward_id,
        "street": street,
        "city_name_raw": city_name_raw,
        "ward_name_raw": ward_name_raw,
        "address_case": "new" if ward_row and any(x for x in child_new.get((province_id, ward_key), [])) else ("old" if ward_row else None),
    }


def build_project_merge_map(conn):
    project_map = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                alonhadat_project_name,
                duan_id,
                duan_ten
            FROM duan_alonhadat_duan_merge
            WHERE alonhadat_project_name IS NOT NULL
              AND alonhadat_project_name <> ''
              AND duan_id IS NOT NULL
            """
        )
        for row in cur.fetchall():
            raw_name = clean_project_name(row.get("alonhadat_project_name"))
            if not raw_name:
                continue
            key = normalize_text(raw_name)
            prev = project_map.get(key)
            if not prev or int(row["duan_id"]) > int(prev["project_id"]):
                project_map[key] = {
                    "project_id": int(row["duan_id"]),
                    "project_name": clean_raw(row.get("duan_ten")) or raw_name,
                }
    return project_map


def map_rule(loaibds, trade_type):
    loaibds = clean_raw(loaibds)
    trade_type = clean_raw(trade_type)
    if not loaibds or not trade_type:
        return None
    return PROPERTY_RULES.get((loaibds, trade_type))


def infer_type_text(trade_type):
    # Real source data shows 's' on sale rows and 'u' on rent rows.
    if trade_type == "s":
        return "Cần bán"
    if trade_type == "u":
        return "Cho thuê"
    return None


def infer_unit(trade_type):
    if trade_type == "u":
        return "thang"
    if trade_type == "s":
        return "md"
    return None


def infer_stratum_id(phaply):
    phaply = clean_raw(phaply)
    if not phaply:
        return None
    return STRATUM_RULES.get(phaply, 8)


def get_image_for_detail(cur, detail_id):
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


def build_payload(row, province_new, province_old, child_new, child_old, project_merge_map, cur):
    trade_type = row.get("trade_type")
    rule = map_rule(row.get("loaibds"), trade_type)
    if not rule:
        return None, "skip_type"

    loc = parse_location(row.get("diachi"), province_new, province_old, child_new, child_old)
    if not loc["province_id"] or not loc["ward_id"]:
        return None, "skip_region"

    raw_project_name = clean_project_name(row.get("thuocduan"))
    merged_project = None
    if raw_project_name:
        merged_project = project_merge_map.get(normalize_text(raw_project_name))
    payload = {
        "title": clean_raw(row.get("title")),
        "address": None,
        "posted_at": parse_posted_at(row.get("ngaydang")),
        "img": get_image_for_detail(cur, row["id"]),
        "price": parse_price_to_vnd(row.get("khoanggia")),
        "area": parse_area(row.get("dientich")),
        "description": clean_raw(row.get("mota")),
        "property_type": rule["property_type"],
        "type": infer_type_text(trade_type),
        "house_direction": clean_raw(row.get("huongnha")),
        "floors": parse_int(row.get("sotang")),
        "bathrooms": parse_int(row.get("sophongvesinh")),
        "road_width": parse_decimal(row.get("duongvao")),
        "living_rooms": None,
        "bedrooms": parse_int(row.get("sophongngu")),
        "legal_status": clean_raw(row.get("phaply")),
        "lat": None,
        "long": None,
        "broker_name": clean_raw(row.get("tenmoigioi")),
        "phone": clean_raw(row.get("sodienthoai")),
        "source": SOURCE_DOMAIN,
        "time_converted_at": datetime.now(),
        "source_post_id": clean_raw(row.get("matin")),
        "width": parse_decimal(row.get("chieungang")),
        "length": parse_decimal(row.get("chieudai")),
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
        "stratum_id": infer_stratum_id(row.get("phaply")),
        "cat_id": rule["cat_id"],
        "type_id": rule["type_id"],
        "unit": infer_unit(trade_type),
        "project_id": (merged_project or {}).get("project_id"),
        "uploaded_at": None,
        "_debug": {
            "loaibds": row.get("loaibds"),
            "trade_type_raw": trade_type,
            "city_name_raw": loc["city_name_raw"],
            "ward_name_raw": loc["ward_name_raw"],
            "address_case": loc["address_case"],
        },
    }
    return payload, None


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
        cur.execute("SHOW INDEX FROM scraped_details_flat WHERE Key_name='idx_sdf_alonhadat_datafull_conv'")
        if not cur.fetchone():
            cur.execute(
                "ALTER TABLE scraped_details_flat "
                "ADD INDEX idx_sdf_alonhadat_datafull_conv (domain, datafull_converted, matin)"
            )
    conn.commit()


def fetch_preview_rows(conn, limit, matin_list=None):
    sql = """
        SELECT *
        FROM scraped_details_flat
        WHERE domain = %s
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


def print_preview(row, payload, skip_reason=None):
    print("=" * 100)
    print(f"link_id={row['id']} matin={row.get('matin')} title={row.get('title')}")
    print(f"url={row.get('url')}")
    if skip_reason:
        print(f"skip_reason={skip_reason}")
        print(f"loaibds={row.get('loaibds')} | trade_type={row.get('trade_type')} | diachi={row.get('diachi')}")
        return
    debug = payload.pop("_debug", {})
    print("converted=")
    print(json.dumps(payload, ensure_ascii=False, default=str, indent=2))
    print(f"debug={json.dumps(debug, ensure_ascii=False)}")


INSERT_COLUMNS = [
    "title",
    "address",
    "posted_at",
    "img",
    "price",
    "area",
    "description",
    "property_type",
    "type",
    "house_direction",
    "floors",
    "bathrooms",
    "road_width",
    "living_rooms",
    "bedrooms",
    "legal_status",
    "lat",
    "`long`",
    "broker_name",
    "phone",
    "source",
    "time_converted_at",
    "source_post_id",
    "width",
    "length",
    "city",
    "district",
    "ward",
    "street",
    "province_id",
    "district_id",
    "ward_id",
    "street_id",
    "id_img",
    "project_name",
    "slug_name",
    "images_status",
    "stratum_id",
    "cat_id",
    "type_id",
    "unit",
    "project_id",
    "uploaded_at",
]


def insert_payload(cur, payload):
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    sql = f"""
        INSERT INTO data_full (
            {", ".join(INSERT_COLUMNS)}
        ) VALUES (
            {placeholders}
        )
    """
    values = [payload.get(col.replace("`", "")) for col in INSERT_COLUMNS]
    cur.execute(sql, values)
    return cur.lastrowid


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


def already_exists(cur, source_post_id):
    cur.execute(
        """
        SELECT id
        FROM data_full
        WHERE source = %s
          AND source_post_id = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (SOURCE_DOMAIN, source_post_id),
    )
    row = cur.fetchone()
    return row["id"] if row else None


def main():
    ap = argparse.ArgumentParser(description="Convert Alonhadat -> data_full (preview or insert)")
    ap.add_argument("--preview-limit", type=int, default=2)
    ap.add_argument("--matin", help="Comma-separated source_post_id/matin to preview")
    ap.add_argument("--insert", action="store_true", help="Insert into data_full")
    args = ap.parse_args()

    db = Database()
    conn = db.get_connection()
    ensure_datafull_converted_column(conn)
    province_new, province_old, child_new, child_old = build_location_maps(conn)
    project_merge_map = build_project_merge_map(conn)
    matin_list = [x.strip() for x in args.matin.split(",") if x.strip()] if args.matin else None
    rows = fetch_preview_rows(conn, args.preview_limit, matin_list=matin_list)
    with conn.cursor() as cur:
        for row in rows:
            payload, skip_reason = build_payload(row, province_new, province_old, child_new, child_old, project_merge_map, cur)
            print_preview(row, payload, skip_reason=skip_reason)
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
                continue

            new_id = insert_payload(cur, payload)
            mark_converted(cur, row["id"])
            conn.commit()
            print(f"inserted_data_full_id={new_id}")
    conn.close()


if __name__ == "__main__":
    main()
