import argparse
import re
import unicodedata
from datetime import datetime

import pymysql

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

DOMAIN = "vinhome"

PREFIXES = [
    "tinh ",
    "thanh pho ",
    "tp. ",
    "tp ",
    "thi xa ",
    "tx. ",
    "tx ",
    "quan ",
    "huyen ",
    "thi tran ",
    "phuong ",
    "xa ",
]


PROJECT_ID_MAP = {
    "Vinhomes Grand Park": 1650,
    "Vinhomes Ocean Park": 2407,
    "Vinhomes Ocean Park 2": 3337,
    "Vinhomes Ocean Park 3": 3721,
    "Vinhomes Royal Island": 4430,
    "Vinhomes Golden City": 4503,
    "Vinhomes Global Gate": 4532,
}


def remove_accents(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.replace("đ", "d").replace("Đ", "D")


def strip_prefix(s: str) -> str:
    s = s.strip().lower()
    for p in PREFIXES:
        if s.startswith(p):
            s = s[len(p) :]
            break
    return s.strip()


def norm_city_title(name: str) -> str:
    if not name:
        return ""
    return strip_prefix(remove_accents(name)).lower()


def slug_name_from_url(url: str) -> str:
    if not url:
        return ""
    url = url.split("?", 1)[0].rstrip("/")
    return url.rsplit("/", 1)[-1]


def parse_price(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val)
    s = re.sub(r"[^0-9.]", "", s)
    return float(s) if s else None


def parse_address_parts(addr: str):
    parts = [p.strip() for p in (addr or "").split(",") if p.strip()]
    if len(parts) >= 3:
        ward = parts[-3]
        district = parts[-2]
        province = parts[-1]
    elif len(parts) == 2:
        ward = None
        district = parts[-2]
        province = parts[-1]
    elif len(parts) == 1:
        ward = None
        district = None
        province = parts[-1]
    else:
        ward = district = province = None
    return ward, district, province


def map_type_id(trade_type: str, loaihinh: str):
    if trade_type == "u":
        return 1
    if not loaihinh:
        return None
    t = loaihinh.strip().lower()
    if "căn hộ" in t:
        return 5
    if "nhà liền kề" in t:
        return 1
    if "shophouse" in t:
        return 1
    if "thương mại" in t:
        return 56
    if "biệt thự" in t:
        return 3
    return None


def main():
    parser = argparse.ArgumentParser(description="Convert Vinhomes scraped_details_flat -> data_no_full")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            limit_sql = f"LIMIT {int(args.limit)}" if args.limit and args.limit > 0 else ""
            cur.execute(
                f"""
                SELECT sdf.*, sdi.image_url AS img_url
                FROM scraped_details_flat sdf
                LEFT JOIN (
                    SELECT detail_id, MIN(id) AS mid
                    FROM scraped_detail_images
                    GROUP BY detail_id
                ) m ON m.detail_id = sdf.id
                LEFT JOIN scraped_detail_images sdi ON sdi.id = m.mid
                WHERE sdf.domain = %s
                {limit_sql}
                """,
                (DOMAIN,),
            )
            rows = cur.fetchall()

            # Preload location tables for fast matching
            cur.execute("SELECT city_id, city_parent_id, city_title FROM transaction_city_new WHERE city_title IS NOT NULL")
            all_city_new = cur.fetchall()

            cur.execute("SELECT old_city_id, new_city_id FROM transaction_city_merge")
            merge_map = {r["old_city_id"]: r["new_city_id"] for r in cur.fetchall()}

        # Build lookup maps
        provinces = {}
        districts_by_prov = {}
        wards_by_name = {}
        for r in all_city_new:
            cid = r["city_id"]
            pid = r["city_parent_id"]
            title = r["city_title"]
            key = norm_city_title(title)
            if pid == 0:
                provinces[key] = r
            if pid not in districts_by_prov:
                districts_by_prov[pid] = []
            districts_by_prov[pid].append(r)
            if key and key not in wards_by_name:
                wards_by_name[key] = r

        inserted = 0
        updated = 0
        batch_count = 0

        for row in rows:
            trade_type = row.get("trade_type")
            cat_id = 1 if trade_type == "s" else 3
            type_id = map_type_id(trade_type, row.get("loaihinh")) or 1
            unit = "VND" if trade_type == "s" else "thang"

            ward, district, province = parse_address_parts(row.get("diachi") or "")

            with conn.cursor() as cur:
                # match province/district/ward in transaction_city_new (preloaded)
                prov = provinces.get(norm_city_title(province)) if province else None
                dist = None
                if prov and district:
                    for r in districts_by_prov.get(prov["city_id"], []):
                        if norm_city_title(r["city_title"]) == norm_city_title(district):
                            dist = r
                            break
                ward_row = wards_by_name.get(norm_city_title(ward)) if ward else None

                new_prov = merge_map.get(prov["city_id"]) if prov else None
                new_ward = merge_map.get(ward_row["city_id"]) if ward_row else None

                source_post_id = str(row.get("matin") or "")
                slug_name = slug_name_from_url(row.get("url") or "")
                project_name = row.get("thuocduan")
                project_id = PROJECT_ID_MAP.get(project_name)

                price = parse_price(row.get("khoanggia"))
                area = parse_price(row.get("dientich"))

                posted_at = None
                ngaydang = row.get("ngaydang")
                if ngaydang:
                    try:
                        posted_at = datetime.fromisoformat(str(ngaydang))
                    except ValueError:
                        posted_at = datetime.utcnow()

                cur.execute(
                    """
                    SELECT id FROM data_no_full
                    WHERE source = %s AND source_post_id = %s
                    LIMIT 1
                    """,
                    (DOMAIN, source_post_id),
                )
                existing = cur.fetchone()

                payload = {
                    "title": row.get("title"),
                    "address": row.get("diachi"),
                    "posted_at": posted_at,
                    "img": row.get("img_url"),
                    "price": price,
                    "area": area,
                    "description": row.get("mota"),
                    "property_type": row.get("loaihinh"),
                    "type": trade_type,
                    "house_direction": row.get("huongnha"),
                    "bathrooms": row.get("sophongvesinh"),
                    "bedrooms": row.get("sophongngu"),
                    "lat": row.get("lat"),
                    "long": row.get("lng"),
                    "source": DOMAIN,
                    "source_post_id": source_post_id,
                    "project_name": project_name,
                    "slug_name": slug_name,
                    "province_id": new_prov,
                    "ward_id": new_ward,
                    "cat_id": cat_id,
                    "type_id": type_id,
                    "unit": unit,
                    "project_id": project_id,
                    "id_img": row.get("id"),
                    "time_converted_at": datetime.utcnow(),
                }

                if existing:
                    cur.execute(
                        """
                        UPDATE data_no_full
                        SET title=%(title)s,
                            address=%(address)s,
                            posted_at=%(posted_at)s,
                            img=%(img)s,
                            price=%(price)s,
                            area=%(area)s,
                            description=%(description)s,
                            property_type=%(property_type)s,
                            type=%(type)s,
                            house_direction=%(house_direction)s,
                            bathrooms=%(bathrooms)s,
                            bedrooms=%(bedrooms)s,
                            lat=%(lat)s,
                            `long`=%(long)s,
                            project_name=%(project_name)s,
                            slug_name=%(slug_name)s,
                            province_id=%(province_id)s,
                            ward_id=%(ward_id)s,
                            cat_id=%(cat_id)s,
                            type_id=%(type_id)s,
                            unit=%(unit)s,
                            project_id=%(project_id)s,
                            id_img=%(id_img)s,
                            time_converted_at=%(time_converted_at)s
                        WHERE id=%(id)s
                        """,
                        {**payload, "id": existing["id"]},
                    )
                    updated += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO data_no_full (
                            title, address, posted_at, img, price, area, description,
                            property_type, type, house_direction, bathrooms, bedrooms,
                            lat, `long`, source, source_post_id, project_name, slug_name,
                            province_id, ward_id, cat_id, type_id, unit, project_id, id_img, time_converted_at
                        ) VALUES (
                            %(title)s, %(address)s, %(posted_at)s, %(img)s, %(price)s, %(area)s, %(description)s,
                            %(property_type)s, %(type)s, %(house_direction)s, %(bathrooms)s, %(bedrooms)s,
                            %(lat)s, %(long)s, %(source)s, %(source_post_id)s, %(project_name)s, %(slug_name)s,
                            %(province_id)s, %(ward_id)s, %(cat_id)s, %(type_id)s, %(unit)s, %(project_id)s, %(id_img)s, %(time_converted_at)s
                        )
                        """,
                        payload,
                    )
                    inserted += 1

            batch_count += 1
            if batch_count >= 200:
                conn.commit()
                batch_count = 0

        conn.commit()

        print(f"DONE inserted={inserted} updated={updated} total={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
