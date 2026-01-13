import argparse
import json
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from craw.database import Database


def _parse_http_datetime(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _load_rows(file_path: str) -> List[Dict[str, Any]]:
    data = json.loads(Path(file_path).read_text(encoding="utf-8"))
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    return rows


def _build_row(item: Dict[str, Any], trade_type: str) -> Dict[str, Any]:
    return {
        "id": item.get("id"),
        "trade_type": trade_type,
        "title": item.get("title"),
        "slug": item.get("slug"),
        "category_real_estate": item.get("categoryRealEstate"),
        "address": item.get("address"),
        "location": item.get("location"),
        "search_district": item.get("searchDistrict"),
        "search_province": item.get("searchProvince"),
        "area": item.get("area"),
        "use_area": item.get("useArea"),
        "balcony_direction": item.get("balconyDirection"),
        "bathroom_range": item.get("bathroomRange"),
        "bedroom_range": item.get("bedroomRange"),
        "price": item.get("price"),
        "create_time": _parse_http_datetime(item.get("createTime")),
        "publish_time": _parse_http_datetime(item.get("publishTime")),
        "update_time": _parse_http_datetime(item.get("updateTime")),
        "media_images": json.dumps(item.get("mediaImages") or [], ensure_ascii=False, separators=(",", ":")),
    }


def upsert_rows(rows: List[Dict[str, Any]], trade_type: str, batch_size: int) -> int:
    if not rows:
        return 0
    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    conn = db.get_connection()
    sql = """
        INSERT INTO cenhomes_ads (
            id, trade_type, title, slug, category_real_estate, address, location,
            search_district, search_province, area, use_area, balcony_direction,
            bathroom_range, bedroom_range, price, create_time, publish_time,
            update_time, media_images
        ) VALUES (
            %(id)s, %(trade_type)s, %(title)s, %(slug)s, %(category_real_estate)s,
            %(address)s, %(location)s, %(search_district)s, %(search_province)s,
            %(area)s, %(use_area)s, %(balcony_direction)s, %(bathroom_range)s,
            %(bedroom_range)s, %(price)s, %(create_time)s, %(publish_time)s,
            %(update_time)s, %(media_images)s
        )
        ON DUPLICATE KEY UPDATE
            trade_type = VALUES(trade_type),
            title = VALUES(title),
            slug = VALUES(slug),
            category_real_estate = VALUES(category_real_estate),
            address = VALUES(address),
            location = VALUES(location),
            search_district = VALUES(search_district),
            search_province = VALUES(search_province),
            area = VALUES(area),
            use_area = VALUES(use_area),
            balcony_direction = VALUES(balcony_direction),
            bathroom_range = VALUES(bathroom_range),
            bedroom_range = VALUES(bedroom_range),
            price = VALUES(price),
            create_time = VALUES(create_time),
            publish_time = VALUES(publish_time),
            update_time = VALUES(update_time),
            media_images = VALUES(media_images),
            updated_at = CURRENT_TIMESTAMP
    """
    try:
        with conn.cursor() as cur:
            total = len(rows)
            inserted = 0
            for i in range(0, total, batch_size):
                chunk = rows[i:i + batch_size]
                cur.executemany(sql, chunk)
                conn.commit()
                inserted += len(chunk)
                print(f"Progress: {inserted}/{total}")
            return inserted
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to template JSON file")
    parser.add_argument("--trade-type", required=True, choices=["mua", "thue"], help="mua hoặc thue")
    parser.add_argument("--batch-size", type=int, default=300)
    args = parser.parse_args()

    rows_raw = _load_rows(args.file)
    if not rows_raw:
        print("No data rows found in file.")
        return 1
    rows = [_build_row(item, args.trade_type) for item in rows_raw]
    inserted = upsert_rows(rows, args.trade_type, args.batch_size)
    print(f"Upserted rows: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
