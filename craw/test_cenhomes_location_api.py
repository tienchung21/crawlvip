
import argparse
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

from database import Database

BASE = "https://api-v3.cenhomes.vn/location/v1"


def _headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://cenhomes.vn",
        "referer": "https://cenhomes.vn/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
    }


def _slugify(value: str) -> str:
    if not value:
        return ""
    value = value.lower()
    value = re.sub(r"[^a-z0-9\s-]", "", value)
    value = re.sub(r"\s+", "-", value).strip("-")
    return value


def _parse_center(center: Optional[List[float]]) -> Tuple[Optional[float], Optional[float]]:
    if not center or len(center) < 2:
        return None, None
    lng, lat = center[0], center[1]
    return lat, lng


def fetch_provinces(session: requests.Session) -> List[Dict]:
    resp = session.post(f"{BASE}/provinces", json={}, headers=_headers(), timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("data") or []


def fetch_districts(session: requests.Session, province_id: int) -> List[Dict]:
    resp = session.post(
        f"{BASE}/districts",
        json={"limit": 100, "skip": 0, "provinceId": province_id},
        headers=_headers(),
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("data") or []


def fetch_wards(session: requests.Session, province_id: int, district_id: int) -> List[Dict]:
    resp = session.post(
        f"{BASE}/wards",
        json={"limit": 100, "skip": 0, "districtIds": [district_id], "provinceId": province_id},
        headers=_headers(),
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("data") or []


def ensure_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cenhomes_locations (
                id BIGINT NOT NULL,
                level VARCHAR(16) NOT NULL,
                province_id BIGINT NULL,
                district_id BIGINT NULL,
                parent_id BIGINT NULL,
                title VARCHAR(255) NULL,
                name VARCHAR(255) NULL,
                english_name VARCHAR(255) NULL,
                slug VARCHAR(255) NULL,
                cenhomes_url VARCHAR(255) NULL,
                lat DOUBLE NULL,
                lng DOUBLE NULL,
                raw_json JSON NULL,
                updated_at DATETIME NULL,
                PRIMARY KEY (id, level),
                INDEX idx_level (level),
                INDEX idx_province_id (province_id),
                INDEX idx_district_id (district_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def upsert_rows(conn, rows: List[List]):
    if not rows:
        return 0
    columns = [
        "id", "level", "province_id", "district_id", "parent_id",
        "title", "name", "english_name", "slug", "cenhomes_url",
        "lat", "lng", "raw_json", "updated_at"
    ]
    cols_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_cols = [c for c in columns if c not in ("id", "level")]
    update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols])
    sql = f"INSERT INTO `cenhomes_locations` ({cols_sql}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_sql}"
    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()


def build_row(level: str, item: Dict, province_id: Optional[int], district_id: Optional[int], parent_id: Optional[int], now: str) -> List:
    english_name = item.get("englishName") or ""
    slug = _slugify(english_name)
    cenhomes_url = f"https://cenhomes.vn/{slug}" if slug else None
    lat, lng = _parse_center(item.get("locationCenter"))
    return [
        item.get("id"),
        level,
        province_id,
        district_id,
        parent_id,
        item.get("title"),
        item.get("name"),
        english_name,
        slug,
        cenhomes_url,
        lat,
        lng,
        json.dumps(item, ensure_ascii=False, separators=(",", ":")),
        now,
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep", type=float, default=0.2, help="Delay between API calls")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-user", default="root")
    parser.add_argument("--db-pass", default="")
    parser.add_argument("--db-name", default="craw_db")
    parser.add_argument("--db-port", type=int, default=3306)
    args = parser.parse_args()

    db = Database(
        host=args.db_host,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        port=args.db_port,
    )
    ensure_table(db)

    session = requests.Session()
    provinces = fetch_provinces(session)
    print(f"Provinces: {len(provinces)}")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = db.get_connection()
    try:
        rows = []
        for p in provinces:
            province_id = p.get("id")
            rows.append(build_row("province", p, province_id, None, None, now))

            if not province_id:
                continue
            districts = fetch_districts(session, province_id)
            print(f"Province {province_id} districts: {len(districts)}")
            time.sleep(args.sleep)
            for d in districts:
                district_id = d.get("id")
                rows.append(build_row("district", d, province_id, district_id, province_id, now))
                if not district_id:
                    continue
                wards = fetch_wards(session, province_id, district_id)
                print(f"  District {district_id} wards: {len(wards)}")
                time.sleep(args.sleep)
                for w in wards:
                    rows.append(build_row("ward", w, province_id, district_id, district_id, now))

        upsert_rows(conn, rows)
    finally:
        conn.close()

    print("Done")


if __name__ == "__main__":
    main()
