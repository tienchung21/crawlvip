#!/usr/bin/env python3
"""
Crawl Meeyland location hierarchy into location_meeland.

Sources:
- POST /v1/locations/cities
- POST /v1/locations/districts
- POST /v1/locations/wards
- POST /v1/locations/streets
"""

from __future__ import annotations

import argparse
import time
from typing import Any, Dict, List, Optional, Tuple

from curl_cffi import requests as cffi_requests

from database import Database


API_BASE = "https://api5.meeyland.com/v1/locations"
DEFAULT_LIMIT = 20
DEFAULT_DELAY = 0.2
DEFAULT_MAX_RETRIES = 3

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi",
    "content-type": "application/json",
    "origin": "https://meeyland.com",
    "referer": "https://meeyland.com/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
    ),
    "x-tenant": "bWVleWxhbmQ=",
}


def log(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Crawl Meeyland locations to location_meeland")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="API page size")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Sleep between requests (seconds)")
    ap.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Retries per request")
    ap.add_argument("--keyword", type=str, default="", help="Keyword filter")
    ap.add_argument("--rebuild", action="store_true", help="Drop and recreate location_meeland before crawling")
    ap.add_argument("--dry-run", action="store_true", help="No DB writes")
    return ap.parse_args()


def make_session() -> Any:
    s = cffi_requests.Session()
    s.headers.update(HEADERS)
    return s


def ensure_table(db: Database, rebuild: bool = False) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if rebuild:
            cur.execute("DROP TABLE IF EXISTS location_meeland")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS location_meeland (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                level_type VARCHAR(20) NOT NULL,
                meey_id VARCHAR(64) NOT NULL,
                code VARCHAR(255) DEFAULT NULL,
                slug VARCHAR(255) DEFAULT NULL,
                postal_code VARCHAR(64) DEFAULT NULL,
                city_meey_id VARCHAR(64) DEFAULT NULL,
                district_meey_id VARCHAR(64) DEFAULT NULL,
                name VARCHAR(255) DEFAULT NULL,
                name_vi VARCHAR(255) DEFAULT NULL,
                prefix_vi VARCHAR(100) DEFAULT NULL,
                lat VARCHAR(64) DEFAULT NULL,
                lng VARCHAR(64) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_level_meey (level_type, meey_id),
                KEY idx_level (level_type),
                KEY idx_city (city_meey_id),
                KEY idx_district (district_meey_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def vi_translation(obj: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    arr = obj.get("translation") or []
    if isinstance(arr, list):
        for item in arr:
            if isinstance(item, dict) and item.get("languageCode") == "vi":
                return item.get("name"), item.get("prefix")
    return None, None


def slug_to_title(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    parts = [p for p in str(value).strip().split("-") if p]
    if not parts:
        return None
    return " ".join(p.capitalize() for p in parts)


def derive_name(level_type: str, obj: Dict[str, Any], name_vi: Optional[str]) -> Optional[str]:
    if name_vi:
        return name_vi

    slug_name = slug_to_title(obj.get("slug"))
    if slug_name:
        return slug_name

    code = obj.get("code")
    if isinstance(code, str):
        if level_type == "city":
            cleaned = code
            for pref in ("thanh-pho-", "tinh-"):
                if cleaned.startswith(pref):
                    cleaned = cleaned[len(pref):]
                    break
            code_name = slug_to_title(cleaned)
            if code_name:
                return code_name
        code_name = slug_to_title(code)
        if code_name:
            return code_name
    return None


def lat_lng(obj: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    loc = obj.get("location") or {}
    coords = loc.get("coordinates") or []
    if isinstance(coords, list) and len(coords) >= 2:
        return str(coords[1]), str(coords[0])
    return None, None


def post_json(
    session: Any,
    endpoint: str,
    payload: Dict[str, Any],
    max_retries: int,
) -> Dict[str, Any]:
    url = f"{API_BASE}/{endpoint}"
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.post(url, json=payload, impersonate="chrome136", timeout=30)
            r.raise_for_status()
            obj = r.json()
            if not isinstance(obj, dict):
                raise RuntimeError("invalid response object")
            return obj
        except Exception as exc:
            last_err = exc
            if attempt >= max_retries:
                break
            time.sleep(min(2 * attempt, 6))
    raise RuntimeError(f"request_failed endpoint={endpoint}: {last_err}")


def upsert_location(
    db: Database,
    level_type: str,
    obj: Dict[str, Any],
    city_meey_id: Optional[str],
    district_meey_id: Optional[str],
) -> None:
    name_vi, prefix_vi = vi_translation(obj)
    name = derive_name(level_type, obj, name_vi)
    lat, lng = lat_lng(obj)
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO location_meeland
            (level_type, meey_id, code, slug, postal_code, city_meey_id, district_meey_id, name, name_vi, prefix_vi, lat, lng)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                code = VALUES(code),
                slug = VALUES(slug),
                postal_code = VALUES(postal_code),
                city_meey_id = VALUES(city_meey_id),
                district_meey_id = VALUES(district_meey_id),
                name = VALUES(name),
                name_vi = VALUES(name_vi),
                prefix_vi = VALUES(prefix_vi),
                lat = VALUES(lat),
                lng = VALUES(lng)
            """,
            (
                level_type,
                str(obj.get("_id") or ""),
                obj.get("code"),
                obj.get("slug"),
                obj.get("postalCode"),
                city_meey_id,
                district_meey_id,
                name,
                name_vi,
                prefix_vi,
                lat,
                lng,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def crawl_paged(
    session: Any,
    endpoint: str,
    base_payload: Dict[str, Any],
    limit: int,
    max_retries: int,
    delay: float,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        payload = dict(base_payload)
        payload["limit"] = limit
        payload["page"] = page
        obj = post_json(session, endpoint, payload, max_retries=max_retries)
        data = obj.get("data") or {}
        rows = data.get("results") or []
        total_pages = int(data.get("totalPages") or 0)
        all_rows.extend(rows)
        log(f"[{endpoint.upper()}] page={page}/{total_pages} items={len(rows)}")
        if page >= total_pages or total_pages == 0:
            break
        page += 1
        if delay > 0:
            time.sleep(delay)
    return all_rows


def main() -> int:
    args = parse_args()
    db = Database()
    ensure_table(db, rebuild=args.rebuild)
    session = make_session()

    cities = crawl_paged(
        session=session,
        endpoint="cities",
        base_payload={"keyword": args.keyword},
        limit=args.limit,
        max_retries=args.max_retries,
        delay=args.delay,
    )
    log(f"[CITIES] total={len(cities)}")

    inserted_cities = 0
    inserted_districts = 0
    inserted_wards = 0
    inserted_streets = 0

    for city in cities:
        city_id = str(city.get("_id") or "")
        if not city_id:
            continue
        if not args.dry_run:
            upsert_location(db, "city", city, city_meey_id=city_id, district_meey_id=None)
        inserted_cities += 1

        districts = crawl_paged(
            session=session,
            endpoint="districts",
            base_payload={"city": city_id, "keyword": args.keyword},
            limit=args.limit,
            max_retries=args.max_retries,
            delay=args.delay,
        )
        log(f"[DISTRICTS] city={city_id} total={len(districts)}")

        for district in districts:
            district_id = str(district.get("_id") or "")
            if not district_id:
                continue
            if not args.dry_run:
                upsert_location(db, "district", district, city_meey_id=city_id, district_meey_id=district_id)
            inserted_districts += 1

            wards = crawl_paged(
                session=session,
                endpoint="wards",
                base_payload={"district": district_id, "keyword": args.keyword},
                limit=args.limit,
                max_retries=args.max_retries,
                delay=args.delay,
            )
            streets = crawl_paged(
                session=session,
                endpoint="streets",
                base_payload={"district": district_id, "keyword": args.keyword},
                limit=args.limit,
                max_retries=args.max_retries,
                delay=args.delay,
            )

            for ward in wards:
                ward_id = str(ward.get("_id") or "")
                if not ward_id:
                    continue
                if not args.dry_run:
                    upsert_location(db, "ward", ward, city_meey_id=city_id, district_meey_id=district_id)
                inserted_wards += 1

            for street in streets:
                street_id = str(street.get("_id") or "")
                if not street_id:
                    continue
                if not args.dry_run:
                    upsert_location(db, "street", street, city_meey_id=city_id, district_meey_id=district_id)
                inserted_streets += 1

            log(
                f"[DISTRICT_DONE] city={city_id} district={district_id} wards={len(wards)} streets={len(streets)} "
                f"totals(city/district/ward/street)={inserted_cities}/{inserted_districts}/{inserted_wards}/{inserted_streets}"
            )

    log(
        f"[DONE] cities={inserted_cities} districts={inserted_districts} wards={inserted_wards} streets={inserted_streets} "
        f"dry_run={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
