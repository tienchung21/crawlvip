#!/usr/bin/env python3
"""
MeeyMap search crawler -> scraped_details_flat.

Phase 1:
- split Vietnam into 10 root horizontal bands
- recursively split any band with totalResults > split_threshold
- crawl each leaf band with a fixed limit
- save rows into scraped_details_flat
- write checkpoint/log/split-log for resume and audit
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from curl_cffi import requests as cffi_requests
except Exception:
    cffi_requests = None

from database import Database


API_URL = "https://api5.meeyland.com/v1/articles/search"
DOMAIN = "meeyland.com"
DEFAULT_LIMIT = 200
DEFAULT_DELAY = 3.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_SPLIT_THRESHOLD = 20000

AUTH_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://meeyland.com",
    "referer": "https://meeyland.com/",
    "x-tenant": "bWVleWxhbmQ=",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
    ),
}

MIN_LNG = 102.0
MAX_LNG = 109.9
MAX_LAT = 23.5
MIN_LAT = 8.3
ROOT_PARTS = 10


def log(message: str) -> None:
    print(message, flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl MeeyMap search API into scraped_details_flat")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Page size, max 500")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Sleep seconds between page requests")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max retries for a failed page")
    parser.add_argument("--split-threshold", type=int, default=DEFAULT_SPLIT_THRESHOLD, help="Split recursively if totalResults > threshold")
    parser.add_argument("--start-part", type=str, default="1", help="Start from this part label, e.g. 2 or 2.2.1")
    parser.add_argument("--start-page", type=int, default=1, help="Start from this page inside start-part")
    parser.add_argument("--max-parts", type=int, default=0, help="Optional cap on root parts for testing")
    parser.add_argument("--max-pages-per-part", type=int, default=0, help="Optional cap on pages per leaf part")
    parser.add_argument(
        "--dup-stop-threshold",
        type=int,
        default=120,
        help="Stop current leaf-part when duplicate items (already in DB / same run) exceed this threshold; 0 disables",
    )
    parser.add_argument("--category", type=str, default="", help="Optional category _id filter (e.g. cho_thue)")
    parser.add_argument("--zoom", type=str, default="", help="Optional zoom value to pass to API")
    parser.add_argument("--fake-coordinates", action="store_true", help="Send fakeCoordinates=true in payload")
    parser.add_argument("--api-url", type=str, default=API_URL, help="Search API URL")
    parser.add_argument("--domain", type=str, default=DOMAIN, help="Domain value stored in scraped_details_flat")
    parser.add_argument("--origin", type=str, default="", help="Override Origin header")
    parser.add_argument("--referer", type=str, default="", help="Override Referer header")
    parser.add_argument("--x-tenant", type=str, default="", help="Optional x-tenant header")
    parser.add_argument("--auth-token", type=str, default="", help="Override authorization token (with or without 'Bearer ')")
    parser.add_argument("--expire-token", type=str, default="", help="Override expiretoken header")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and map only, no DB writes")
    parser.add_argument(
        "--checkpoint-file",
        default="/home/chungnt/crawlvip/craw/logs/meeymap_search_checkpoint.json",
        help="Checkpoint JSON path",
    )
    parser.add_argument(
        "--log-file",
        default="/home/chungnt/crawlvip/craw/logs/meeymap_search.log",
        help="Plain text log path",
    )
    parser.add_argument(
        "--log-jsonl",
        default="/home/chungnt/crawlvip/craw/logs/meeymap_search.jsonl",
        help="JSONL event log path",
    )
    parser.add_argument(
        "--split-log-file",
        default="/home/chungnt/crawlvip/craw/logs/meeymap_search_split.log",
        help="Recursive split log path",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint if available")
    parser.add_argument(
        "--use-location-table",
        action="store_true",
        help="Use location table (city/district[/ward]) instead of map bounds split",
    )
    parser.add_argument(
        "--location-table",
        type=str,
        default="location_meeland",
        help="Location table name (default: location_meeland)",
    )
    parser.add_argument(
        "--location-level",
        type=str,
        choices=["city", "district", "ward"],
        default="district",
        help="Crawl granularity when using --use-location-table",
    )
    parser.add_argument(
        "--auto-split-threshold",
        type=int,
        default=20000,
        help="Hard threshold for oversized buckets (default 20000, for logging/safety)",
    )
    parser.add_argument(
        "--city-split-threshold",
        type=int,
        default=5000,
        help="When using location table: split city to district if city totalResults exceeds this threshold",
    )
    parser.add_argument(
        "--district-split-threshold",
        type=int,
        default=5000,
        help="When using location table: split district to ward if district totalResults exceeds this threshold",
    )
    parser.add_argument(
        "--location-city-meey-id",
        type=str,
        default="",
        help="Optional filter by city_meey_id in location table",
    )
    parser.add_argument(
        "--location-district-meey-id",
        type=str,
        default="",
        help="Optional filter by district_meey_id in location table (mainly for ward mode)",
    )
    parser.add_argument(
        "--worker-index",
        type=int,
        default=1,
        help="1-based worker index when splitting location parts by city",
    )
    parser.add_argument(
        "--worker-total",
        type=int,
        default=1,
        help="Total workers when splitting location parts by city",
    )
    parser.add_argument(
        "--refresh-existing-contact",
        action="store_true",
        help="When duplicate rows exist, update tenmoigioi/sodienthoai instead of skipping",
    )
    return parser.parse_args()


def ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def write_line(path: str, line: str) -> None:
    ensure_parent_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def log_both(args: argparse.Namespace, message: str) -> None:
    log(message)
    write_line(args.log_file, message)


def log_split(args: argparse.Namespace, message: str) -> None:
    log(message)
    write_line(args.split_log_file, message)


def log_event(args: argparse.Namespace, payload: Dict[str, Any]) -> None:
    ensure_parent_dir(args.log_jsonl)
    with open(args.log_jsonl, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def save_checkpoint(args: argparse.Namespace, payload: Dict[str, Any]) -> None:
    ensure_parent_dir(args.checkpoint_file)
    with open(args.checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_checkpoint(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_root_bounds(part_index: int) -> List[List[float]]:
    step = (MAX_LAT - MIN_LAT) / ROOT_PARTS
    top = MAX_LAT - ((part_index - 1) * step)
    bottom = MAX_LAT - (part_index * step)
    return [
        [MIN_LNG, round(top, 6)],
        [MAX_LNG, round(top, 6)],
        [MAX_LNG, round(bottom, 6)],
        [MIN_LNG, round(bottom, 6)],
    ]


def split_bounds_vertical(bounds: List[List[float]]) -> Tuple[List[List[float]], List[List[float]]]:
    top = bounds[0][1]
    bottom = bounds[2][1]
    mid = round((top + bottom) / 2, 6)
    upper = [
        [bounds[0][0], top],
        [bounds[1][0], top],
        [bounds[2][0], mid],
        [bounds[3][0], mid],
    ]
    lower = [
        [bounds[0][0], mid],
        [bounds[1][0], mid],
        [bounds[2][0], bottom],
        [bounds[3][0], bottom],
    ]
    return upper, lower


def build_payload(bounds: List[List[float]], limit: int, page: int, args: argparse.Namespace) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"bounds": bounds, "typeOfHouses": [], "limit": limit, "page": page}
    if args.category:
        payload["category"] = args.category
    if args.zoom:
        payload["zoom"] = args.zoom
    if args.fake_coordinates:
        payload["fakeCoordinates"] = True
    return payload


def build_payload_locations(
    city_meey_id: str,
    district_meey_id: Optional[str],
    ward_meey_id: Optional[str],
    limit: int,
    page: int,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    loc: Dict[str, Any] = {"city": city_meey_id}
    if district_meey_id:
        loc["district"] = district_meey_id
    if ward_meey_id:
        loc["wards"] = [ward_meey_id]

    payload: Dict[str, Any] = {
        "locations": [loc],
        "typeOfHouses": [],
        "limit": limit,
        "page": page,
    }
    if args.category:
        payload["category"] = args.category
    if args.zoom:
        payload["zoom"] = args.zoom
    if args.fake_coordinates:
        payload["fakeCoordinates"] = True
    return payload


def make_session(args: argparse.Namespace):
    if cffi_requests is None:
        raise RuntimeError("curl_cffi is required for meeymap_search_crawler.py")
    headers = dict(AUTH_HEADERS)
    if args.origin:
        headers["origin"] = args.origin
    if args.referer:
        headers["referer"] = args.referer
    if args.x_tenant:
        headers["x-tenant"] = args.x_tenant
    session = cffi_requests.Session()
    session.headers.update(headers)
    return session


def apply_auth_overrides(args: argparse.Namespace) -> None:
    if args.auth_token:
        token = args.auth_token.strip()
        if token and not token.lower().startswith("bearer "):
            token = f"Bearer {token}"
        AUTH_HEADERS["authorization"] = token
    if args.expire_token:
        AUTH_HEADERS["expiretoken"] = args.expire_token.strip()


def map_search_item(item: Dict[str, Any], domain: str) -> Dict[str, Any]:
    created_by = item.get("createdBy") or {}
    contact = item.get("contact") or {}
    city = item.get("city") or {}
    district = item.get("district") or {}
    category = item.get("category") or {}
    type_of_house = item.get("typeOfHouse") or {}
    price_label = item.get("priceLabel") or {}
    category_value = category.get("value")
    # Prefer numeric price fields from API payload.
    # - sale: totalPrice
    # - rent: totalRentalPrice
    raw_price = item.get("totalPrice")
    raw_rent = item.get("totalRentalPrice")
    if raw_rent is not None:
        price_value = str(raw_rent)
    elif raw_price is not None:
        price_value = str(raw_price)
    else:
        price_value = price_label.get("totalRentalPrice") or price_label.get("totalPrice")
    contact_name = (contact.get("fullName") or "").strip()
    created_by_name = (created_by.get("name") or "").strip()
    broker_name = contact_name or created_by_name or None

    phone_value = None
    phones = contact.get("phones")
    if isinstance(phones, list):
        for p in phones:
            if p is not None and str(p).strip():
                phone_value = str(p).strip()
                break
    if not phone_value:
        cb_phone = created_by.get("phone")
        if cb_phone is not None and str(cb_phone).strip():
            phone_value = str(cb_phone).strip()

    return {
        "url": item.get("url"),
        "domain": domain,
        "title": item.get("title"),
        "img_count": None,
        "mota": None,
        "khoanggia": price_value,
        "dientich": item.get("area"),
        "sophongngu": None,
        "sophongvesinh": None,
        "huongnha": None,
        "huongbancong": None,
        "mattien": None,
        "duongvao": None,
        "sotang": None,
        "loaihinhnhao": None,
        "dientichsudung": None,
        "gia_m2": None,
        "gia_mn": None,
        "dacdiemnhadat": None,
        "chieungang": item.get("facade"),
        "chieudai": item.get("depth"),
        "phaply": None,
        "noithat": None,
        "thuocduan": None,
        "trangthaiduan": None,
        "tenmoigioi": broker_name,
        "sodienthoai": phone_value,
        "map": None,
        "matin": str(item.get("code")) if item.get("code") is not None else None,
        "loaitin": None,
        "ngayhethan": None,
        "ngaydang": item.get("createdAt"),
        "diachi": None,
        "street_ext": None,
        "ward_ext": None,
        "district_ext": str(district.get("code")) if district.get("code") is not None else None,
        "city_ext": str(city.get("code")) if city.get("code") is not None else None,
        "city_code": None,
        "district_id": None,
        "ward_id": None,
        "street_id": None,
        "lat": None,
        "lng": None,
        "loaibds": None,
        "loaihinh": type_of_house.get("value"),
        "trade_type": "s" if category_value == "mua_ban" else "u",
    }


def load_existing_keys(db: Database, domain: str, matins: List[str], urls: List[str]) -> Tuple[set, set]:
    existing_matins: set = set()
    existing_urls: set = set()
    if not matins and not urls:
        return existing_matins, existing_urls

    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if matins:
            uniq_matins = list(dict.fromkeys([m for m in matins if m]))
            if uniq_matins:
                placeholders = ",".join(["%s"] * len(uniq_matins))
                cur.execute(
                    f"""
                    SELECT matin
                    FROM scraped_details_flat
                    WHERE domain = %s AND matin IN ({placeholders})
                    """,
                    [domain] + uniq_matins,
                )
                for row in cur.fetchall():
                    val = row.get("matin") if isinstance(row, dict) else row[0]
                    if val is not None:
                        existing_matins.add(str(val))

        if urls:
            uniq_urls = list(dict.fromkeys([u for u in urls if u]))
            if uniq_urls:
                placeholders = ",".join(["%s"] * len(uniq_urls))
                cur.execute(
                    f"""
                    SELECT url
                    FROM scraped_details_flat
                    WHERE domain = %s AND url IN ({placeholders})
                    """,
                    [domain] + uniq_urls,
                )
                for row in cur.fetchall():
                    val = row.get("url") if isinstance(row, dict) else row[0]
                    if val:
                        existing_urls.add(val)
    finally:
        cur.close()
        conn.close()

    return existing_matins, existing_urls


def refresh_contact_for_existing(db: Database, domain: str, mapped: Dict[str, Any]) -> int:
    matin = mapped.get("matin")
    url = mapped.get("url")
    if not matin and not url:
        return 0
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if matin:
            cur.execute(
                """
                UPDATE scraped_details_flat
                SET tenmoigioi = %s, sodienthoai = %s
                WHERE domain = %s AND matin = %s
                """,
                (mapped.get("tenmoigioi"), mapped.get("sodienthoai"), domain, str(matin)),
            )
        else:
            cur.execute(
                """
                UPDATE scraped_details_flat
                SET tenmoigioi = %s, sodienthoai = %s
                WHERE domain = %s AND url = %s
                """,
                (mapped.get("tenmoigioi"), mapped.get("sodienthoai"), domain, url),
            )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        cur.close()
        conn.close()


def split_parts_by_worker(parts: List[Dict[str, Any]], worker_index: int, worker_total: int) -> List[Dict[str, Any]]:
    if worker_total <= 1:
        return parts
    if worker_index < 1 or worker_index > worker_total:
        raise SystemExit("--worker-index must be in range 1..worker-total")

    ordered_city_ids: List[str] = []
    city_to_parts: Dict[str, List[Dict[str, Any]]] = {}
    for p in parts:
        city_id = str(p.get("city_meey_id") or "")
        if not city_id:
            continue
        if city_id not in city_to_parts:
            city_to_parts[city_id] = []
            ordered_city_ids.append(city_id)
        city_to_parts[city_id].append(p)

    if not ordered_city_ids:
        return parts

    n = len(ordered_city_ids)
    chunk = (n + worker_total - 1) // worker_total
    start = (worker_index - 1) * chunk
    end = min(start + chunk, n)
    if start >= n:
        return []
    selected_city_ids = ordered_city_ids[start:end]

    out: List[Dict[str, Any]] = []
    for city_id in selected_city_ids:
        out.extend(city_to_parts.get(city_id, []))
    return out


def extract_page_data(obj: Dict[str, Any]) -> Tuple[int, int, List[Dict[str, Any]]]:
    data = obj.get("data")
    if isinstance(data, list):
        if data and isinstance(data[0], dict) and data[0].get("key") == "limit":
            raise RuntimeError(data[0].get("message") or "invalid limit")
        raise RuntimeError("unexpected list response")
    data = data or {}
    return int(data.get("totalResults") or 0), int(data.get("totalPages") or 0), data.get("results") or []


def fetch_page(
    session: Any,
    bounds: List[List[float]],
    limit: int,
    page: int,
    args: argparse.Namespace,
    part_label: str,
) -> Tuple[int, Dict[str, Any]]:
    payload = build_payload(bounds, limit, page, args)
    last_error: Optional[Exception] = None
    for attempt in range(1, args.max_retries + 1):
        try:
            response = session.post(args.api_url, json=payload, impersonate="chrome136", timeout=30)
            response.raise_for_status()
            return response.status_code, response.json()
        except Exception as exc:
            last_error = exc
            if attempt >= args.max_retries:
                break
            sleep_s = min(2 * attempt, 10)
            log_both(
                args,
                f"[RETRY] part={part_label} page={page} attempt={attempt}/{args.max_retries} "
                f"sleep={sleep_s}s error={str(exc)[:200]}",
            )
            log_event(
                args,
                {
                    "event": "retry",
                    "part": part_label,
                    "page": page,
                    "attempt": attempt,
                    "max_retries": args.max_retries,
                    "bounds": bounds,
                    "error": str(exc),
                    "sleep_s": sleep_s,
                    "updated_at": utc_now(),
                },
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"fetch_page_failed part={part_label} page={page}: {last_error}")


def fetch_page_locations(
    session: Any,
    city_meey_id: str,
    district_meey_id: Optional[str],
    ward_meey_id: Optional[str],
    limit: int,
    page: int,
    args: argparse.Namespace,
    part_label: str,
) -> Tuple[int, Dict[str, Any]]:
    payload = build_payload_locations(city_meey_id, district_meey_id, ward_meey_id, limit, page, args)
    last_error: Optional[Exception] = None
    for attempt in range(1, args.max_retries + 1):
        try:
            response = session.post(args.api_url, json=payload, impersonate="chrome136", timeout=30)
            response.raise_for_status()
            return response.status_code, response.json()
        except Exception as exc:
            last_error = exc
            if attempt >= args.max_retries:
                break
            sleep_s = min(2 * attempt, 10)
            log_both(
                args,
                f"[RETRY] part={part_label} page={page} attempt={attempt}/{args.max_retries} "
                f"sleep={sleep_s}s error={str(exc)[:200]}",
            )
            log_event(
                args,
                {
                    "event": "retry",
                    "part": part_label,
                    "page": page,
                    "city_meey_id": city_meey_id,
                    "district_meey_id": district_meey_id,
                    "ward_meey_id": ward_meey_id,
                    "attempt": attempt,
                    "max_retries": args.max_retries,
                    "error": str(exc),
                    "sleep_s": sleep_s,
                    "updated_at": utc_now(),
                },
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"fetch_page_locations_failed part={part_label} page={page}: {last_error}")


def resolve_location_table(db: Database, preferred_table: str) -> str:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        table_name = preferred_table
        cur.execute("SHOW TABLES LIKE %s", (table_name,))
        exists = cur.fetchone()
        if not exists and table_name == "location_meeyland":
            cur.execute("SHOW TABLES LIKE 'location_meeland'")
            if cur.fetchone():
                table_name = "location_meeland"
        elif not exists and table_name == "location_meeland":
            cur.execute("SHOW TABLES LIKE 'location_meeyland'")
            if cur.fetchone():
                table_name = "location_meeyland"
        if not exists and table_name == preferred_table:
            raise RuntimeError(f"Location table not found: {preferred_table}")
        return table_name
    finally:
        cur.close()
        conn.close()


def fetch_location_rows(
    db: Database,
    table_name: str,
    level_type: str,
    city_meey_id: str = "",
    district_meey_id: str = "",
) -> List[Dict[str, Any]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        where = ["level_type = %s"]
        params: List[Any] = [level_type]
        if level_type != "city" and city_meey_id:
            where.append("city_meey_id = %s")
            params.append(city_meey_id)
        if level_type == "ward" and district_meey_id:
            where.append("district_meey_id = %s")
            params.append(district_meey_id)
        cur.execute(
            f"""
            SELECT meey_id, city_meey_id, district_meey_id, name, name_vi
            FROM {table_name}
            WHERE {' AND '.join(where)}
            ORDER BY city_meey_id, district_meey_id, meey_id
            """,
            params,
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def probe_total_results(
    session: Any,
    args: argparse.Namespace,
    city_meey_id: str,
    district_meey_id: Optional[str],
    ward_meey_id: Optional[str],
    part_label: str,
) -> Tuple[int, int]:
    _, obj = fetch_page_locations(
        session=session,
        city_meey_id=city_meey_id,
        district_meey_id=district_meey_id,
        ward_meey_id=ward_meey_id,
        limit=min(args.limit, 10),
        page=1,
        args=args,
        part_label=part_label,
    )
    total_results, total_pages, _ = extract_page_data(obj)
    return total_results, total_pages


def load_location_parts(db: Database, session: Any, args: argparse.Namespace) -> List[Dict[str, Any]]:
    table_name = resolve_location_table(db, args.location_table)

    if args.auto_split_threshold <= 0:
        rows = fetch_location_rows(
            db=db,
            table_name=table_name,
            level_type=args.location_level,
            city_meey_id=args.location_city_meey_id,
            district_meey_id=args.location_district_meey_id,
        )
        parts: List[Dict[str, Any]] = []
        for idx, row in enumerate(rows, 1):
            if args.location_level == "city":
                city_meey_id = row.get("meey_id")
                district_meey_id = None
                ward_meey_id = None
            else:
                city_meey_id = row.get("city_meey_id")
                district_meey_id = row.get("meey_id") if args.location_level == "district" else row.get("district_meey_id")
                ward_meey_id = row.get("meey_id") if args.location_level == "ward" else None
            parts.append(
                {
                    "label": f"L{idx}",
                    "city_meey_id": city_meey_id,
                    "district_meey_id": district_meey_id,
                    "ward_meey_id": ward_meey_id,
                    "name": row.get("name_vi") or row.get("name") or row.get("meey_id"),
                }
            )
        return parts

    # Auto split with mixed thresholds:
    # - city_total <= city_split_threshold   => crawl city directly
    # - city_total >  city_split_threshold   => split to districts
    # - district_total <= district_split_threshold => crawl district directly
    # - district_total >  district_split_threshold => split to wards
    # Keep auto_split_threshold (default 20k) as an oversized warning threshold.
    city_rows = fetch_location_rows(db=db, table_name=table_name, level_type="city")
    if args.location_city_meey_id:
        city_rows = [r for r in city_rows if str(r.get("meey_id")) == str(args.location_city_meey_id)]
    elif args.worker_total > 1:
        # Split city scope first so each worker only probes/splits its own cities.
        ordered = list(city_rows)
        n = len(ordered)
        chunk = (n + args.worker_total - 1) // args.worker_total
        start = (args.worker_index - 1) * chunk
        end = min(start + chunk, n)
        if start >= n:
            city_rows = []
        else:
            city_rows = ordered[start:end]

    parts: List[Dict[str, Any]] = []
    idx = 1
    for city in city_rows:
        city_id = city.get("meey_id")
        city_name = city.get("name_vi") or city.get("name") or city_id
        if not city_id:
            continue
        city_total, city_pages = probe_total_results(session, args, city_id, None, None, f"CITY:{city_id}")
        log_split(
            args,
            f"[SPLIT_CHECK] level=city city={city_id} name={city_name} totalResults={city_total} totalPages={city_pages}",
        )
        if city_total <= args.city_split_threshold:
            parts.append(
                {
                    "label": f"L{idx}",
                    "city_meey_id": city_id,
                    "district_meey_id": None,
                    "ward_meey_id": None,
                    "name": city_name,
                }
            )
            idx += 1
            continue

        district_rows = fetch_location_rows(db=db, table_name=table_name, level_type="district", city_meey_id=city_id)
        for district in district_rows:
            district_id = district.get("meey_id")
            district_name = district.get("name_vi") or district.get("name") or district_id
            if args.location_district_meey_id and str(district_id) != str(args.location_district_meey_id):
                continue
            d_total, d_pages = probe_total_results(session, args, city_id, district_id, None, f"DIST:{district_id}")
            log_split(
                args,
                f"[SPLIT_CHECK] level=district city={city_id} district={district_id} name={district_name} "
                f"totalResults={d_total} totalPages={d_pages}",
            )
            if d_total <= args.district_split_threshold:
                parts.append(
                    {
                        "label": f"L{idx}",
                        "city_meey_id": city_id,
                        "district_meey_id": district_id,
                        "ward_meey_id": None,
                        "name": district_name,
                    }
                )
                idx += 1
                continue

            ward_rows = fetch_location_rows(
                db=db,
                table_name=table_name,
                level_type="ward",
                city_meey_id=city_id,
                district_meey_id=district_id,
            )
            for ward in ward_rows:
                ward_id = ward.get("meey_id")
                ward_name = ward.get("name_vi") or ward.get("name") or ward_id
                if not ward_id:
                    continue
                w_total, w_pages = probe_total_results(session, args, city_id, district_id, ward_id, f"WARD:{ward_id}")
                log_split(
                    args,
                    f"[SPLIT_CHECK] level=ward city={city_id} district={district_id} ward={ward_id} name={ward_name} "
                    f"totalResults={w_total} totalPages={w_pages}",
                )
                if args.auto_split_threshold > 0 and w_total > args.auto_split_threshold:
                    log_split(
                        args,
                        f"[OVERSIZE_WARD] city={city_id} district={district_id} ward={ward_id} "
                        f"totalResults={w_total} hard_threshold={args.auto_split_threshold}",
                    )
                parts.append(
                    {
                        "label": f"L{idx}",
                        "city_meey_id": city_id,
                        "district_meey_id": district_id,
                        "ward_meey_id": ward_id,
                        "name": ward_name,
                    }
                )
                idx += 1

    return parts

    parts: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, 1):
        city_meey_id = row.get("city_meey_id")
        district_meey_id = row.get("district_meey_id")
        ward_meey_id = row.get("meey_id") if args.location_level == "ward" else None
        district_for_payload = row.get("meey_id") if args.location_level == "district" else district_meey_id
        label = f"L{idx}"
        parts.append(
            {
                "label": label,
                "city_meey_id": city_meey_id,
                "district_meey_id": district_for_payload,
                "ward_meey_id": ward_meey_id,
                "name": row.get("name_vi") or row.get("name") or row.get("meey_id"),
            }
        )
    return parts


def crawl_location_part(
    session: Any,
    db: Database,
    args: argparse.Namespace,
    part: Dict[str, Any],
    start_page: int,
    total_seen_saved: Tuple[int, int],
) -> Tuple[int, int]:
    total_seen, total_saved = total_seen_saved
    part_label = part["label"]
    city_meey_id = part["city_meey_id"]
    district_meey_id = part["district_meey_id"]
    ward_meey_id = part["ward_meey_id"]
    part_name = part.get("name")

    if not city_meey_id:
        log_both(args, f"[SKIP_PART] part={part_label} reason=missing_city name={part_name}")
        return total_seen, total_saved

    save_checkpoint(
        args,
        {
            "status": "part_started",
            "part": part_label,
            "page": start_page,
            "next_page": start_page,
            "city_meey_id": city_meey_id,
            "district_meey_id": district_meey_id,
            "ward_meey_id": ward_meey_id,
            "name": part_name,
            "updated_at": utc_now(),
        },
    )
    log_both(
        args,
        f"[PART] part={part_label} start_page={start_page} "
        f"city={city_meey_id} district={district_meey_id} ward={ward_meey_id or '-'} name={part_name}",
    )

    _, first_obj = fetch_page_locations(
        session,
        city_meey_id,
        district_meey_id,
        ward_meey_id,
        args.limit,
        start_page,
        args,
        part_label,
    )
    total_results, total_pages, items = extract_page_data(first_obj)
    max_page = total_pages
    if args.max_pages_per_part:
        max_page = min(max_page, start_page + args.max_pages_per_part - 1)

    log_both(
        args,
        f"[PART_META] part={part_label} totalResults={total_results} totalPages={total_pages} run_to_page={max_page}",
    )
    if total_pages == 0:
        return total_seen, total_saved

    part_dup_hits = 0
    run_seen_matins: set = set()
    run_seen_urls: set = set()
    empty_page_streak = 0

    for page in range(start_page, max_page + 1):
        if page == start_page:
            page_items = items
        else:
            if args.delay > 0:
                log_both(args, f"[SLEEP] part={part_label} next_page={page} seconds={args.delay}")
                time.sleep(args.delay)
            _, page_obj = fetch_page_locations(
                session,
                city_meey_id,
                district_meey_id,
                ward_meey_id,
                args.limit,
                page,
                args,
                part_label,
            )
            _, _, page_items = extract_page_data(page_obj)

        log_both(args, f"[PAGE] part={part_label} page={page}/{max_page} items={len(page_items)}")
        if not page_items:
            empty_page_streak += 1
            if empty_page_streak >= 3:
                log_both(args, f"[STOP_EMPTY] part={part_label} page={page} empty_streak={empty_page_streak}")
                break
        else:
            empty_page_streak = 0

        page_saved = 0
        page_dup_hits = 0
        page_matins: List[str] = []
        page_urls: List[str] = []
        mapped_items: List[Dict[str, Any]] = []
        for item in page_items:
            mapped = map_search_item(item, args.domain)
            mapped_items.append(mapped)
            if mapped.get("matin"):
                page_matins.append(str(mapped["matin"]))
            if mapped.get("url"):
                page_urls.append(mapped["url"])

        existing_matins, existing_urls = load_existing_keys(db, args.domain, page_matins, page_urls)
        for idx, mapped in enumerate(mapped_items, 1):
            total_seen += 1
            matin = str(mapped.get("matin")) if mapped.get("matin") else None
            url = mapped.get("url")

            is_dup = False
            if matin and (matin in existing_matins or matin in run_seen_matins):
                is_dup = True
            elif url and (url in existing_urls or url in run_seen_urls):
                is_dup = True

            if is_dup:
                if args.refresh_existing_contact and not args.dry_run:
                    upd = refresh_contact_for_existing(db, args.domain, mapped)
                    if upd > 0:
                        page_saved += 1
                        total_saved += 1
                        log_both(
                            args,
                            f"  [REFRESH] part={part_label} page={page} idx={idx} code={mapped.get('matin')} updated={upd}",
                        )
                    else:
                        log_both(args, f"  [DUP] part={part_label} page={page} idx={idx} code={mapped.get('matin')} url={url}")
                else:
                    page_dup_hits += 1
                    part_dup_hits += 1
                    log_both(args, f"  [DUP] part={part_label} page={page} idx={idx} code={mapped.get('matin')} url={url}")
                continue

            if args.dry_run:
                page_saved += 1
                total_saved += 1
                log_both(args, f"  [DRY] part={part_label} page={page} idx={idx} code={mapped.get('matin')}")
                if matin:
                    run_seen_matins.add(matin)
                if url:
                    run_seen_urls.add(url)
                continue

            row_id = db.add_scraped_detail_flat(
                link_id=None,
                url=mapped["url"],
                domain=args.domain,
                data=mapped,
                loaihinh=mapped["loaihinh"],
                trade_type=mapped["trade_type"],
            )
            if row_id:
                page_saved += 1
                total_saved += 1
                log_both(args, f"  [SAVE] part={part_label} page={page} idx={idx} code={mapped.get('matin')} row_id={row_id}")
                if matin:
                    run_seen_matins.add(matin)
                if url:
                    run_seen_urls.add(url)

        save_checkpoint(
            args,
            {
                "status": "page_done",
                "part": part_label,
                "page": page,
                "next_page": page + 1,
                "city_meey_id": city_meey_id,
                "district_meey_id": district_meey_id,
                "ward_meey_id": ward_meey_id,
                "name": part_name,
                "total_results": total_results,
                "total_pages": total_pages,
                "items": len(page_items),
                "saved": page_saved,
                "duplicate_items": page_dup_hits,
                "duplicate_total_part": part_dup_hits,
                "updated_at": utc_now(),
            },
        )
        log_both(
            args,
            f"[PAGE_SUMMARY] part={part_label} page={page} saved={page_saved} dup={page_dup_hits} dup_total_part={part_dup_hits}",
        )
        if args.dup_stop_threshold > 0 and part_dup_hits > args.dup_stop_threshold:
            log_both(
                args,
                f"[STOP_DUP_THRESHOLD] part={part_label} page={page} dup_total_part={part_dup_hits} "
                f"threshold={args.dup_stop_threshold}",
            )
            break

    log_both(args, f"[DONE_PART] part={part_label} seen={total_seen} saved={total_saved} dup_total_part={part_dup_hits}")
    return total_seen, total_saved


def get_part_meta(session: Any, bounds: List[List[float]], args: argparse.Namespace, part_label: str) -> Tuple[int, int]:
    probe_limit = min(args.limit, 10)
    _, obj = fetch_page(session, bounds, probe_limit, 1, args, part_label)
    total_results, total_pages, _ = extract_page_data(obj)
    return total_results, total_pages


def build_leaf_parts(
    session: Any,
    args: argparse.Namespace,
    part_label: str,
    bounds: List[List[float]],
) -> List[Dict[str, Any]]:
    total_results, total_pages_probe = get_part_meta(session, bounds, args, part_label)
    log_split(
        args,
        f"[SPLIT_CHECK] part={part_label} totalResults={total_results} totalPages_probe={total_pages_probe} bounds={bounds}",
    )
    log_event(
        args,
        {
            "event": "split_check",
            "part": part_label,
            "bounds": bounds,
            "total_results": total_results,
            "total_pages_probe": total_pages_probe,
            "updated_at": utc_now(),
        },
    )
    if total_results <= args.split_threshold:
        log_split(args, f"[LEAF] part={part_label} totalResults={total_results} bounds={bounds}")
        return [{"label": part_label, "bounds": bounds, "total_results": total_results}]

    upper, lower = split_bounds_vertical(bounds)
    left_label = f"{part_label}.1"
    right_label = f"{part_label}.2"
    log_split(
        args,
        f"[SPLIT] part={part_label} totalResults={total_results} -> {left_label} bounds={upper} | {right_label} bounds={lower}",
    )
    leaves: List[Dict[str, Any]] = []
    leaves.extend(build_leaf_parts(session, args, left_label, upper))
    leaves.extend(build_leaf_parts(session, args, right_label, lower))
    return leaves


def crawl_leaf_part(
    session: Any,
    db: Database,
    args: argparse.Namespace,
    part_label: str,
    bounds: List[List[float]],
    part_total_results: int,
    start_page: int,
    total_seen_saved: Tuple[int, int],
) -> Tuple[int, int]:
    total_seen, total_saved = total_seen_saved
    part_dup_hits = 0
    run_seen_matins: set = set()
    run_seen_urls: set = set()

    save_checkpoint(
        args,
        {
            "status": "part_started",
            "part": part_label,
            "page": start_page,
            "next_page": start_page,
            "bounds": bounds,
            "split_total_results": part_total_results,
            "updated_at": utc_now(),
        },
    )
    log_both(args, f"[PART] part={part_label} start_page={start_page} bounds={bounds}")
    log_event(
        args,
        {
            "event": "part_started",
            "part": part_label,
            "bounds": bounds,
            "split_total_results": part_total_results,
            "updated_at": utc_now(),
        },
    )

    _, first_obj = fetch_page(session, bounds, args.limit, start_page, args, part_label)
    total_results, total_pages, items = extract_page_data(first_obj)
    max_page = total_pages
    if args.max_pages_per_part:
        max_page = min(max_page, start_page + args.max_pages_per_part - 1)

    log_both(
        args,
        f"[PART_META] part={part_label} totalResults={total_results} totalPages={total_pages} run_to_page={max_page}",
    )
    log_event(
        args,
        {
            "event": "part_meta",
            "part": part_label,
            "bounds": bounds,
            "total_results": total_results,
            "total_pages": total_pages,
            "run_to_page": max_page,
            "updated_at": utc_now(),
        },
    )
    if total_pages == 0:
        return total_seen, total_saved

    empty_page_streak = 0
    for page in range(start_page, max_page + 1):
        if page == start_page:
            page_items = items
        else:
            if args.delay > 0:
                log_both(args, f"[SLEEP] part={part_label} next_page={page} seconds={args.delay}")
                time.sleep(args.delay)
            save_checkpoint(
                args,
                {
                    "status": "page_started",
                    "part": part_label,
                    "page": page,
                    "next_page": page,
                    "bounds": bounds,
                    "total_results": total_results,
                    "total_pages": total_pages,
                    "updated_at": utc_now(),
                },
            )
            _, page_obj = fetch_page(session, bounds, args.limit, page, args, part_label)
            _, _, page_items = extract_page_data(page_obj)

        log_both(args, f"[PAGE] part={part_label} page={page}/{max_page} items={len(page_items)} bounds={bounds}")
        log_event(
            args,
            {
                "event": "page_loaded",
                "part": part_label,
                "page": page,
                "max_page": max_page,
                "bounds": bounds,
                "items": len(page_items),
                "updated_at": utc_now(),
            },
        )

        if not page_items:
            empty_page_streak += 1
            if empty_page_streak >= 3:
                log_both(args, f"[STOP_EMPTY] part={part_label} page={page} empty_streak={empty_page_streak}")
                save_checkpoint(
                    args,
                    {
                        "status": "stop_empty",
                        "part": part_label,
                        "page": page,
                        "next_page": page + 1,
                        "bounds": bounds,
                        "total_results": total_results,
                        "total_pages": total_pages,
                        "updated_at": utc_now(),
                    },
                )
                break
        else:
            empty_page_streak = 0

        page_saved = 0
        page_dup_hits = 0
        page_matins: List[str] = []
        page_urls: List[str] = []
        mapped_items: List[Dict[str, Any]] = []
        for item in page_items:
            mapped = map_search_item(item, args.domain)
            mapped_items.append(mapped)
            matin = mapped.get("matin")
            if matin:
                page_matins.append(str(matin))
            url = mapped.get("url")
            if url:
                page_urls.append(url)
        existing_matins, existing_urls = load_existing_keys(db, args.domain, page_matins, page_urls)

        for idx, mapped in enumerate(mapped_items, 1):
            total_seen += 1
            matin = str(mapped.get("matin")) if mapped.get("matin") else None
            url = mapped.get("url")
            is_dup = False
            if matin and (matin in existing_matins or matin in run_seen_matins):
                is_dup = True
            elif url and (url in existing_urls or url in run_seen_urls):
                is_dup = True

            if is_dup:
                if args.refresh_existing_contact and not args.dry_run:
                    upd = refresh_contact_for_existing(db, args.domain, mapped)
                    if upd > 0:
                        page_saved += 1
                        total_saved += 1
                        log_both(
                            args,
                            f"  [REFRESH] part={part_label} page={page} idx={idx} code={mapped.get('matin')} updated={upd}",
                        )
                    else:
                        log_both(
                            args,
                            f"  [DUP] part={part_label} page={page} idx={idx} code={mapped.get('matin')} url={url}",
                        )
                else:
                    page_dup_hits += 1
                    part_dup_hits += 1
                    log_both(
                        args,
                        f"  [DUP] part={part_label} page={page} idx={idx} code={mapped.get('matin')} url={url}",
                    )
                continue

            if args.dry_run:
                page_saved += 1
                total_saved += 1
                log_both(args, f"  [DRY] part={part_label} page={page} idx={idx} code={mapped['matin']} title={mapped['title']}")
                if matin:
                    run_seen_matins.add(matin)
                if url:
                    run_seen_urls.add(url)
                continue

            row_id = db.add_scraped_detail_flat(
                link_id=None,
                url=mapped["url"],
                domain=args.domain,
                data=mapped,
                loaihinh=mapped["loaihinh"],
                trade_type=mapped["trade_type"],
            )
            if row_id:
                page_saved += 1
                total_saved += 1
                log_both(args, f"  [SAVE] part={part_label} page={page} idx={idx} code={mapped['matin']} row_id={row_id}")
                if matin:
                    run_seen_matins.add(matin)
                if url:
                    run_seen_urls.add(url)
            else:
                log_both(args, f"  [SKIP] part={part_label} page={page} idx={idx} code={mapped['matin']} reason=save_failed")

        next_page = page + 1
        save_checkpoint(
            args,
            {
                "status": "page_done",
                "part": part_label,
                "page": page,
                "next_page": next_page,
                "bounds": bounds,
                "total_results": total_results,
                "total_pages": total_pages,
                "items": len(page_items),
                "saved": page_saved,
                "duplicate_items": page_dup_hits,
                "duplicate_total_part": part_dup_hits,
                "updated_at": utc_now(),
            },
        )
        log_both(
            args,
            f"[PAGE_SUMMARY] part={part_label} page={page} saved={page_saved} dup={page_dup_hits} dup_total_part={part_dup_hits}",
        )
        log_event(
            args,
            {
                "event": "page_done",
                "part": part_label,
                "page": page,
                "next_page": next_page,
                "bounds": bounds,
                "total_results": total_results,
                "total_pages": total_pages,
                "items": len(page_items),
                "saved": page_saved,
                "duplicate_items": page_dup_hits,
                "duplicate_total_part": part_dup_hits,
                "updated_at": utc_now(),
            },
        )

        if args.dup_stop_threshold > 0 and part_dup_hits > args.dup_stop_threshold:
            log_both(
                args,
                f"[STOP_DUP_THRESHOLD] part={part_label} page={page} dup_total_part={part_dup_hits} "
                f"threshold={args.dup_stop_threshold}",
            )
            save_checkpoint(
                args,
                {
                    "status": "stop_duplicate_threshold",
                    "part": part_label,
                    "page": page,
                    "next_page": next_page,
                    "bounds": bounds,
                    "total_results": total_results,
                    "total_pages": total_pages,
                    "duplicate_total_part": part_dup_hits,
                    "updated_at": utc_now(),
                },
            )
            break

    log_both(args, f"[DONE_PART] part={part_label} seen={total_seen} saved={total_saved} dup_total_part={part_dup_hits}")
    return total_seen, total_saved


def main() -> int:
    args = parse_args()
    if args.limit > 500:
        raise SystemExit("--limit must be <= 500")
    if args.start_page < 1:
        raise SystemExit("--start-page must be >= 1")

    start_part = args.start_part
    start_page = args.start_page
    if args.resume:
        cp = load_checkpoint(args.checkpoint_file)
        if cp and cp.get("status") in {"page_done", "page_started", "part_started", "stop_empty"}:
            start_part = str(cp.get("part") or start_part)
            start_page = int(cp.get("next_page") or cp.get("page") or start_page)
            log_both(args, f"[RESUME] part={start_part} page={start_page} checkpoint={args.checkpoint_file}")

    apply_auth_overrides(args)
    log_both(
        args,
        f"[CONFIG] api={args.api_url} domain={args.domain} category={args.category or '-'} "
        f"zoom={args.zoom or '-'} fakeCoordinates={bool(args.fake_coordinates)} useLocationTable={bool(args.use_location_table)} "
        f"worker={args.worker_index}/{args.worker_total} refreshExistingContact={bool(args.refresh_existing_contact)} "
        f"autoSplitThreshold={args.auto_split_threshold}",
    )

    db = Database()
    session = make_session(args)

    if args.use_location_table:
        if start_part.isdigit():
            start_part = f"L{start_part}"
        all_parts = load_location_parts(db, session, args)
        if args.auto_split_threshold > 0:
            parts = all_parts
        else:
            parts = split_parts_by_worker(all_parts, args.worker_index, args.worker_total)
        if not parts:
            raise SystemExit("No locations assigned for this worker")
        part_labels = {p["label"] for p in parts}
        if start_part not in part_labels:
            start_part = parts[0]["label"]
        log_both(
            args,
            f"[LOCATIONS] table={args.location_table} level={args.location_level} total_parts_worker={len(parts)} "
            f"total_parts_all={len(all_parts)} "
            f"city_filter={args.location_city_meey_id or '-'} district_filter={args.location_district_meey_id or '-'}",
        )
        total_seen = 0
        total_saved = 0
        started = False
        for part in parts:
            if not started:
                if part["label"] != start_part:
                    continue
                started = True
            current_start_page = start_page if part["label"] == start_part else 1
            total_seen, total_saved = crawl_location_part(
                session,
                db,
                args,
                part,
                current_start_page,
                (total_seen, total_saved),
            )
            start_page = 1

        save_checkpoint(
            args,
            {
                "status": "done",
                "part": None,
                "page": None,
                "next_page": None,
                "updated_at": utc_now(),
                "total_seen": total_seen,
                "total_saved": total_saved,
            },
        )
        log_both(args, f"[DONE] seen={total_seen} saved={total_saved} dry_run={args.dry_run}")
        return 0

    if "." in start_part:
        first_root = int(start_part.split(".")[0])
    else:
        first_root = int(start_part)
    if first_root < 1 or first_root > ROOT_PARTS:
        raise SystemExit("--start-part root must be in 1..10")

    leaf_parts: List[Dict[str, Any]] = []
    root_count = 0
    for root in range(first_root, ROOT_PARTS + 1):
        if args.max_parts and root_count >= args.max_parts:
            break
        leaf_parts.extend(build_leaf_parts(session, args, str(root), build_root_bounds(root)))
        root_count += 1

    total_seen = 0
    total_saved = 0
    started = False
    start_prefix = f"{start_part}."
    for leaf in leaf_parts:
        if not started:
            if leaf["label"] != start_part and not leaf["label"].startswith(start_prefix):
                continue
            started = True
        current_start_page = start_page if leaf["label"] == start_part else 1
        total_seen, total_saved = crawl_leaf_part(
            session,
            db,
            args,
            leaf["label"],
            leaf["bounds"],
            leaf["total_results"],
            current_start_page,
            (total_seen, total_saved),
        )
        start_page = 1

    save_checkpoint(
        args,
        {
            "status": "done",
            "part": None,
            "page": None,
            "next_page": None,
            "updated_at": utc_now(),
            "total_seen": total_seen,
            "total_saved": total_saved,
        },
    )
    log_both(args, f"[DONE] seen={total_seen} saved={total_saved} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
