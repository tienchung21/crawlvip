#!/usr/bin/env python3
"""
Homedy list crawler.

Fetches paginated product data from https://homedy.com/Maps/ListProduct
and stores rows into scraped_details / scraped_details_flat.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from curl_cffi import requests as cffi_requests
except Exception:
    cffi_requests = None

from database import Database


API_URL = "https://homedy.com/Maps/ListProduct"
BASE_URL = "https://homedy.com/"
DOMAIN = "homedy.com"
DEFAULT_PAGE_SIZE = 200
DEFAULT_DELAY_SECONDS = 0.0

CITY_DATA = [
    {"Id": 2, "Name": "TP Hồ Chí Minh"},
    {"Id": 1, "Name": "Hà Nội"},
    {"Id": 4, "Name": "Đà Nẵng"},
    {"Id": 3, "Name": "Bình Dương"},
    {"Id": 8, "Name": "An Giang"},
    {"Id": 7, "Name": "Bà Rịa Vũng Tàu"},
    {"Id": 11, "Name": "Bạc Liêu"},
    {"Id": 9, "Name": "Bắc Giang"},
    {"Id": 10, "Name": "Bắc Kạn"},
    {"Id": 12, "Name": "Bắc Ninh"},
    {"Id": 13, "Name": "Bến Tre"},
    {"Id": 14, "Name": "Bình Định"},
    {"Id": 15, "Name": "Bình Phước"},
    {"Id": 16, "Name": "Bình Thuận"},
    {"Id": 17, "Name": "Cà Mau"},
    {"Id": 19, "Name": "Cao Bằng"},
    {"Id": 18, "Name": "Cần Thơ"},
    {"Id": 20, "Name": "Đắk Lắk"},
    {"Id": 21, "Name": "Đắk Nông"},
    {"Id": 22, "Name": "Điện Biên"},
    {"Id": 23, "Name": "Đồng Nai"},
    {"Id": 24, "Name": "Đồng Tháp"},
    {"Id": 25, "Name": "Gia Lai"},
    {"Id": 26, "Name": "Hà Giang"},
    {"Id": 27, "Name": "Hà Nam"},
    {"Id": 28, "Name": "Hà Tĩnh"},
    {"Id": 29, "Name": "Hải Dương"},
    {"Id": 5, "Name": "Hải Phòng"},
    {"Id": 30, "Name": "Hậu Giang"},
    {"Id": 31, "Name": "Hòa Bình"},
    {"Id": 32, "Name": "Hưng Yên"},
    {"Id": 33, "Name": "Khánh Hòa"},
    {"Id": 34, "Name": "Kiên Giang"},
    {"Id": 35, "Name": "Kon Tum"},
    {"Id": 36, "Name": "Lai Châu"},
    {"Id": 38, "Name": "Lạng Sơn"},
    {"Id": 39, "Name": "Lào Cai"},
    {"Id": 37, "Name": "Lâm Đồng"},
    {"Id": 6, "Name": "Long An"},
    {"Id": 40, "Name": "Nam Định"},
    {"Id": 41, "Name": "Nghệ An"},
    {"Id": 42, "Name": "Ninh Bình"},
    {"Id": 43, "Name": "Ninh Thuận"},
    {"Id": 44, "Name": "Phú Thọ"},
    {"Id": 45, "Name": "Phú Yên"},
    {"Id": 46, "Name": "Quảng Bình"},
    {"Id": 47, "Name": "Quảng Nam"},
    {"Id": 48, "Name": "Quảng Ngãi"},
    {"Id": 49, "Name": "Quảng Ninh"},
    {"Id": 50, "Name": "Quảng Trị"},
    {"Id": 51, "Name": "Sóc Trăng"},
    {"Id": 52, "Name": "Sơn La"},
    {"Id": 53, "Name": "Tây Ninh"},
    {"Id": 54, "Name": "Thái Bình"},
    {"Id": 55, "Name": "Thái Nguyên"},
    {"Id": 56, "Name": "Thanh Hóa"},
    {"Id": 57, "Name": "Thừa Thiên Huế"},
    {"Id": 58, "Name": "Tiền Giang"},
    {"Id": 59, "Name": "Trà Vinh"},
    {"Id": 60, "Name": "Tuyên Quang"},
    {"Id": 61, "Name": "Vĩnh Long"},
    {"Id": 62, "Name": "Vĩnh Phúc"},
    {"Id": 63, "Name": "Yên Bái"},
]

SELL_TYPE_TO_TRADE_TYPE = {
    1: "s",
    2: "u",
}


def log(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Homedy list API into scraped_details_flat")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout per page request, seconds")
    parser.add_argument("--max-retries", type=int, default=4, help="Max retries for a failed page request")
    parser.add_argument("--city-id", type=int, action="append", help="Only crawl these city IDs")
    parser.add_argument("--skip-city-id", type=int, action="append", help="Skip these city IDs")
    parser.add_argument("--sell-type", type=int, action="append", choices=[1, 2], help="Only crawl these sell types")
    parser.add_argument("--max-pages-per-city", type=int, default=0, help="Optional cap for testing")
    parser.add_argument("--start-page", type=int, default=1, help="Start from this page index")
    parser.add_argument("--max-items", type=int, default=0, help="Optional total item cap for testing")
    parser.add_argument(
        "--dup-stop-threshold",
        type=int,
        default=0,
        help="Stop current city when consecutive duplicated link_id reaches this threshold (0=off)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print stats without writing DB")
    return parser.parse_args()


def make_session():
    if cffi_requests is None:
        raise RuntimeError("curl_cffi is required for homedy_crawler.py")
    session = cffi_requests.Session()
    session.headers.update(
        {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": BASE_URL.rstrip("/"),
            "Referer": BASE_URL,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
        }
    )
    return session


def build_payload(city_id: int, sell_type: int, page_index: int, page_size: int) -> Dict[str, Any]:
    return {
        "SellType": sell_type,
        "UrlType": 1,
        "ProjectId": 0,
        "CategoryId": 0,
        "MinPriceName": "Tất cả",
        "MaxPriceName": "Tất cả",
        "MinAcreage": 0,
        "MaxAcreage": 0,
        "CityId": city_id,
        "DistrictId": 0,
        "WardId": 0,
        "StreetId": 0,
        "BedRoom": 0,
        "BathRoom": 0,
        "MainDirection": 0,
        "LatitudeMin": 0,
        "LatitudeMax": 0,
        "LongitudeMin": 0,
        "LongitudeMax": 0,
        "GeographyTaggedText": "",
        "Latitude": 0,
        "Longitude": 0,
        "Distance": 0,
        "PageIndex": page_index,
        "PageSize": page_size,
    }


def absolute_url(relative: Optional[str]) -> Optional[str]:
    if not relative:
        return None
    if relative.startswith("http://") or relative.startswith("https://"):
        return relative
    return BASE_URL + relative.lstrip("/")


def parse_media_urls(media_json: Any) -> List[str]:
    if not media_json:
        return []
    if isinstance(media_json, str):
        try:
            media_json = json.loads(media_json)
        except Exception:
            return []
    if not isinstance(media_json, list):
        return []
    urls: List[str] = []
    for item in media_json:
        if not isinstance(item, dict):
            continue
        media_url = item.get("MediaUrl")
        if not media_url:
            continue
        full_url = absolute_url(media_url)
        if full_url and full_url not in urls:
            urls.append(full_url)
    return urls


def format_khoanggia(product: Dict[str, Any]) -> Optional[str]:
    total = str(product.get("HtmlPriceTotal") or "").strip()
    currency = str(product.get("HtmlPriceTotalCurrency") or "").strip()
    if total and currency:
        return f"{total} {currency}"
    if total:
        return total
    price = product.get("Price")
    if price is None:
        return None
    return str(price)


def format_dientich(product: Dict[str, Any]) -> Optional[str]:
    acreage = product.get("Acreage")
    if acreage not in (None, ""):
        return str(acreage)
    html_acreage = product.get("HtmlAcreage")
    if html_acreage not in (None, ""):
        return str(html_acreage)
    return None


def product_to_data(product: Dict[str, Any], sell_type: int) -> Tuple[Optional[str], Dict[str, Any]]:
    product_id = product.get("Id")
    relative_url = product.get("Url")
    url = absolute_url(relative_url)
    if not product_id or not url:
        return None, {}

    lat = product.get("Latitude")
    lng = product.get("Longitude")

    trade_type = SELL_TYPE_TO_TRADE_TYPE.get(sell_type)
    media_urls = parse_media_urls(product.get("MediaJson"))
    agency = product.get("Agency") or {}
    project_id = product.get("ProjectId")

    data = {
        "title": product.get("Name"),
        "mota": product.get("Description"),
        "khoanggia": format_khoanggia(product),
        "dientich": format_dientich(product),
        "sophongngu": product.get("BedRoom"),
        "sophongvesinh": product.get("BathRoom"),
        "tenmoigioi": agency.get("FullName"),
        "sodienthoai": agency.get("Mobile"),
        "map": None,
        "matin": str(product_id),
        "ngaydang": product.get("StartDate"),
        "diachi": product.get("Address"),
        "street_ext": str(product.get("StreetId")) if product.get("StreetId") not in (None, "") else None,
        "ward_ext": str(product.get("WardId")) if product.get("WardId") not in (None, "") else None,
        "district_ext": str(product.get("DistrictId")) if product.get("DistrictId") not in (None, "") else None,
        "city_ext": str(product.get("CityId")) if product.get("CityId") not in (None, "") else None,
        "lat": str(lat) if lat not in (None, "") else None,
        "lng": str(lng) if lng not in (None, "") else None,
        "img": media_urls,
        "img_count": product.get("MediaCount"),
        "trade_type": trade_type,
        # Homedy currently returns numeric ProjectId (no ProjectName in this API payload).
        # Store project id as string in thuocduan for downstream project mapping.
        "thuocduan": str(project_id) if project_id not in (None, "", 0, "0") else None,
    }
    return url, data


def get_existing_detail_id(db: Database, link_id: int) -> Optional[int]:
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM scraped_details_flat WHERE link_id = %s LIMIT 1", (link_id,))
        row = cursor.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return row.get("id")
        return row[0]
    finally:
        cursor.close()
        conn.close()


def save_product(db: Database, product: Dict[str, Any], sell_type: int) -> Tuple[bool, str]:
    url, data = product_to_data(product, sell_type)
    if not url:
        return False, "skip_missing_url"

    link_id = product.get("Id")
    trade_type = SELL_TYPE_TO_TRADE_TYPE.get(sell_type)
    loaihinh = str(product.get("CategoryId")) if product.get("CategoryId") not in (None, "") else None

    db.add_scraped_detail(
        url=url,
        data=product,
        domain=DOMAIN,
        link_id=link_id,
        success=True,
    )

    detail_id = db.add_scraped_detail_flat(
        url=url,
        data=data,
        domain=DOMAIN,
        link_id=link_id,
        loaihinh=loaihinh,
        trade_type=trade_type,
    )

    if not detail_id:
        detail_id = get_existing_detail_id(db, int(link_id))

    if not detail_id:
        return False, "save_flat_failed"

    if detail_id and data.get("img"):
        db.add_detail_images(detail_id, data["img"])

    return True, "saved"


def fetch_page(
    session,
    city_id: int,
    sell_type: int,
    page_index: int,
    page_size: int,
    timeout: int,
    max_retries: int,
) -> Dict[str, Any]:
    payload = build_payload(city_id=city_id, sell_type=sell_type, page_index=page_index, page_size=page_size)
    last_error: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.post(API_URL, json=payload, timeout=timeout, impersonate="chrome124")
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            sleep_s = min(attempt * 2, 10)
            log(
                f"  [RETRY] city_id={city_id} sell_type={sell_type} page={page_index} "
                f"attempt={attempt}/{max_retries} error={type(exc).__name__} sleep={sleep_s}s"
            )
            time.sleep(sleep_s)
    raise RuntimeError(
        f"fetch_page failed city_id={city_id} sell_type={sell_type} "
        f"page={page_index} after {max_retries} attempts: {last_error}"
    )


def iter_cities(city_ids: Optional[List[int]], skip_city_ids: Optional[List[int]] = None) -> Iterable[Dict[str, Any]]:
    skipped = set(skip_city_ids or [])
    if not city_ids:
        for city in CITY_DATA:
            if city["Id"] not in skipped:
                yield city
        return
    allowed = set(city_ids)
    for city in CITY_DATA:
        if city["Id"] in allowed and city["Id"] not in skipped:
            yield city


def main() -> int:
    args = parse_args()
    db = Database()
    session = make_session()
    sell_types = args.sell_type or [1, 2]

    total_saved = 0
    total_seen = 0

    for city in iter_cities(args.city_id, args.skip_city_id):
        city_id = city["Id"]
        city_name = city["Name"]
        city_dup_consecutive = 0
        stop_current_city = False
        for sell_type in sell_types:
            if stop_current_city:
                break
            trade_type = SELL_TYPE_TO_TRADE_TYPE.get(sell_type, str(sell_type))
            log(f"[START] city_id={city_id} city={city_name} sell_type={sell_type} trade_type={trade_type}")
            first_page = fetch_page(
                session,
                city_id=city_id,
                sell_type=sell_type,
                page_index=1,
                page_size=args.page_size,
                timeout=args.timeout,
                max_retries=args.max_retries,
            )
            total = int(first_page.get("Total") or 0)
            products = first_page.get("Products") or []
            if total <= 0 or not products:
                log(f"[EMPTY] city_id={city_id} city={city_name} sell_type={sell_type} total={total}")
                continue

            max_pages = math.ceil(total / args.page_size)
            if args.max_pages_per_city > 0:
                max_pages = min(max_pages, args.max_pages_per_city)
            start_page = max(args.start_page, 1)
            if start_page > max_pages:
                log(
                    f"[SKIP_RANGE] city_id={city_id} city={city_name} sell_type={sell_type} "
                    f"start_page={start_page} max_pages={max_pages}"
                )
                continue

            log(
                f"[CITY] city_id={city_id} city={city_name} sell_type={sell_type} total={total} "
                f"page_size={args.page_size} pages={max_pages} start_page={start_page}"
            )

            for page_index in range(start_page, max_pages + 1):
                page_data = first_page if page_index == 1 else fetch_page(
                    session,
                    city_id=city_id,
                    sell_type=sell_type,
                    page_index=page_index,
                    page_size=args.page_size,
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                )
                page_products = page_data.get("Products") or []
                log(
                    f"  [PAGE] index={page_index}/{max_pages} city_id={city_id} sell_type={sell_type} "
                    f"items={len(page_products)}"
                )
                if not page_products:
                    log(f"  [STOP_PAGE] no items at page_index={page_index}")
                    break

                for product in page_products:
                    if args.max_items > 0 and total_seen >= args.max_items:
                        log(f"STOP reached max_items={args.max_items}")
                        log(f"DONE seen={total_seen} saved={total_saved} dry_run={args.dry_run}")
                        return 0
                    total_seen += 1
                    product_id = product.get("Id")
                    product_name = str(product.get("Name") or "").strip().replace("\n", " ")[:90]

                    if (
                        not args.dry_run
                        and args.dup_stop_threshold > 0
                        and product_id not in (None, "")
                    ):
                        try:
                            existing_id = get_existing_detail_id(db, int(product_id))
                        except Exception:
                            existing_id = None
                        if existing_id:
                            city_dup_consecutive += 1
                            log(
                                f"    [DUP] city_id={city_id} sell_type={sell_type} "
                                f"product_id={product_id} consecutive={city_dup_consecutive}/{args.dup_stop_threshold}"
                            )
                            if city_dup_consecutive >= args.dup_stop_threshold:
                                stop_current_city = True
                                log(
                                    f"[STOP_CITY_DUP] city_id={city_id} city={city_name} "
                                    f"sell_type={sell_type} threshold={args.dup_stop_threshold}"
                                )
                                break
                            continue
                        city_dup_consecutive = 0

                    if args.dry_run:
                        log(f"    [DRY] product_id={product_id} title={product_name}")
                        continue
                    ok, reason = save_product(db, product, sell_type)
                    if ok:
                        total_saved += 1
                        log(f"    [SAVE] product_id={product_id} title={product_name}")
                    else:
                        log(f"    [SKIP] product_id={product_id} reason={reason} title={product_name}")

                if args.delay > 0:
                    log(f"  [SLEEP] {args.delay}s")
                    time.sleep(args.delay)

                if stop_current_city:
                    break

            log(f"[DONE_TYPE] city_id={city_id} city={city_name} sell_type={sell_type}")

        if stop_current_city:
            log(f"[DONE_CITY_EARLY] city_id={city_id} city={city_name} reason=dup_threshold")

    log(f"DONE seen={total_seen} saved={total_saved} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
