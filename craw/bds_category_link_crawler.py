#!/usr/bin/env python3
import argparse
import hashlib
import os
import random
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pymysql
from curl_cffi import requests
from lxml import html as lxml_html


DOMAIN = "batdongsan.com.vn"
DEFAULT_IMPERSONATE = "chrome124"
DEFAULT_DELAY_MIN = 5.0
DEFAULT_DELAY_MAX = 5.0
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_SLEEP = 3.0

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://batdongsan.com.vn/",
    "Origin": "https://batdongsan.com.vn",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def get_db_conn():
    return pymysql.connect(**DB_CONFIG)


def ensure_schema():
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'collected_links'
              AND column_name = 'crawl_bo_sung'
            LIMIT 1
            """
        )
        if not cur.fetchone():
            cur.execute(
                """
                ALTER TABLE collected_links
                ADD COLUMN crawl_bo_sung TINYINT(1) NOT NULL DEFAULT 0
                """
            )
            conn.commit()
            print("[SCHEMA] Added collected_links.crawl_bo_sung")
    finally:
        conn.close()


def extract_prj_id_from_url(url: str) -> Optional[int]:
    m = re.search(r"pr(\d+)", url or "")
    return int(m.group(1)) if m else None


def extract_url_base(url: str) -> str:
    s = (url or "").strip()
    if not s:
        return ""
    s = s.split("?", 1)[0].strip()
    return re.sub(r"-pr\d+$", "", s)


def url_base_md5(url_base: str) -> Optional[str]:
    s = (url_base or "").strip()
    if not s:
        return None
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def get_bds_mapping() -> Dict[str, Tuple[str, str]]:
    mapping = {
        "ban-can-ho-chung-cu-mini": ("Bán", "Căn hộ chung cư mini"),
        "ban-can-ho-chung-cu": ("Bán", "Căn hộ chung cư"),
        "ban-nha-rieng": ("Bán", "Nhà riêng"),
        "ban-nha-biet-thu-lien-ke": ("Bán", "Biệt thự liền kề"),
        "ban-nha-mat-pho": ("Bán", "Nhà mặt phố"),
        "ban-shophouse-nha-pho-thuong-mai": ("Bán", "Shophouse"),
        "ban-dat-nen-du-an": ("Bán", "Đất nền dự án"),
        "ban-dat": ("Bán", "Đất"),
        "ban-trang-trai-khu-nghi-duong": ("Bán", "Trang trại/Khu nghỉ dưỡng"),
        "ban-condotel": ("Bán", "Condotel"),
        "ban-kho-nha-xuong": ("Bán", "Kho, nhà xưởng"),
        "ban-loai-bat-dong-san-khac": ("Bán", "BĐS khác"),
        "cho-thue-can-ho-chung-cu-mini": ("Thuê", "Căn hộ chung cư mini"),
        "cho-thue-can-ho-chung-cu": ("Thuê", "Căn hộ chung cư"),
        "cho-thue-nha-rieng": ("Thuê", "Nhà riêng"),
        "cho-thue-nha-biet-thu-lien-ke": ("Thuê", "Biệt thự liền kề"),
        "cho-thue-nha-mat-pho": ("Thuê", "Nhà mặt phố"),
        "cho-thue-shophouse-nha-pho-thuong-mai": ("Thuê", "Shophouse"),
        "cho-thue-nha-tro-phong-tro": ("Thuê", "Nhà trọ, phòng trọ"),
        "cho-thue-van-phong": ("Thuê", "Văn phòng"),
        "cho-thue-sang-nhuong-cua-hang-ki-ot": ("Thuê", "Cửa hàng, Ki-ốt"),
        "cho-thue-kho-nha-xuong-dat": ("Thuê", "Kho, nhà xưởng, đất"),
        "cho-thue-loai-bat-dong-san-khac": ("Thuê", "BĐS khác"),
    }
    return dict(sorted(mapping.items(), key=lambda x: len(x[0]), reverse=True))


def classify_detail_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    path = urlparse(url).path.lstrip("/")
    for prefix, (trade_type, loaihinh) in get_bds_mapping().items():
        if path == prefix or path.startswith(prefix + "-"):
            return loaihinh, trade_type
    return None, None


def normalize_category_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        raise ValueError("Empty URL")
    u = u.rstrip("/")
    u = re.sub(r"/p\d+$", "", u)
    return u


def derive_rent_url(url: str) -> Optional[str]:
    base = normalize_category_url(url)
    if "/nha-dat-ban-" in base:
        return base.replace("/nha-dat-ban-", "/nha-dat-cho-thue-", 1)
    return None


def build_page_url(base_url: str, page: int) -> str:
    return f"{normalize_category_url(base_url)}/p{page}"


def fetch_html(
    session: requests.Session,
    url: str,
    impersonate: str,
    retry_count: int,
    retry_sleep: float,
) -> str:
    last_exc = None
    for attempt in range(1, retry_count + 1):
        try:
            resp = session.get(
                url,
                headers=HEADERS,
                impersonate=impersonate,
                timeout=30,
                allow_redirects=True,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            if attempt >= retry_count:
                raise
            print(
                f"  [WARN] Fetch failed ({attempt}/{retry_count}) for {url}: {exc}. "
                f"Retry after {retry_sleep:.1f}s..."
            )
            time.sleep(retry_sleep)
    raise last_exc


def parse_total_count(page_html: str) -> Optional[int]:
    m = re.search(
        r'<span[^>]*id=["\']count-number["\'][^>]*>\s*([0-9,\.]+)\s*</span>',
        page_html,
        flags=re.I,
    )
    if not m:
        return None
    raw = re.sub(r"[^0-9]", "", m.group(1))
    return int(raw) if raw else 0


def parse_listing_links(page_html: str) -> List[Dict[str, Optional[str]]]:
    tree = lxml_html.fromstring(page_html)
    rows: List[Dict[str, Optional[str]]] = []
    seen = set()
    for a in tree.xpath("//a[contains(@class,'js__product-link-for-product-id')]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = f"https://{DOMAIN}{href}"
        if not href.startswith("https://batdongsan.com.vn/"):
            continue
        if href in seen:
            continue
        seen.add(href)
        product_id = (a.get("data-product-id") or "").strip() or None
        rows.append(
            {
                "url": href,
                "prj_id": int(product_id) if product_id and product_id.isdigit() else extract_prj_id_from_url(href),
            }
        )
    return rows


def load_existing_maps(
    conn,
    urls: List[str],
    prj_ids: List[int],
    url_base_md5s: List[str],
) -> Tuple[Dict[str, Dict], Dict[int, Dict], Dict[str, Dict]]:
    url_map: Dict[str, Dict] = {}
    prj_map: Dict[int, Dict] = {}
    base_map: Dict[str, Dict] = {}
    cur = conn.cursor()

    if urls:
        placeholders = ",".join(["%s"] * len(urls))
        cur.execute(
            f"""
            SELECT id, url, status, prj_id, url_base_md5
            FROM collected_links
            WHERE domain=%s AND url IN ({placeholders})
            ORDER BY id DESC
            """,
            [DOMAIN] + urls,
        )
        for row in cur.fetchall():
            url_map.setdefault(row["url"], row)

    if prj_ids:
        placeholders = ",".join(["%s"] * len(prj_ids))
        cur.execute(
            f"""
            SELECT id, url, status, prj_id, url_base_md5
            FROM collected_links
            WHERE domain=%s AND prj_id IN ({placeholders})
            ORDER BY id DESC
            """,
            [DOMAIN] + prj_ids,
        )
        for row in cur.fetchall():
            if row["prj_id"] is not None:
                prj_map.setdefault(int(row["prj_id"]), row)

    if url_base_md5s:
        placeholders = ",".join(["%s"] * len(url_base_md5s))
        cur.execute(
            f"""
            SELECT id, url, status, prj_id, url_base_md5
            FROM collected_links
            WHERE domain=%s AND url_base_md5 IN ({placeholders})
            ORDER BY id DESC
            """,
            [DOMAIN] + url_base_md5s,
        )
        for row in cur.fetchall():
            key = row.get("url_base_md5")
            if key:
                base_map.setdefault(key, row)

    return url_map, prj_map, base_map


def upsert_links(
    links: List[Dict[str, Optional[str]]],
    batch_date: str,
) -> Dict[str, int]:
    if not links:
        return {"inserted": 0, "updated": 0}

    conn = get_db_conn()
    try:
        cur = conn.cursor()
        prepared = []
        for item in links:
            url = (item["url"] or "").strip()
            if not url:
                continue
            prj_id = item.get("prj_id")
            url_base = extract_url_base(url)
            base_md5 = url_base_md5(url_base)
            loaihinh, trade_type = classify_detail_url(url)
            prepared.append(
                {
                    "url": url,
                    "prj_id": prj_id,
                    "url_base": url_base,
                    "url_base_md5": base_md5,
                    "loaihinh": loaihinh,
                    "trade_type": trade_type,
                }
            )

        urls = [x["url"] for x in prepared]
        prj_ids = sorted({int(x["prj_id"]) for x in prepared if x.get("prj_id") is not None})
        base_md5s = sorted({x["url_base_md5"] for x in prepared if x.get("url_base_md5")})
        url_map, prj_map, base_map = load_existing_maps(conn, urls, prj_ids, base_md5s)

        updates = []
        inserts = []
        touched_ids = set()

        for item in prepared:
            existing = url_map.get(item["url"])
            if not existing and item["prj_id"] is not None:
                existing = prj_map.get(int(item["prj_id"]))
            if not existing and item["url_base_md5"]:
                existing = base_map.get(item["url_base_md5"])

            if existing:
                eid = int(existing["id"])
                if eid in touched_ids:
                    continue
                touched_ids.add(eid)
                updates.append(
                    (
                        item["url"],
                        batch_date,
                        item["prj_id"],
                        item["url_base"],
                        item["url_base_md5"],
                        item["loaihinh"],
                        item["trade_type"],
                        eid,
                    )
                )
            else:
                inserts.append(
                    (
                        item["url"],
                        batch_date,
                        item["prj_id"],
                        item["url_base"],
                        item["url_base_md5"],
                        item["loaihinh"],
                        item["trade_type"],
                    )
                )

        if updates:
            cur.executemany(
                """
                UPDATE collected_links
                SET
                    url=%s,
                    batch_date=%s,
                    crawl_bo_sung=1,
                    prj_id=%s,
                    url_base=%s,
                    url_base_md5=%s,
                    loaihinh=%s,
                    trade_type=%s,
                    status=CASE
                        WHEN COALESCE(status, 'PENDING') IN ('done', 'IN_PROGRESS') THEN status
                        ELSE 'PENDING'
                    END,
                    updated_at=NOW()
                WHERE id=%s AND domain=%s
                """.replace("WHERE id=%s AND domain=%s", "WHERE id=%s AND domain='batdongsan.com.vn'"),
                updates,
            )

        if inserts:
            cur.executemany(
                """
                INSERT INTO collected_links
                    (url, domain, status, batch_date, crawl_bo_sung, prj_id, url_base, url_base_md5, loaihinh, trade_type, created_at, updated_at)
                VALUES
                    (%s, 'batdongsan.com.vn', 'PENDING', %s, 1, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                inserts,
            )

        conn.commit()
        return {"inserted": len(inserts), "updated": len(updates)}
    finally:
        conn.close()


def crawl_category(
    category_url: str,
    max_pages: int,
    start_page: int,
    delay_min: float,
    delay_max: float,
    impersonate: str,
    retry_count: int,
    retry_sleep: float,
) -> Dict[str, int]:
    base_url = normalize_category_url(category_url)
    batch_date = datetime.now().strftime("%Y%m%d")
    total_pages = 0
    total_links = 0
    total_inserted = 0
    total_updated = 0
    prev_page_urls: Optional[List[str]] = None
    session = requests.Session()
    session.headers.update(HEADERS)

    for page in range(start_page, max_pages + 1):
        url = build_page_url(base_url, page)
        print(f"\n-> Crawling {url}")
        html = fetch_html(
            session,
            url,
            impersonate=impersonate,
            retry_count=retry_count,
            retry_sleep=retry_sleep,
        )
        total_count = parse_total_count(html)
        if total_count == 0:
            print("  Count-number = 0. Stop.")
            break

        links = parse_listing_links(html)
        print(f"  Found {len(links)} links on page {page}. total_count={total_count}")

        if not links:
            print("  No links found on page. Stop.")
            break

        current_page_urls = [item["url"] for item in links if item.get("url")]
        if prev_page_urls is not None and current_page_urls == prev_page_urls:
            print("  Same links as previous page. Stop.")
            break

        stats = upsert_links(links, batch_date=batch_date)
        print(f"  inserted={stats['inserted']} updated={stats['updated']} batch_date={batch_date}")

        total_pages += 1
        total_links += len(links)
        total_inserted += stats["inserted"]
        total_updated += stats["updated"]
        prev_page_urls = current_page_urls

        sleep_s = random.uniform(delay_min, delay_max)
        print(f"  Sleeping {sleep_s:.2f}s...")
        time.sleep(sleep_s)

    return {
        "pages": total_pages,
        "links": total_links,
        "inserted": total_inserted,
        "updated": total_updated,
    }


def main():
    parser = argparse.ArgumentParser(description="Batdongsan category link crawler to collected_links.")
    parser.add_argument(
        "urls",
        nargs="+",
        help="One or more category URLs, e.g. https://batdongsan.com.vn/nha-dat-ban-xa-bau-bang-tp-ho-chi-minh",
    )
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--delay-min", type=float, default=DEFAULT_DELAY_MIN)
    parser.add_argument("--delay-max", type=float, default=DEFAULT_DELAY_MAX)
    parser.add_argument("--impersonate", default=DEFAULT_IMPERSONATE)
    parser.add_argument("--retry-count", type=int, default=DEFAULT_RETRY_COUNT)
    parser.add_argument("--retry-sleep", type=float, default=DEFAULT_RETRY_SLEEP)
    parser.add_argument(
        "--also-rent",
        action="store_true",
        help="After each sale URL, auto-crawl the matching rent URL by replacing /nha-dat-ban- with /nha-dat-cho-thue-",
    )
    args = parser.parse_args()

    if args.delay_max < args.delay_min:
        parser.error("--delay-max must be >= --delay-min")
    if args.start_page < 1:
        parser.error("--start-page must be >= 1")
    if args.retry_count < 1:
        parser.error("--retry-count must be >= 1")

    ordered_urls: List[str] = []
    seen_urls = set()
    for raw_url in args.urls:
        normalized = normalize_category_url(raw_url)
        if normalized not in seen_urls:
            seen_urls.add(normalized)
            ordered_urls.append(normalized)
        if args.also_rent:
            rent_url = derive_rent_url(normalized)
            if rent_url and rent_url not in seen_urls:
                seen_urls.add(rent_url)
                ordered_urls.append(rent_url)

    grand_pages = 0
    grand_links = 0
    grand_inserted = 0
    grand_updated = 0
    total_urls = len(ordered_urls)
    ensure_schema()

    for idx, category_url in enumerate(ordered_urls, start=1):
        print(f"\n========== [{idx}/{total_urls}] {category_url} ==========")
        effective_start_page = args.start_page if idx == 1 else 1
        try:
            stats = crawl_category(
                category_url=category_url,
                max_pages=args.max_pages,
                start_page=effective_start_page,
                delay_min=args.delay_min,
                delay_max=args.delay_max,
                impersonate=args.impersonate,
                retry_count=args.retry_count,
                retry_sleep=args.retry_sleep,
            )
        except Exception as exc:
            print(f"URL FAILED | start_page={effective_start_page} | error={exc}")
            continue
        grand_pages += stats["pages"]
        grand_links += stats["links"]
        grand_inserted += stats["inserted"]
        grand_updated += stats["updated"]
        print(
            f"URL DONE | pages={stats['pages']} | links={stats['links']} "
            f"| inserted={stats['inserted']} | updated={stats['updated']}"
        )

    print("\n=== ALL DONE ===")
    print(
        f"urls={total_urls} | pages={grand_pages} | links={grand_links} "
        f"| inserted={grand_inserted} | updated={grand_updated}"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
