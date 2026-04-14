#!/usr/bin/env python3
"""
Guland link crawler.

Usage examples:
  python3 -u craw/guland_link_crawler.py thai-nguyen
  python3 -u craw/guland_link_crawler.py quang-ninh,hue,dak-lak --max-pages 200 --stop-no-new-pages 6
"""

import argparse
import os
import random
import sys
import time
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from curl_cffi import requests
except Exception as exc:
    raise RuntimeError("curl_cffi is required for guland_link_crawler.py") from exc

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    from database import Database


BASE_URL = "https://guland.vn"
DOMAIN = "guland.vn"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Referer": "https://guland.vn/",
}


def parse_province_slugs(raw: str) -> List[str]:
    items = []
    seen = set()
    for part in str(raw or "").split(","):
        slug = part.strip().strip("/").lower()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        items.append(slug)
    return items


def build_category_url(province_slug: str, trade_type: str) -> str:
    if trade_type == "s":
        return f"{BASE_URL}/mua-ban-bat-dong-san-{province_slug}"
    return f"{BASE_URL}/cho-thue-bat-dong-san-{province_slug}"


def build_page_url(category_url: str, page: int) -> str:
    if page <= 1:
        return category_url
    return f"{category_url}?page={page}"


def extract_links(html_text: str) -> List[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    out = []
    for a in soup.select(".c-sdb-card__tle a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(BASE_URL, href)
        if "/post/" in abs_url:
            out.append(abs_url)
    # keep order, remove duplicates
    seen = set()
    dedup = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        dedup.append(x)
    return dedup


def is_blocked(html_text: str) -> bool:
    t = (html_text or "").lower()
    # Avoid false-positive: normal pages may contain "cloudflare" script strings.
    blocked_markers = [
        "attention required!",
        "verify you are human",
        "please verify you are a human",
        "/cdn-cgi/challenge-platform/",
        "cf-chl-bypass",
        "g-recaptcha",
        "hcaptcha",
        "captcha",
    ]
    return any(m in t for m in blocked_markers)


def crawl_category(
    db: Database,
    session: requests.Session,
    province_slug: str,
    trade_type: str,
    max_pages: int,
    stop_no_new_pages: int,
    delay_min: float,
    delay_max: float,
) -> int:
    category_url = build_category_url(province_slug, trade_type)
    print(f"Category URL: {category_url} | trade_type={trade_type}")
    total_added = 0
    no_new_streak = 0

    for page in range(1, max_pages + 1):
        url = build_page_url(category_url, page)
        print(f"\n-> Crawling {url}")

        try:
            resp = session.get(url, headers=HEADERS, impersonate="chrome124", timeout=40)
        except Exception as e:
            print(f"  [x] Request failed: {e}")
            no_new_streak += 1
            if no_new_streak >= stop_no_new_pages:
                print(
                    f"  [STOP] >={stop_no_new_pages} consecutive pages with no new links "
                    f"(including request error)."
                )
                break
            continue

        if resp.status_code != 200:
            print(f"  [x] HTTP {resp.status_code}")
            no_new_streak += 1
            if no_new_streak >= stop_no_new_pages:
                print(
                    f"  [STOP] >={stop_no_new_pages} consecutive pages with no new links "
                    f"(including HTTP error)."
                )
                break
            continue

        html_text = resp.text or ""
        if is_blocked(html_text):
            print("  [x] Block/verify page detected. Stop this category.")
            break

        links = extract_links(html_text)
        found = len(links)
        print(f"  Found {found} links on page {page}.")

        if found == 0:
            no_new_streak += 1
            print(f"  No links found. no_new_streak={no_new_streak}")
        else:
            added = db.add_collected_links(
                links,
                domain=DOMAIN,
                loaihinh="Guland",
                trade_type=trade_type,
                city_name=province_slug,
            )
            total_added += added
            if added == 0:
                no_new_streak += 1
            else:
                no_new_streak = 0
            print(f"  Added {added} new links. no_new_streak={no_new_streak}")

        if no_new_streak >= stop_no_new_pages:
            print(
                f"  [STOP] >={stop_no_new_pages} consecutive pages with no new links "
                "(including empty pages)."
            )
            break

        sleep_s = random.uniform(delay_min, delay_max)
        print(f"  Sleeping {sleep_s:.2f}s...")
        time.sleep(sleep_s)

    return total_added


def run(
    provinces_csv: str,
    max_pages: int,
    stop_no_new_pages: int,
    delay_min: float,
    delay_max: float,
) -> None:
    provinces = parse_province_slugs(provinces_csv)
    if not provinces:
        raise SystemExit("No valid province slug provided.")

    db = Database()
    session = requests.Session()

    print("=" * 50)
    print("Starting Guland Link Crawler")
    print(f"Provinces: {', '.join(provinces)}")
    print(f"max_pages={max_pages} stop_no_new_pages={stop_no_new_pages}")
    print("=" * 50)

    grand_total = 0
    for idx, province in enumerate(provinces, start=1):
        print(f"\n########## [{idx}/{len(provinces)}] START {province} ##########")
        # phase 1: sale
        grand_total += crawl_category(
            db=db,
            session=session,
            province_slug=province,
            trade_type="s",
            max_pages=max_pages,
            stop_no_new_pages=stop_no_new_pages,
            delay_min=delay_min,
            delay_max=delay_max,
        )
        # phase 2: rent
        grand_total += crawl_category(
            db=db,
            session=session,
            province_slug=province,
            trade_type="u",
            max_pages=max_pages,
            stop_no_new_pages=stop_no_new_pages,
            delay_min=delay_min,
            delay_max=delay_max,
        )
        print(f"########## [{idx}/{len(provinces)}] END {province} ##########")

    print("\n" + "=" * 50)
    print(f"Crawl Finished. Total new links added: {grand_total}")
    print("=" * 50)


def main() -> int:
    parser = argparse.ArgumentParser(description="Guland link crawler by province slug")
    parser.add_argument("provinces", help="Province slug(s), e.g. thai-nguyen or quang-ninh,hue,dak-lak")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=200,
        help="Max pages per category (default: 200)",
    )
    parser.add_argument(
        "--stop-no-new-pages",
        type=int,
        default=2,
        help="Stop category when >N consecutive pages have no new links (default: 2)",
    )
    parser.add_argument("--delay-min", type=float, default=9.0, help="Delay min seconds between pages")
    parser.add_argument("--delay-max", type=float, default=12.0, help="Delay max seconds between pages")
    args = parser.parse_args()

    if args.max_pages <= 0:
        raise SystemExit("--max-pages must be > 0")
    if args.stop_no_new_pages < 0:
        raise SystemExit("--stop-no-new-pages must be >= 0")
    if args.delay_min < 0 or args.delay_max < 0 or args.delay_min > args.delay_max:
        raise SystemExit("Invalid delay range")

    run(
        provinces_csv=args.provinces,
        max_pages=args.max_pages,
        stop_no_new_pages=args.stop_no_new_pages,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
