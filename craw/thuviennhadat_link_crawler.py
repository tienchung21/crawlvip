#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crawl full listing links from Thư Viện Nhà Đất category URLs listed in a markdown file.

- Input categories: from Test/crawl_thuviennhadat.md
- Pagination: ?trang=N
- Extract detail links: URLs matching *-pst<id>.html
- Save to MySQL table collected_links via Database.add_collected_links
"""

from __future__ import annotations

import argparse
import html
import random
import re
import time
from pathlib import Path
from typing import List, Dict
from urllib.parse import urlsplit, urlunsplit

from curl_cffi import requests

from database import Database

BASE_DOMAIN = "thuviennhadat.vn"
BASE_URL = f"https://{BASE_DOMAIN}"

# Match both relative and absolute detail links ending with -pst123.html
DETAIL_LINK_RE = re.compile(r'href=["\']([^"\']*?-pst\d+\.html)["\']', re.IGNORECASE)


def normalize_category_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.startswith("/"):
        raw = BASE_URL + raw
    if not raw.startswith("http"):
        return ""
    # strip query/fragment for category root
    sp = urlsplit(raw)
    return urlunsplit((sp.scheme, sp.netloc, sp.path.rstrip("/"), "", ""))


def slug_from_url(url: str) -> str:
    sp = urlsplit(url)
    return sp.path.strip("/")


def infer_trade_type(slug: str) -> str:
    # Bán -> s ; Cho thuê -> u
    if slug.startswith("cho-thue-"):
        return "u"
    return "s"


def load_category_urls_from_md(md_path: Path) -> List[str]:
    urls: List[str] = []
    seen = set()

    text = md_path.read_text(encoding="utf-8", errors="ignore")
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln:
            continue

        cand = ""
        if ln.startswith("http://") or ln.startswith("https://"):
            cand = ln.split()[0].strip()
        elif ln.startswith("/") and "toan-quoc" in ln:
            cand = BASE_URL + ln.split()[0].strip()

        if not cand:
            continue

        norm = normalize_category_url(cand)
        if not norm:
            continue
        if "thuviennhadat.vn" not in norm:
            continue

        slug = slug_from_url(norm)
        if not slug:
            continue
        if slug in seen:
            continue

        seen.add(slug)
        urls.append(norm)

    return urls


def page_url(base_category_url: str, page: int) -> str:
    # Site accepts ?trang=N
    return f"{base_category_url}?trang={page}"


def extract_detail_links(html_text: str) -> List[str]:
    if not html_text:
        return []

    links: List[str] = []
    seen = set()

    for m in DETAIL_LINK_RE.findall(html_text):
        href = html.unescape((m or "").strip())
        if not href:
            continue
        if href.startswith("/"):
            href = BASE_URL + href
        elif href.startswith("//"):
            href = "https:" + href
        elif href.startswith("http://") or href.startswith("https://"):
            pass
        else:
            # weird relative case
            href = BASE_URL + "/" + href.lstrip("/")

        if BASE_DOMAIN not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        links.append(href)

    return links


def crawl_category(
    db: Database,
    session: requests.Session,
    category_url: str,
    max_pages: int,
    min_delay: float,
    max_delay: float,
    stop_empty_pages: int,
    stop_no_new_pages: int,
) -> Dict[str, int]:
    slug = slug_from_url(category_url)
    trade_type = infer_trade_type(slug)

    print(f"[CATEGORY] slug={slug} trade={trade_type} base={category_url}")

    total_seen = 0
    total_added = 0
    empty_pages = 0
    no_new_pages = 0

    for page in range(1, max_pages + 1):
        url = page_url(category_url, page)

        try:
            resp = session.get(url, timeout=35)
            code = resp.status_code
            text = resp.text if code == 200 else ""
        except Exception as e:
            print(f"  [ERR] page={page} fetch_failed={e}")
            break

        if code != 200:
            print(f"  [STOP_HTTP] page={page} status={code}")
            break

        links = extract_detail_links(text)
        found = len(links)
        total_seen += found
        print(f"  [PAGE] page={page} found={found}")

        if found == 0:
            empty_pages += 1
            if empty_pages >= stop_empty_pages:
                print(f"  [STOP_EMPTY] empty_pages={empty_pages}")
                break
            time.sleep(random.uniform(min_delay, max_delay))
            continue

        # reset empty counter when found > 0
        empty_pages = 0

        added = db.add_collected_links(
            links_list=links,
            domain=BASE_DOMAIN,
            loaihinh=slug,
            trade_type=trade_type,
        )
        dup = found - added
        total_added += added

        if added == 0:
            no_new_pages += 1
        else:
            no_new_pages = 0

        print(f"    [ADD] added={added} dup={dup} no_new_pages={no_new_pages}/{stop_no_new_pages}")

        if no_new_pages >= stop_no_new_pages:
            print(f"  [STOP_NO_NEW] pages={no_new_pages}")
            break

        if page < max_pages:
            slp = random.uniform(min_delay, max_delay)
            print(f"    [SLEEP] {slp:.2f}s")
            time.sleep(slp)

    print(f"[DONE_CATEGORY] slug={slug} seen={total_seen} added={total_added}")
    return {"seen": total_seen, "added": total_added}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Thuviennhadat full link crawler")
    ap.add_argument("--md-path", default="/home/chungnt/crawlvip/Test/crawl_thuviennhadat.md", help="Path to markdown containing category URLs")
    ap.add_argument("--max-pages", type=int, default=500, help="Max pages per category")
    ap.add_argument("--min-delay", type=float, default=0.3, help="Min delay between pages")
    ap.add_argument("--max-delay", type=float, default=0.7, help="Max delay between pages")
    ap.add_argument("--stop-empty-pages", type=int, default=2, help="Stop category after N consecutive empty pages")
    ap.add_argument("--stop-no-new-pages", type=int, default=6, help="Stop category after N consecutive pages added=0")
    ap.add_argument("--start-from-slug", default="", help="Resume from this slug (inclusive)")
    ap.add_argument("--category-limit", type=int, default=0, help="Only crawl first N categories after start-from-slug")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    md_path = Path(args.md_path)

    if not md_path.exists():
        print(f"[FATAL] md not found: {md_path}")
        return 1

    categories = load_category_urls_from_md(md_path)
    if not categories:
        print("[FATAL] no category urls found")
        return 1

    # Resume logic
    if args.start_from_slug:
        start_idx = 0
        target = args.start_from_slug.strip("/")
        for i, u in enumerate(categories):
            if slug_from_url(u) == target:
                start_idx = i
                break
        categories = categories[start_idx:]

    if args.category_limit and args.category_limit > 0:
        categories = categories[: args.category_limit]

    print(f"[START] categories={len(categories)} max_pages={args.max_pages} delay={args.min_delay}-{args.max_delay}")

    db = Database()
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
            "Referer": BASE_URL + "/",
        }
    )

    grand_seen = 0
    grand_added = 0

    for idx, cat in enumerate(categories, 1):
        print(f"\n[{idx}/{len(categories)}] >>> {cat}")
        rs = crawl_category(
            db=db,
            session=session,
            category_url=cat,
            max_pages=args.max_pages,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
            stop_empty_pages=args.stop_empty_pages,
            stop_no_new_pages=args.stop_no_new_pages,
        )
        grand_seen += rs.get("seen", 0)
        grand_added += rs.get("added", 0)

    print(f"\n[DONE] total_seen_links={grand_seen} total_added={grand_added}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
