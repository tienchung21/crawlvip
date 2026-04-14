#!/usr/bin/env python3
"""
Guland project crawler.

Crawler danh sách dự án từ:
  https://guland.vn/du-an
  https://guland.vn/du-an?page=N

Lấy:
  - tên dự án
  - URL dự án

Lưu vào bảng:
  duan_guland

Mặc định:
  - bắt đầu từ page 1
  - delay 3 giây
  - dừng khi page không còn link dự án nào
"""

import argparse
import os
import sys
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from curl_cffi import requests
except Exception as exc:
    raise RuntimeError("curl_cffi is required for guland_project_crawler.py") from exc

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    from database import Database


BASE_URL = "https://guland.vn"
LIST_URL = f"{BASE_URL}/du-an"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Referer": "https://guland.vn/",
}


def ensure_table(db: Database):
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS duan_guland (
                id INT AUTO_INCREMENT PRIMARY KEY,
                project_name VARCHAR(500) NOT NULL,
                project_url VARCHAR(1000) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_project_url (project_url(255)),
                KEY idx_project_name (project_name(191))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def build_page_url(page: int) -> str:
    if page <= 1:
        return LIST_URL
    return f"{LIST_URL}?page={page}"


def is_blocked(html_text: str) -> bool:
    t = (html_text or "").lower()
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


def extract_projects(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    projects = []
    seen = set()

    for a in soup.select("h3.c-prj-card__tle a[href]"):
        name = (a.get_text(" ", strip=True) or "").strip()
        href = (a.get("href") or "").strip()
        if not name or not href:
            continue
        abs_url = urljoin(BASE_URL, href)
        key = abs_url.lower()
        if key in seen:
            continue
        seen.add(key)
        projects.append((name, abs_url))

    return projects


def save_projects(db: Database, projects):
    if not projects:
        return 0
    conn = db.get_connection()
    cur = conn.cursor()
    inserted = 0
    try:
        for name, url in projects:
            cur.execute(
                """
                INSERT IGNORE INTO duan_guland (project_name, project_url)
                VALUES (%s, %s)
                """,
                (name, url),
            )
            inserted += cur.rowcount
        conn.commit()
        return inserted
    finally:
        cur.close()
        conn.close()


def run(start_page: int, delay_s: float):
    db = Database()
    ensure_table(db)
    session = requests.Session()

    page = max(1, int(start_page))
    total_found = 0
    total_inserted = 0

    print("=" * 50)
    print("Starting Guland Project Crawler")
    print(f"start_page={page}")
    print(f"delay={delay_s:.1f}s")
    print("=" * 50)

    while True:
        url = build_page_url(page)
        print(f"\n-> Crawling {url}")

        try:
            resp = session.get(url, headers=HEADERS, impersonate="chrome124", timeout=40)
        except Exception as e:
            print(f"  [x] Request failed: {e}")
            break

        if resp.status_code != 200:
            print(f"  [x] HTTP {resp.status_code}")
            break

        html_text = resp.text or ""
        if is_blocked(html_text):
            print("  [x] Block/verify page detected. Stop.")
            break

        projects = extract_projects(html_text)
        found = len(projects)
        print(f"  Found {found} project links.")

        if found == 0:
            print("  [STOP] No project links found.")
            break

        inserted = save_projects(db, projects)
        total_found += found
        total_inserted += inserted
        print(f"  Inserted {inserted} new rows.")

        page += 1
        print(f"  Sleeping {delay_s:.1f}s...")
        time.sleep(delay_s)

    print("\n" + "=" * 50)
    print(f"Finished. total_found={total_found} total_inserted={total_inserted}")
    print("=" * 50)


def main():
    ap = argparse.ArgumentParser(description="Crawl Guland project list into duan_guland")
    ap.add_argument("--start-page", type=int, default=1, help="Start page (default: 1)")
    ap.add_argument("--delay", type=float, default=3.0, help="Delay in seconds between pages (default: 3)")
    args = ap.parse_args()

    if args.start_page <= 0:
        raise SystemExit("--start-page must be > 0")
    if args.delay < 0:
        raise SystemExit("--delay must be >= 0")

    run(start_page=args.start_page, delay_s=args.delay)


if __name__ == "__main__":
    main()
