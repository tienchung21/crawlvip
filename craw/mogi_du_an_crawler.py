#!/usr/bin/env python3
"""
Crawl danh sach du an tu Mogi (https://mogi.vn/du-an?cp=N) bang curl_cffi,
chi lay ten du an theo selector: h2.project-title

Luu vao MySQL table: du_mogi

Usage:
  python3 craw/mogi_du_an_crawler.py --start-page 1 --max-page 547
  python3 craw/mogi_du_an_crawler.py --max-page 10 --delay-min 0.8 --delay-max 1.6
  python3 craw/mogi_du_an_crawler.py --proxy http://IP:PORT
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from typing import Iterable, List, Optional, Tuple

from curl_cffi import requests
from lxml import html as lxml_html

# Import Database from craw/database.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402


BASE_URL = "https://mogi.vn/du-an"
DEFAULT_IMPERSONATE = "chrome124"


def build_page_url(page: int) -> str:
    return f"{BASE_URL}?cp={page}"


def ensure_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS du_mogi (
                id INT AUTO_INCREMENT PRIMARY KEY,
                project_name VARCHAR(512) NOT NULL,
                page INT NOT NULL,
                source_url VARCHAR(2048) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_du_mogi_project_name (project_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def parse_project_names(html: str) -> List[str]:
    # Avoid bs4 dependency; lxml is already used elsewhere in this repo.
    try:
        tree = lxml_html.fromstring(html)
    except Exception:
        return []

    # Match: <h2 class="project-title">...</h2>
    nodes = tree.xpath(
        "//h2[contains(concat(' ', normalize-space(@class), ' '), ' project-title ')]"
    )
    out: List[str] = []
    for n in nodes:
        try:
            name = n.text_content().strip()
        except Exception:
            name = ""
        if name:
            out.append(name)
    return out


def insert_projects(
    db: Database, rows: Iterable[Tuple[str, int, str]]
) -> int:
    conn = db.get_connection()
    cur = conn.cursor()
    inserted = 0
    try:
        sql = """
            INSERT IGNORE INTO du_mogi (project_name, page, source_url)
            VALUES (%s, %s, %s)
        """
        for name, page, source_url in rows:
            cur.execute(sql, (name, page, source_url))
            if getattr(cur, "rowcount", 0) > 0:
                inserted += 1
        conn.commit()
    finally:
        try:
            cur.close()
        finally:
            conn.close()
    return inserted


def fetch_page(
    session: requests.Session,
    url: str,
    impersonate: str,
    timeout: int,
    proxies: Optional[dict],
    retries: int,
) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                url,
                impersonate=impersonate,
                timeout=timeout,
                proxies=proxies,
            )
            # Mogi co the tra ve 403/429 neu bi block.
            if resp.status_code == 200 and resp.text:
                return resp.text
            raise RuntimeError(f"HTTP {resp.status_code}")
        except Exception as e:
            last_err = e
            # Backoff nho, tranh spam.
            time.sleep(min(10.0, 0.8 * attempt + random.uniform(0.0, 0.6)))
    raise RuntimeError(f"Fetch failed after {retries} retries: {url}. Last error: {last_err}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Mogi du an crawler (curl_cffi)")
    parser.add_argument("--start-page", type=int, default=1, help="Trang bat dau (cp)")
    parser.add_argument("--max-page", type=int, default=547, help="Trang ket thuc (cp)")
    parser.add_argument("--delay-min", type=float, default=0.8, help="Delay toi thieu giua cac page (s)")
    parser.add_argument("--delay-max", type=float, default=1.6, help="Delay toi da giua cac page (s)")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout (s)")
    parser.add_argument("--retries", type=int, default=3, help="So lan retry moi page")
    parser.add_argument("--proxy", type=str, default="", help="Proxy URL, vi du: http://IP:PORT")
    parser.add_argument("--impersonate", type=str, default=DEFAULT_IMPERSONATE, help="curl_cffi impersonate profile")
    parser.add_argument("--stop-empty", type=int, default=2, help="Dung neu gap N trang lien tiep khong co du an")
    args = parser.parse_args()

    if args.start_page < 1:
        print("start-page phai >= 1")
        return 2
    if args.max_page < args.start_page:
        print("max-page phai >= start-page")
        return 2
    if args.delay_max < args.delay_min:
        print("delay-max phai >= delay-min")
        return 2

    proxies = None
    if args.proxy.strip():
        proxies = {"http": args.proxy.strip(), "https": args.proxy.strip()}

    db = Database()
    ensure_table(db)

    session = requests.Session()

    total_found = 0
    total_inserted = 0
    empty_streak = 0

    for page in range(args.start_page, args.max_page + 1):
        url = build_page_url(page)
        print(f"[cp={page}] Fetch: {url}")

        try:
            html = fetch_page(
                session=session,
                url=url,
                impersonate=args.impersonate,
                timeout=args.timeout,
                proxies=proxies,
                retries=args.retries,
            )
        except Exception as e:
            print(f"[cp={page}] Loi fetch: {e}")
            # Neu bi block/tam thoi, van di tiep trang sau.
            time.sleep(random.uniform(args.delay_min, args.delay_max))
            continue

        names = parse_project_names(html)
        found = len(names)
        total_found += found

        if found == 0:
            empty_streak += 1
            print(f"[cp={page}] Khong tim thay h2.project-title (empty_streak={empty_streak}/{args.stop_empty})")
            if empty_streak >= args.stop_empty:
                print("Dung do gap qua nhieu trang rong lien tiep.")
                break
        else:
            empty_streak = 0
            inserted = insert_projects(db, ((n, page, url) for n in names))
            total_inserted += inserted
            print(f"[cp={page}] Found={found}, Inserted={inserted}")

        time.sleep(random.uniform(args.delay_min, args.delay_max))

    print(f"COMPLETED. Total found: {total_found}. Total inserted: {total_inserted}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
