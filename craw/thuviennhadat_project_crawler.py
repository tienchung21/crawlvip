#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crawl du an from thuviennhadat.vn and save to location_thuviennhadat.
"""

from __future__ import annotations

import argparse
import random
import re
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import pymysql
from bs4 import BeautifulSoup
from curl_cffi import requests

BASE = "https://thuviennhadat.vn"
LIST_URL = BASE + "/du-an-bat-dong-san?trang={page}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Referer": BASE + "/",
}


def get_conn():
    return pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="",
        database="craw_db",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def ensure_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS location_thuviennhadat (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        project_id BIGINT NULL,
        project_name VARCHAR(512) NULL,
        project_slug VARCHAR(512) NULL,
        project_url VARCHAR(1024) NOT NULL,
        status_text VARCHAR(255) NULL,
        address_text TEXT NULL,
        description_text MEDIUMTEXT NULL,
        image_url TEXT NULL,
        image_count INT NULL,
        source_page INT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uq_project_url (project_url),
        KEY idx_project_id (project_id),
        KEY idx_project_slug (project_slug)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fetch_page(page: int, retries: int = 3) -> Optional[str]:
    url = LIST_URL.format(page=page)
    last_err = None
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, impersonate="chrome124", timeout=40)
            if r.status_code == 200:
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        if i < retries:
            time.sleep(0.5 * i)
    print(f"[ERR] page={page} fetch_failed={last_err}")
    return None


def text_or_none(node) -> Optional[str]:
    if not node:
        return None
    t = " ".join(node.stripped_strings).strip()
    return t or None


def parse_projects(html_text: str, page: int) -> List[Dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    items = soup.select("div.ui.divided.items.duan-index-project-tag > div.item")

    projects: List[Dict] = []
    seen_url = set()

    for item in items:
        a = item.select_one('a[href*="/du-an-bat-dong-san/"]')
        if not a:
            continue
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(BASE, href)
        if full_url in seen_url:
            continue
        seen_url.add(full_url)

        m = re.search(r"/du-an-bat-dong-san/([^/]+?)-(\d+)\.html$", href)
        slug = m.group(1) if m else None
        project_id = int(m.group(2)) if m else None

        project_name = (
            (item.select_one(".duan-index-project-title") and item.select_one(".duan-index-project-title").get("title"))
            or text_or_none(item.select_one(".duan-index-project-title"))
            or a.get("title")
        )
        status_text = text_or_none(item.select_one(".duan-index-status-title"))
        address_text = text_or_none(item.select_one(".duan-index-project-location .location"))
        description_text = text_or_none(item.select_one(".duan-index-project-description"))

        img = item.select_one("div.image img[src]")
        image_url = (img.get("src") or "").strip() if img else None
        if image_url:
            image_url = urljoin(BASE, image_url)

        image_count = None
        c = text_or_none(item.select_one(".duan-index-more-images .count"))
        if c and c.isdigit():
            image_count = int(c)

        projects.append(
            {
                "project_id": project_id,
                "project_name": project_name,
                "project_slug": slug,
                "project_url": full_url,
                "status_text": status_text,
                "address_text": address_text,
                "description_text": description_text,
                "image_url": image_url,
                "image_count": image_count,
                "source_page": page,
            }
        )

    return projects


def upsert_projects(conn, rows: List[Dict]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO location_thuviennhadat (
        project_id, project_name, project_slug, project_url,
        status_text, address_text, description_text,
        image_url, image_count, source_page
    ) VALUES (
        %(project_id)s, %(project_name)s, %(project_slug)s, %(project_url)s,
        %(status_text)s, %(address_text)s, %(description_text)s,
        %(image_url)s, %(image_count)s, %(source_page)s
    )
    ON DUPLICATE KEY UPDATE
        project_id=VALUES(project_id),
        project_name=VALUES(project_name),
        project_slug=VALUES(project_slug),
        status_text=VALUES(status_text),
        address_text=VALUES(address_text),
        description_text=VALUES(description_text),
        image_url=VALUES(image_url),
        image_count=VALUES(image_count),
        source_page=VALUES(source_page),
        updated_at=CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def run(max_pages: int, min_delay: float, max_delay: float, stop_empty_pages: int):
    conn = get_conn()
    ensure_table(conn)

    total_pages = 0
    total_items = 0
    total_upsert = 0
    empty_streak = 0

    try:
        for page in range(1, max_pages + 1):
            html_text = fetch_page(page)
            if not html_text:
                print(f"[STOP] page={page} fetch_failed")
                break

            rows = parse_projects(html_text, page)
            found = len(rows)
            total_pages += 1
            total_items += found

            if found == 0:
                empty_streak += 1
                print(f"[PAGE] {page}: found=0 empty_streak={empty_streak}/{stop_empty_pages}")
                if empty_streak >= stop_empty_pages:
                    print(f"[STOP_EMPTY] page={page}")
                    break
            else:
                empty_streak = 0
                n = upsert_projects(conn, rows)
                total_upsert += n
                print(f"[PAGE] {page}: found={found} upsert={n}")

            if page < max_pages:
                slp = random.uniform(min_delay, max_delay)
                print(f"  [SLEEP] {slp:.2f}s")
                time.sleep(slp)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM location_thuviennhadat")
            total_db = cur.fetchone()["c"]
        print(f"[DONE] pages={total_pages} parsed={total_items} upsert={total_upsert} total_in_table={total_db}")
    finally:
        conn.close()


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=500)
    ap.add_argument("--min-delay", type=float, default=0.2)
    ap.add_argument("--max-delay", type=float, default=0.4)
    ap.add_argument("--stop-empty-pages", type=int, default=2)
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        max_pages=args.max_pages,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        stop_empty_pages=args.stop_empty_pages,
    )
