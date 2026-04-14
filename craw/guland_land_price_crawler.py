#!/usr/bin/env python3
"""
Crawler bang gia dat Guland.

Mac dinh crawl:
  https://guland.vn/bang-gia-dat/0?page=1 .. ?page=4293

Schema dich:
  price_land_guland(
    city,
    ward_district,
    street,
    street_segment,
    land_type,
    land_subtype,
    price_1,
    price_2,
    price_3,
    price_4
  )
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from bs4 import BeautifulSoup

try:
    from curl_cffi import requests
except Exception as exc:
    raise RuntimeError("curl_cffi is required for guland_land_price_crawler.py") from exc

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    from database import Database


BASE_URL = "https://guland.vn/bang-gia-dat/0?page={page}"
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_FILE = LOG_DIR / "guland_land_price.log"
CHECKPOINT_FILE = LOG_DIR / "guland_land_price_checkpoint.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Referer": "https://guland.vn/",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log_line(message: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = message.rstrip()
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def save_checkpoint(payload: Dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["updated_at"] = utc_now_iso()
    CHECKPOINT_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_table(db: Database) -> None:
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS price_land_guland (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    city VARCHAR(255) NULL,
                    ward_district VARCHAR(255) NULL,
                    street VARCHAR(1000) NULL,
                    street_segment TEXT NULL,
                    land_type VARCHAR(500) NULL,
                    land_subtype VARCHAR(500) NULL,
                    price_1 VARCHAR(50) NULL,
                    price_2 VARCHAR(50) NULL,
                    price_3 VARCHAR(50) NULL,
                    price_4 VARCHAR(50) NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_price_land_guland_city (city),
                    INDEX idx_price_land_guland_ward_district (ward_district)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
        conn.commit()
    finally:
        conn.close()


def fetch_page(session: requests.Session, page: int, timeout: int, max_retries: int) -> str:
    url = BASE_URL.format(page=page)
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=HEADERS, impersonate="chrome124", timeout=timeout)
            resp.raise_for_status()
            return resp.text or ""
        except Exception as exc:
            last_error = exc
            sleep_s = min(2 * attempt, 8)
            log_line(f"[RETRY] page={page} attempt={attempt}/{max_retries} sleep={sleep_s}s error={exc}")
            time.sleep(sleep_s)
    raise RuntimeError(f"fetch_page_failed page={page}: {last_error}")


def text_or_none(node) -> str | None:
    if not node:
        return None
    text = node.get_text(" ", strip=True)
    return text or None


def parse_rows(html_text: str) -> List[Dict[str, str | None]]:
    soup = BeautifulSoup(html_text, "html.parser")
    rows = []
    for tr in soup.select("table.table.table-bordered.table-striped > tbody > tr"):
        tds = tr.select("td")
        if len(tds) < 7:
            continue
        row = {
            "city": text_or_none(tds[0].select_one("div:nth-of-type(1) > a")),
            "ward_district": text_or_none(tds[0].select_one("div:nth-of-type(2) > a")),
            "street": text_or_none(tds[1].select_one(".area-detail > a > b")),
            "street_segment": text_or_none(tds[1].select_one(".area-detail > .sub-text")),
            "land_type": text_or_none(tds[2].select_one(".area-type > .main-text")),
            "land_subtype": text_or_none(tds[2].select_one(".area-type > .sub-text")),
            "price_1": text_or_none(tds[3]),
            "price_2": text_or_none(tds[4]),
            "price_3": text_or_none(tds[5]),
            "price_4": text_or_none(tds[6]),
        }
        rows.append(row)
    return rows


def insert_rows(db: Database, rows: List[Dict[str, str | None]]) -> int:
    if not rows:
        return 0
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO price_land_guland
                (city, ward_district, street, street_segment, land_type, land_subtype, price_1, price_2, price_3, price_4)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        row["city"],
                        row["ward_district"],
                        row["street"],
                        row["street_segment"],
                        row["land_type"],
                        row["land_subtype"],
                        row["price_1"],
                        row["price_2"],
                        row["price_3"],
                        row["price_4"],
                    )
                    for row in rows
                ],
            )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Crawl Guland land price table into price_land_guland")
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--end-page", type=int, default=4293)
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    db = Database()
    ensure_table(db)
    session = requests.Session()

    start_page = args.start_page
    if args.resume and CHECKPOINT_FILE.exists():
        try:
            checkpoint = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
            next_page = int(checkpoint.get("next_page") or start_page)
            if next_page > start_page:
                start_page = next_page
        except Exception:
            pass

    total_seen = 0
    total_saved = 0
    log_line(
        f"[START] start_page={start_page} end_page={args.end_page} delay={args.delay} "
        f"dry_run={args.dry_run}"
    )

    for page in range(start_page, args.end_page + 1):
        url = BASE_URL.format(page=page)
        html_text = fetch_page(session, page, timeout=args.timeout, max_retries=args.max_retries)
        rows = parse_rows(html_text)
        total_seen += len(rows)
        log_line(f"[PAGE] page={page}/{args.end_page} rows={len(rows)} url={url}")

        if args.dry_run:
            saved = 0
        else:
            saved = insert_rows(db, rows)
            total_saved += saved

        save_checkpoint(
            {
                "status": "page_done",
                "page": page,
                "next_page": page + 1,
                "rows": len(rows),
                "saved": saved,
                "url": url,
                "dry_run": args.dry_run,
            }
        )

        if page < args.end_page and args.delay > 0:
            log_line(f"[SLEEP] next_page={page + 1} seconds={args.delay}")
            time.sleep(args.delay)

    log_line(f"[DONE] seen={total_seen} saved={total_saved} dry_run={args.dry_run}")
    save_checkpoint(
        {
            "status": "done",
            "page": args.end_page,
            "next_page": args.end_page + 1,
            "rows": total_seen,
            "saved": total_saved,
            "dry_run": args.dry_run,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
