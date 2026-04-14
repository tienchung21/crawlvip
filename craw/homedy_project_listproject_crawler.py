#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime
import math
from typing import Dict, List, Optional, Tuple

import pymysql
from curl_cffi import requests

API_URL = "https://homedy.com/Maps/ListProject"


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Crawl Homedy Maps/ListProject -> duan_homedy")
    p.add_argument("--db-host", default="localhost")
    p.add_argument("--db-port", type=int, default=3306)
    p.add_argument("--db-user", default="root")
    p.add_argument("--db-pass", default="")
    p.add_argument("--db-name", default="craw_db")
    p.add_argument("--sell-type", type=int, default=1)
    p.add_argument("--url-type", type=int, default=1)
    p.add_argument("--start-page", type=int, default=1)
    p.add_argument("--page-size", type=int, default=200)
    p.add_argument("--max-pages", type=int, default=0, help="0 = no limit")
    p.add_argument("--known-total", type=int, default=0, help="Neu biet tong (vd 5019) thi truyen vao de tinh max_page")
    p.add_argument("--delay", type=float, default=0.2)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--stop-empty-pages", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def db_conn(args: argparse.Namespace):
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def ensure_table(conn) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS duan_homedy (
      id BIGINT NOT NULL AUTO_INCREMENT,
      project_id BIGINT NOT NULL,
      homedy_id BIGINT DEFAULT NULL,
      project_name VARCHAR(500) DEFAULT NULL,
      project_url VARCHAR(1000) DEFAULT NULL,
      city_id INT DEFAULT NULL,
      district_id INT DEFAULT NULL,
      ward_id INT DEFAULT NULL,
      street_id INT DEFAULT NULL,
      created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_project_id (project_id),
      KEY idx_homedy_id (homedy_id),
      KEY idx_project_name (project_name(191))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = str(url).strip()
    if not u:
        return None
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if not u.startswith("/"):
        u = "/" + u
    return "https://homedy.com" + u


def as_int(v) -> Optional[int]:
    try:
        if v in (None, "", "null"):
            return None
        return int(v)
    except Exception:
        return None


def fetch_page(
    page_index: int,
    page_size: int,
    sell_type: int,
    url_type: int,
    retries: int,
) -> Tuple[Optional[List[Dict]], Optional[int], Optional[str]]:
    params = {
        "SellType": sell_type,
        "UrlType": url_type,
        "ProjectId": 0,
        "CategoryId": 0,
        "Latitude": 0,
        "Longitude": 0,
        "Distance": 0,
        "PageIndex": page_index,
        "PageSize": page_size,
    }
    session = requests.Session()
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(API_URL, params=params, timeout=30, impersonate="chrome124")
            if r.status_code != 200:
                last_err = f"http_{r.status_code}"
            else:
                obj = r.json()
                arr = obj.get("Projects")
                if isinstance(arr, list):
                    total_val = obj.get("Total")
                    total = as_int(total_val)
                    return arr, total, None
                last_err = "invalid_projects"
        except Exception as ex:
            last_err = f"{type(ex).__name__}: {ex}"
        if attempt < retries:
            time.sleep(min(attempt, 3))
    return None, None, last_err


def probe_total(sell_type: int, url_type: int, retries: int) -> Optional[int]:
    _, total, _ = fetch_page(
        page_index=1,
        page_size=10,
        sell_type=sell_type,
        url_type=url_type,
        retries=retries,
    )
    if total and total > 0:
        return total
    return None


def upsert_project(conn, item: Dict, dry_run: bool) -> Tuple[bool, str]:
    project_id = as_int(item.get("Code"))
    if not project_id:
        return False, "missing_project_code"
    homedy_id = as_int(item.get("Id"))
    name = (item.get("Name") or "").strip() or None
    url = normalize_url(item.get("Url"))
    city_id = as_int(item.get("CityId"))
    district_id = as_int(item.get("DistrictId"))
    ward_id = as_int(item.get("WardId"))
    street_id = as_int(item.get("StreetId"))

    if dry_run:
        return True, f"dry_run pid={project_id} name={name}"

    sql = """
    INSERT INTO duan_homedy (
      project_id, homedy_id, project_name, project_url, city_id, district_id, ward_id, street_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      homedy_id = VALUES(homedy_id),
      project_name = VALUES(project_name),
      project_url = VALUES(project_url),
      city_id = VALUES(city_id),
      district_id = VALUES(district_id),
      ward_id = VALUES(ward_id),
      street_id = VALUES(street_id),
      updated_at = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        cur.execute(sql, (project_id, homedy_id, name, url, city_id, district_id, ward_id, street_id))
    return True, f"saved pid={project_id} name={name}"


def main() -> int:
    args = parse_args()
    conn = db_conn(args)
    ensure_table(conn)

    seen = saved = fail = 0
    empty_streak = 0
    discovered_total: Optional[int] = args.known_total if args.known_total > 0 else None
    max_page_by_total: Optional[int] = (
        math.ceil(args.known_total / args.page_size) if args.known_total > 0 else None
    )
    if discovered_total:
        print(f"[TOTAL_KNOWN] total={discovered_total} page_size={args.page_size} max_page={max_page_by_total}")
    page = max(args.start_page, 1)

    while True:
        if args.max_pages > 0 and (page - args.start_page + 1) > args.max_pages:
            break

        projects, total, err = fetch_page(
            page_index=page,
            page_size=args.page_size,
            sell_type=args.sell_type,
            url_type=args.url_type,
            retries=args.retries,
        )
        if discovered_total is None and total and total > 0:
            discovered_total = total
            max_page_by_total = math.ceil(total / args.page_size)
            print(f"[TOTAL] total={discovered_total} page_size={args.page_size} max_page={max_page_by_total}")
        elif discovered_total is None:
            # Fallback: mot so truong hop API tra Total=0 khi PageSize lon
            probed = probe_total(args.sell_type, args.url_type, args.retries)
            if probed and probed > 0:
                discovered_total = probed
                max_page_by_total = math.ceil(probed / args.page_size)
                print(
                    f"[TOTAL_PROBE] total={discovered_total} (probe page_size=10) "
                    f"crawl_page_size={args.page_size} max_page={max_page_by_total}"
                )
        if projects is None:
            print(f"[ERR] page={page} err={err}")
            fail += 1
            page += 1
            continue

        n = len(projects)
        print(f"[PAGE] page={page} items={n}")
        if n == 0:
            empty_streak += 1
            if empty_streak >= max(args.stop_empty_pages, 1):
                print(f"[STOP] empty pages reached: {empty_streak}")
                break
            page += 1
            continue

        empty_streak = 0
        for idx, it in enumerate(projects, start=1):
            seen += 1
            ok, msg = upsert_project(conn, it, args.dry_run)
            if ok:
                saved += 1
            else:
                fail += 1
            if seen % 500 == 0:
                print(f"  [PROGRESS] seen={seen} saved={saved} fail={fail}")
            if not args.dry_run and seen % 200 == 0:
                conn.commit()

        if not args.dry_run:
            conn.commit()
        page += 1
        if args.delay > 0:
            time.sleep(args.delay)

        if max_page_by_total and page > max_page_by_total:
            print(f"[STOP] reached max_page by Total: {max_page_by_total}")
            break

    if not args.dry_run:
        conn.commit()
    print(
        f"[DONE] seen={seen} saved={saved} fail={fail} "
        f"last_page={page} total={discovered_total} at={utc_now()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
