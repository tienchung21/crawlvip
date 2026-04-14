#!/usr/bin/env python3
"""
Crawl danh sach du an tu Meeyland project API vao bang duan_meeyland.

API:
  https://api.meeyproject.com/v1/projects?sortBy=createdAt:desc&page=1&limit=30

Luu:
  - results[*]._id   -> duan_meeyland.project_id
  - results[*].name  -> duan_meeyland.name
"""

from __future__ import annotations

import argparse
import os
import sys
import time

try:
    from curl_cffi import requests
except Exception as exc:
    raise RuntimeError("curl_cffi is required for meeyland_project_crawler.py") from exc

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402


API_URL = "https://api.meeyproject.com/v1/projects"
DEFAULT_IMPERSONATE = "chrome124"


def build_url(page: int, limit: int) -> str:
    return f"{API_URL}?sortBy=createdAt:desc&page={page}&limit={limit}"


def fetch_page(
    session: requests.Session,
    page: int,
    limit: int,
    impersonate: str,
    timeout: int,
    retries: int,
):
    url = build_url(page, limit)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, impersonate=impersonate, timeout=timeout)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            obj = resp.json()
            if not isinstance(obj, dict):
                raise RuntimeError("Response is not JSON object")
            return obj
        except Exception as exc:
            last_err = exc
            sleep_s = min(10.0, 0.8 * attempt)
            print(f"[RETRY] page={page} attempt={attempt}/{retries} sleep={sleep_s:.1f}s error={exc}")
            time.sleep(sleep_s)
    raise RuntimeError(f"Fetch failed page={page}: {last_err}")


def save_projects(db: Database, rows: list[tuple[str, str]]) -> int:
    if not rows:
        return 0

    conn = db.get_connection()
    cur = conn.cursor()
    inserted = 0
    try:
        for project_id, name in rows:
            cur.execute(
                """
                INSERT INTO duan_meeyland (project_id, name)
                SELECT %s, %s
                FROM DUAL
                WHERE NOT EXISTS (
                    SELECT 1 FROM duan_meeyland WHERE project_id = %s
                )
                """,
                (project_id, name, project_id),
            )
            inserted += int(cur.rowcount or 0)
        conn.commit()
        return inserted
    finally:
        cur.close()
        conn.close()


def run(start_page: int, max_page: int | None, limit: int, delay_s: float, timeout: int, retries: int, impersonate: str):
    db = Database()
    session = requests.Session()

    page = max(1, int(start_page))
    total_seen = 0
    total_inserted = 0
    discovered_total_pages = None

    while True:
        if max_page is not None and page > max_page:
            break
        if discovered_total_pages is not None and page > discovered_total_pages:
            break

        obj = fetch_page(
            session=session,
            page=page,
            limit=limit,
            impersonate=impersonate,
            timeout=timeout,
            retries=retries,
        )

        results = obj.get("results") or []
        total_pages = obj.get("totalPages")
        total_results = obj.get("totalResults")
        discovered_total_pages = int(total_pages or 0) or discovered_total_pages

        payload = []
        for item in results:
            if not isinstance(item, dict):
                continue
            project_id = str(item.get("_id") or "").strip()
            name = str(item.get("name") or "").strip()
            if not project_id or not name:
                continue
            payload.append((project_id, name))

        seen = len(payload)
        inserted = save_projects(db, payload)
        total_seen += seen
        total_inserted += inserted

        print(
            f"[PAGE] page={page}/{discovered_total_pages or '?'} "
            f"items={seen} inserted={inserted} totalResults={total_results}"
        )

        if seen == 0:
            print("[STOP] Empty results")
            break

        page += 1
        if delay_s > 0:
            print(f"[SLEEP] {delay_s:.1f}s")
            time.sleep(delay_s)

    print(f"[DONE] seen={total_seen} inserted={total_inserted} last_page={page - 1}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Crawl Meeyland projects into duan_meeyland")
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--max-page", type=int, default=0, help="0 = crawl until API totalPages")
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--delay", type=float, default=0.2)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--impersonate", type=str, default=DEFAULT_IMPERSONATE)
    args = ap.parse_args()

    if args.start_page < 1:
        raise SystemExit("--start-page must be >= 1")
    if args.max_page < 0:
        raise SystemExit("--max-page must be >= 0")
    if args.limit < 1:
        raise SystemExit("--limit must be >= 1")
    if args.delay < 0:
        raise SystemExit("--delay must be >= 0")

    run(
        start_page=args.start_page,
        max_page=(args.max_page or None),
        limit=args.limit,
        delay_s=args.delay,
        timeout=args.timeout,
        retries=args.retries,
        impersonate=args.impersonate,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
