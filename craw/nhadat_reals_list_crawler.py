import argparse
import json
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pymysql
import requests


API_URL = "https://api.cafeland.vn/api/website-service/api/app-sync/reals/list/"
DEFAULT_NHDAT_TOKEN = "$2y$10$0f/Frpwde3r0.th2lxB3Nuq7dGgZUhPMe4aoAC9Toz0how..g1rJ6"
DEFAULT_NHDAT_SECRET = "8aHAzSUUJw"

PRICE_M2_MIN_VND = 500_000
PRICE_TOTAL_MIN_VND_FOR_M2_ADJUST = 500_000_000


def _parse_area(area: Any) -> Optional[float]:
    if area is None:
        return None
    s = str(area).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        v = float(s)
    except Exception:
        return None
    return v if v > 0 else None


def _compute_price_vnd(price: Any, area: Any, area_unit: Any) -> Optional[int]:
    """
    Compute normalized price_vnd for nhadat_data.

    Rule requested:
    - If area_unit == 'm2' AND price >= 500,000,000:
        computed = price / area
        if computed >= 500,000 then use computed else keep original price
    - Else: keep original price
    """
    if price is None:
        return None
    try:
        p = int(price)
    except Exception:
        return None
    if p < 0:
        return None

    unit = (str(area_unit).strip().lower() if area_unit is not None else "")
    if unit == "m2" and p >= PRICE_TOTAL_MIN_VND_FOR_M2_ADJUST:
        a = _parse_area(area)
        if a:
            computed = int(p / a)
            if computed >= PRICE_M2_MIN_VND:
                return computed
    return p


def get_connection():
    # Keep in sync with other scripts in this repo.
    return pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="",
        database="craw_db",
        port=3306,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def ensure_table(conn, rebuild: bool = False, truncate: bool = False) -> None:
    with conn.cursor() as cur:
        if rebuild:
            cur.execute("DROP TABLE IF EXISTS nhadat_data")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nhadat_data (
                realestate_id BIGINT NOT NULL PRIMARY KEY,
                trade_type INT NULL,
                category_id INT NULL,
                category_name VARCHAR(255) NULL,
                price BIGINT NULL,
                price_vnd BIGINT NULL,
                project_id BIGINT NULL,
                orig_list_time VARCHAR(32) NULL,
                city_id INT NULL,
                ward_id INT NULL,
                street_id BIGINT NULL,
                area VARCHAR(64) NULL,
                area_unit VARCHAR(32) NULL,
                page INT NULL,
                fetched_at DATETIME NOT NULL,
                converted TINYINT(1) NOT NULL DEFAULT 0,

                INDEX idx_nhadat_data_city (city_id),
                INDEX idx_nhadat_data_ward (ward_id),
                INDEX idx_nhadat_data_street (street_id),
                INDEX idx_nhadat_data_converted (converted),
                INDEX idx_nhadat_data_trade (trade_type),
                INDEX idx_nhadat_data_category (category_id),
                INDEX idx_nhadat_data_project (project_id),
                INDEX idx_nhadat_data_price_vnd (price_vnd),
                INDEX idx_nhadat_data_orig_list_time (orig_list_time),
                INDEX idx_nhadat_data_fetched_at (fetched_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )

        # Backward-compatible migration for existing deployments.
        try:
            cur.execute("ALTER TABLE nhadat_data ADD COLUMN street_id BIGINT NULL")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX idx_nhadat_data_street ON nhadat_data (street_id)")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE nhadat_data ADD COLUMN converted TINYINT(1) NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX idx_nhadat_data_converted ON nhadat_data (converted)")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE nhadat_data ADD COLUMN price_vnd BIGINT NULL")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX idx_nhadat_data_price_vnd ON nhadat_data (price_vnd)")
        except Exception:
            pass
        if truncate:
            cur.execute("TRUNCATE TABLE nhadat_data")


def _headers(token: str, secret: str) -> Dict[str, str]:
    # Minimal headers known to work with this API.
    return {
        "token": token,
        "secret": secret,
        "accept": "application/json",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64)",
    }


def fetch_page(
    session: requests.Session,
    token: str,
    secret: str,
    page: int,
    timeout: int = 30,
    retry: int = 3,
) -> Tuple[int, Optional[int], List[Dict[str, Any]], Optional[str]]:
    """
    Returns: (page, data_total, data_list, error)
    """
    params = {"page": page}
    last_err: Optional[str] = None

    for attempt in range(1, retry + 1):
        try:
            r = session.get(
                API_URL,
                headers=_headers(token, secret),
                params=params,
                timeout=timeout,
            )
            if r.status_code != 200:
                last_err = f"http {r.status_code}: {r.text[:200]}"
                time.sleep(0.5 * attempt)
                continue

            j = r.json()
            if j.get("success") is True:
                data_total = j.get("data_total")
                data = j.get("data") or []
                if not isinstance(data, list):
                    data = []
                return page, int(data_total) if data_total is not None else None, data, None

            # Known failure message: "Vui lòng nâng cấp phiên bản"
            last_err = f"api: {j.get('message')} (code={j.get('code')})"
            time.sleep(0.7 * attempt)
        except Exception as e:
            last_err = str(e)
            time.sleep(0.7 * attempt)

    return page, None, [], last_err


def insert_rows(conn, page: int, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    values = []
    for r in rows:
        price = r.get("price")
        price_vnd = _compute_price_vnd(price, r.get("area"), r.get("area_unit"))
        values.append(
            (
                r.get("realestate_id"),
                r.get("trade_type"),
                r.get("category_id"),
                r.get("category_name"),
                price,
                price_vnd,
                r.get("project_id"),
                r.get("orig_list_time"),
                r.get("city_id"),
                r.get("ward_id"),
                r.get("street_id"),
                r.get("area"),
                r.get("area_unit"),
                page,
                now,
            )
        )

    sql = """
        INSERT IGNORE INTO nhadat_data (
            realestate_id, trade_type, category_id, category_name, price, price_vnd, project_id,
            orig_list_time, city_id, ward_id, street_id, area, area_unit,
            page, fetched_at
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s
        )
    """

    inserted = 0
    with conn.cursor() as cur:
        cur.executemany(sql, values)
        inserted = cur.rowcount
    return inserted


def upsert_rows(conn, page: int, rows: List[Dict[str, Any]]) -> int:
    """
    Upsert for schema evolution (e.g., new street_id). Returns affected rowcount from executemany.
    """
    if not rows:
        return 0

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    values = []
    for r in rows:
        price = r.get("price")
        price_vnd = _compute_price_vnd(price, r.get("area"), r.get("area_unit"))
        values.append(
            (
                r.get("realestate_id"),
                r.get("trade_type"),
                r.get("category_id"),
                r.get("category_name"),
                price,
                price_vnd,
                r.get("project_id"),
                r.get("orig_list_time"),
                r.get("city_id"),
                r.get("ward_id"),
                r.get("street_id"),
                r.get("area"),
                r.get("area_unit"),
                page,
                now,
            )
        )

    sql = """
        INSERT INTO nhadat_data (
            realestate_id, trade_type, category_id, category_name, price, price_vnd, project_id,
            orig_list_time, city_id, ward_id, street_id, area, area_unit,
            page, fetched_at
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s
        )
        ON DUPLICATE KEY UPDATE
            trade_type=VALUES(trade_type),
            category_id=VALUES(category_id),
            category_name=VALUES(category_name),
            price=VALUES(price),
            price_vnd=VALUES(price_vnd),
            project_id=VALUES(project_id),
            orig_list_time=VALUES(orig_list_time),
            city_id=VALUES(city_id),
            ward_id=VALUES(ward_id),
            street_id=VALUES(street_id),
            area=VALUES(area),
            area_unit=VALUES(area_unit),
            page=VALUES(page),
            fetched_at=VALUES(fetched_at)
    """

    with conn.cursor() as cur:
        cur.executemany(sql, values)
        return cur.rowcount


def count_missing_ids(conn, realestate_ids: List[Any]) -> int:
    """
    Count how many IDs in this batch do not exist in nhadat_data yet.
    Used by incremental mode to stop early when no new data appears.
    """
    ids: List[int] = []
    for rid in realestate_ids:
        try:
            if rid is not None:
                ids.append(int(rid))
        except Exception:
            continue
    if not ids:
        return 0

    placeholders = ",".join(["%s"] * len(ids))
    sql = f"SELECT COUNT(*) AS c FROM nhadat_data WHERE realestate_id IN ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, ids)
        row = cur.fetchone() or {"c": 0}
        exists_count = int(row.get("c") or 0)
    return max(0, len(ids) - exists_count)


def run_once(args) -> int:
    """
    Execute one crawl cycle.
    Returns process exit code (0=ok, non-zero=failed).
    """
    parser = argparse.ArgumentParser(description="Nhadat Cafeland Reals List Crawler")
    if not args.token or not args.secret:
        print("Missing token/secret. Provide --token/--secret or set env NHDAT_TOKEN/NHDAT_SECRET.")
        return 2

    conn = get_connection()
    ensure_table(conn, rebuild=args.rebuild_table, truncate=args.truncate)

    # Probe to find total pages if not set.
    session = requests.Session()
    probe_page = max(args.start_page, 1)
    print(f"Probing page={probe_page} ...")
    p, data_total, data, err = fetch_page(
        session=session,
        token=args.token,
        secret=args.secret,
        page=probe_page,
        timeout=args.timeout,
        retry=args.retries,
    )
    if err:
        print(f"Probe failed at page {probe_page}: {err}")
        # Try the next page as fallback (observed page=1 sometimes returns 'upgrade version').
        fallback = probe_page + 1
        print(f"Probing fallback page={fallback} ...")
        p, data_total, data, err = fetch_page(
            session=session,
            token=args.token,
            secret=args.secret,
            page=fallback,
            timeout=args.timeout,
            retry=args.retries,
        )
        if err:
            print(f"Probe fallback failed at page {fallback}: {err}")
            return 1

    per_page = max(len(data), 1)
    if args.max_page and args.max_page > 0:
        max_page = args.max_page
    else:
        if data_total is None:
            print("Probe returned no data_total; set --max-page manually.")
            return 1
        max_page = int(math.ceil(int(data_total) / float(per_page)))

    if args.max_pages_per_run and args.max_pages_per_run > 0:
        max_page = min(max_page, args.start_page + args.max_pages_per_run - 1)

    print(f"data_total={data_total} per_page~={per_page} => max_page={max_page}")
    print(
        f"workers={args.workers} start_page={args.start_page} dry_run={args.dry_run} "
        f"incremental={args.incremental} upsert={args.upsert} delay={args.delay_seconds}s"
    )

    inserted_total = 0
    ok_pages = 0
    failed_pages = 0
    started_at = time.time()

    # Fetch pages concurrently; each worker uses its own session + DB connection for inserts.
    def _task(page_num: int) -> Tuple[int, int, Optional[str], int, int]:
        if args.delay_seconds > 0:
            time.sleep(float(args.delay_seconds))
        s = requests.Session()
        page, _total, rows, e = fetch_page(
            session=s,
            token=args.token,
            secret=args.secret,
            page=page_num,
            timeout=args.timeout,
            retry=args.retries,
        )
        if e:
            return page, 0, e, 0, 0
        if args.dry_run:
            return page, len(rows), None, 0, len(rows)
        c = get_connection()
        try:
            ids = [r.get("realestate_id") for r in rows]
            # True new rows must be counted before INSERT/UPSERT.
            new_count = count_missing_ids(c, ids)
            if args.upsert:
                ins = upsert_rows(c, page, rows)
            else:
                ins = insert_rows(c, page, rows)
            return page, len(rows), None, ins, new_count
        finally:
            try:
                c.close()
            except Exception:
                pass

    pages = list(range(args.start_page, max_page + 1))

    if args.incremental:
        empty_streak = 0
        processed_pages = 0
        stop_after = max(1, int(args.stop_after_empty_pages))
        print(f"incremental_stop_after_empty_pages={stop_after}")

        # Process pages in ordered chunks so we can keep "consecutive no-new pages" logic deterministic.
        chunk_size = max(1, int(args.workers))
        for i in range(0, len(pages), chunk_size):
            chunk = pages[i:i + chunk_size]
            processed_pages += len(chunk)

            if chunk_size == 1:
                results = []
                for pg in chunk:
                    results.append(_task(pg))
            else:
                results_by_page = {}
                with ThreadPoolExecutor(max_workers=chunk_size) as ex:
                    futs = {ex.submit(_task, pg): pg for pg in chunk}
                    for fut in as_completed(futs):
                        pg = futs[fut]
                        try:
                            results_by_page[pg] = fut.result()
                        except Exception as e:
                            results_by_page[pg] = (pg, 0, str(e), 0, 0)
                results = [results_by_page[pg] for pg in chunk]

            for page, nrows, e, ins, new_count in results:
                if e:
                    failed_pages += 1
                    print(f"[page {page}] FAIL: {e}")
                    continue

                ok_pages += 1
                inserted_total += ins

                if args.dry_run:
                    print(f"[page {page}] OK rows={nrows} new_guess={new_count}")
                else:
                    print(f"[page {page}] OK rows={nrows} inserted={ins} new={new_count}")

                if new_count <= 0:
                    empty_streak += 1
                else:
                    # Reset streak immediately when any page has new rows.
                    empty_streak = 0

                if empty_streak >= stop_after:
                    print(
                        f"Stopping incremental crawl at page={page}: "
                        f"{empty_streak} consecutive pages without new rows."
                    )
                    break
            if empty_streak >= stop_after:
                break
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(_task, pg): pg for pg in pages}
            for fut in as_completed(futs):
                pg = futs[fut]
                try:
                    page, nrows, e, ins, _new_count = fut.result()
                except Exception as e:
                    failed_pages += 1
                    print(f"[page {pg}] FAIL: {e}")
                    continue

                if e:
                    failed_pages += 1
                    print(f"[page {page}] FAIL: {e}")
                    continue

                ok_pages += 1
                inserted_total += ins
                if args.dry_run:
                    print(f"[page {page}] OK rows={nrows}")
                else:
                    print(f"[page {page}] OK rows={nrows} inserted={ins}")

    dur = time.time() - started_at
    print("=== DONE ===")
    print(f"ok_pages={ok_pages} failed_pages={failed_pages}")
    print(f"inserted_total={inserted_total} dry_run={args.dry_run}")
    print(f"duration={dur:.2f}s")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Nhadat Cafeland Reals List Crawler")
    parser.add_argument(
        "--token",
        default=os.environ.get("NHDAT_TOKEN", DEFAULT_NHDAT_TOKEN),
        help="API token (or env NHDAT_TOKEN)",
    )
    parser.add_argument(
        "--secret",
        default=os.environ.get("NHDAT_SECRET", DEFAULT_NHDAT_SECRET),
        help="API secret (or env NHDAT_SECRET)",
    )
    parser.add_argument("--workers", type=int, default=2, help="Number of concurrent workers (pages in flight)")
    parser.add_argument("--start-page", type=int, default=1, help="Start page")
    parser.add_argument("--max-page", type=int, default=0, help="Max page (0=auto by data_total)")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retries per page")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only, do not insert DB")
    parser.add_argument("--truncate", action="store_true", help="Delete all old rows before crawling")
    parser.add_argument("--rebuild-table", action="store_true", help="Drop and recreate nhadat_data table before crawling")
    parser.add_argument("--upsert", action="store_true", help="Update existing rows (ON DUPLICATE KEY UPDATE)")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Crawl newest pages only and stop early when consecutive pages have no new rows.",
    )
    parser.add_argument(
        "--stop-after-empty-pages",
        type=int,
        default=10,
        help="In incremental mode, stop after this many consecutive pages with no new rows.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.5,
        help="Delay before each page request (seconds).",
    )
    parser.add_argument(
        "--max-pages-per-run",
        type=int,
        default=100,
        help="Safety cap pages per run (0=unlimited).",
    )
    parser.add_argument("--daily-loop", action="store_true", help="Run forever and execute one incremental crawl each day.")
    parser.add_argument("--interval-seconds", type=int, default=86400, help="Sleep interval in daily loop mode.")
    args = parser.parse_args()

    if args.daily_loop:
        # Daily mode should be incremental by default.
        args.incremental = True
        cycle = 0
        print(f"Daily loop mode enabled. interval_seconds={args.interval_seconds}")
        while True:
            cycle += 1
            started = datetime.now()
            print(f"\n===== DAILY CYCLE {cycle} START {started.strftime('%Y-%m-%d %H:%M:%S')} =====")
            try:
                code = run_once(args)
                if code != 0:
                    print(f"[WARN] cycle={cycle} finished with code={code}")
            except Exception as e:
                print(f"[ERROR] cycle={cycle} crashed: {e}")
            ended = datetime.now()
            print(f"===== DAILY CYCLE {cycle} END {ended.strftime('%Y-%m-%d %H:%M:%S')} =====")
            time.sleep(max(1, int(args.interval_seconds)))
    else:
        sys.exit(run_once(args))


if __name__ == "__main__":
    main()
