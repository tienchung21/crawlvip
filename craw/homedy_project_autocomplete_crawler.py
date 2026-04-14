#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from collections import deque
from datetime import UTC, datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pymysql
from curl_cffi import requests

API_URL = "https://homedy.com/Search/SearchAutoCompleteProject"
PARAM_CANDIDATES = ["Keyword", "keyword", "q", "query", "term", "search", "text", "k"]

# autocomplete tra ve toi da ~10 item/keyword
API_HARD_LIMIT = 10

# quet prefix de vet can
DEFAULT_SEEDS = list("abcdefghijklmnopqrstuvwxyz0123456789")
DEFAULT_EXPAND_CHARS = list("abcdefghijklmnopqrstuvwxyz0123456789 ")


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Crawl du an Homedy -> duan_homedy")
    p.add_argument("--db-host", default="localhost")
    p.add_argument("--db-port", type=int, default=3306)
    p.add_argument("--db-user", default="root")
    p.add_argument("--db-pass", default="")
    p.add_argument("--db-name", default="craw_db")

    p.add_argument("--keyword", action="append", help="Mode keyword thu cong (co the repeat)")
    p.add_argument("--full-scan", action="store_true", help="Mode quet vet can theo prefix")
    p.add_argument("--force-key", default="", help="Force key request (Keyword/keyword/q...)")

    p.add_argument("--max-depth", type=int, default=5, help="Do sau toi da khi split prefix")
    p.add_argument("--delay", type=float, default=0.15, help="Delay giua request")
    p.add_argument("--max-requests", type=int, default=0, help="Chan tren so request (0=khong gioi han)")
    p.add_argument("--max-items", type=int, default=0, help="Chan tren so row save/update (0=khong gioi han)")
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--recreate-table", action="store_true", help="Drop+create lai duan_homedy")
    return p.parse_args()


def connect_db(args: argparse.Namespace):
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


def ensure_table(conn, recreate: bool = False) -> None:
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
        if recreate:
            cur.execute("DROP TABLE IF EXISTS duan_homedy")
        cur.execute(ddl)
    conn.commit()


def fetch_data(session: requests.Session, key_name: str, keyword: str, retries: int = 3) -> Tuple[Optional[List[Dict]], Optional[str]]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                API_URL,
                params={key_name: keyword},
                timeout=30,
                impersonate="chrome124",
            )
            if resp.status_code != 200:
                last_err = f"http_{resp.status_code}"
            else:
                obj = resp.json()
                data = obj.get("Data")
                if isinstance(data, list):
                    return data, None
                last_err = "invalid_data"
        except Exception as ex:
            last_err = f"error:{type(ex).__name__}"
        if attempt < retries:
            time.sleep(min(attempt, 3))
    return None, last_err


def choose_best_key(session: requests.Session, probe_keyword: str, retries: int) -> str:
    best_key = "Keyword"
    best_cnt = -1
    print(f"[KEY_TEST] keyword='{probe_keyword}'")
    for key_name in PARAM_CANDIDATES:
        data, err = fetch_data(session, key_name, probe_keyword, retries=retries)
        cnt = len(data) if data is not None else -1
        print(f"  key={key_name:<8} count={cnt} err={err}")
        if cnt > best_cnt:
            best_cnt = cnt
            best_key = key_name
    print(f"[KEY_PICK] {best_key} (count={best_cnt})")
    return best_key


def normalize_project_url(url: Optional[str]) -> Optional[str]:
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


def extract_project_id(item: Dict) -> Optional[int]:
    # yeu cau: lay id cuoi moi item (trong URL ...-pjxxxxx)
    url = item.get("Url") or ""
    m = re.search(r"-pj(\d+)$", str(url))
    if m:
        return int(m.group(1))
    # fallback
    pid = item.get("Id")
    try:
        if pid not in (None, "", 0, "0"):
            return int(pid)
    except Exception:
        pass
    return None


def upsert_item(conn, item: Dict, dry_run: bool) -> Tuple[bool, str]:
    project_id = extract_project_id(item)
    if not project_id:
        return False, "missing_project_id"

    homedy_id = item.get("Id")
    try:
        homedy_id = int(homedy_id) if homedy_id not in (None, "", 0, "0") else None
    except Exception:
        homedy_id = None

    project_name = (item.get("Name") or "").strip() or None
    project_url = normalize_project_url(item.get("Url"))
    city_id = item.get("CityId")
    district_id = item.get("DistrictId")
    ward_id = item.get("WardId")
    street_id = item.get("StreetId")

    if dry_run:
        return True, f"dry_run pid={project_id} name={project_name}"

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
        cur.execute(
            sql,
            (project_id, homedy_id, project_name, project_url, city_id, district_id, ward_id, street_id),
        )
    return True, f"saved pid={project_id} name={project_name}"


def run_keyword_mode(
    conn,
    session: requests.Session,
    req_key: str,
    keywords: Iterable[str],
    args: argparse.Namespace,
) -> Tuple[int, int, int, int]:
    total_requests = 0
    total_seen = 0
    total_saved = 0
    total_fail = 0

    for kw in keywords:
        kw = (kw or "").strip()
        if not kw:
            continue
        data, err = fetch_data(session, req_key, kw, retries=args.retries)
        total_requests += 1
        if data is None:
            total_fail += 1
            print(f"[ERR] kw='{kw}' err={err}")
            continue
        print(f"[KW] '{kw}' items={len(data)}")
        for i, item in enumerate(data, 1):
            total_seen += 1
            ok, msg = upsert_item(conn, item, args.dry_run)
            if ok:
                total_saved += 1
                print(f"  [OK] {i}/{len(data)} {msg}")
            else:
                total_fail += 1
                print(f"  [SKIP] {i}/{len(data)} {msg}")
            if not args.dry_run and total_seen % 200 == 0:
                conn.commit()
            if args.max_items > 0 and total_saved >= args.max_items:
                if not args.dry_run:
                    conn.commit()
                return total_requests, total_seen, total_saved, total_fail
        if args.delay > 0:
            time.sleep(args.delay)
    if not args.dry_run:
        conn.commit()
    return total_requests, total_seen, total_saved, total_fail


def run_full_scan(
    conn,
    session: requests.Session,
    req_key: str,
    args: argparse.Namespace,
) -> Tuple[int, int, int, int]:
    total_requests = 0
    total_seen = 0
    total_saved = 0
    total_fail = 0

    q: deque[Tuple[str, int]] = deque((s, 1) for s in DEFAULT_SEEDS)
    scanned: Set[str] = set()

    while q:
        prefix, depth = q.popleft()
        if prefix in scanned:
            continue
        scanned.add(prefix)

        data, err = fetch_data(session, req_key, prefix, retries=args.retries)
        total_requests += 1
        if data is None:
            total_fail += 1
            print(f"[ERR] prefix='{prefix}' depth={depth} err={err}")
            continue

        print(f"[SCAN] prefix='{prefix}' depth={depth} items={len(data)} req={total_requests}")
        for item in data:
            total_seen += 1
            ok, msg = upsert_item(conn, item, args.dry_run)
            if ok:
                total_saved += 1
            else:
                total_fail += 1

            if total_seen % 100 == 0:
                print(f"  [PROGRESS] seen={total_seen} saved={total_saved} fail={total_fail}")
            if not args.dry_run and total_seen % 100 == 0:
                conn.commit()
            if args.max_items > 0 and total_saved >= args.max_items:
                if not args.dry_run:
                    conn.commit()
                return total_requests, total_seen, total_saved, total_fail

        # neu hit tran 10 => co kha nang con du lieu bi cat, can chia nho tiep
        if len(data) >= API_HARD_LIMIT and depth < args.max_depth:
            for ch in DEFAULT_EXPAND_CHARS:
                child = f"{prefix}{ch}"
                if child not in scanned:
                    q.append((child, depth + 1))

        if args.max_requests > 0 and total_requests >= args.max_requests:
            if not args.dry_run:
                conn.commit()
            return total_requests, total_seen, total_saved, total_fail

        if args.delay > 0:
            time.sleep(args.delay)

    if not args.dry_run:
        conn.commit()
    return total_requests, total_seen, total_saved, total_fail


def main() -> int:
    args = parse_args()
    conn = connect_db(args)
    ensure_table(conn, recreate=args.recreate_table)

    session = requests.Session()
    req_key = args.force_key.strip() if args.force_key.strip() else choose_best_key(session, "chung", retries=args.retries)

    use_full_scan = args.full_scan or not args.keyword
    if use_full_scan:
        req, seen, saved, fail = run_full_scan(conn, session, req_key, args)
    else:
        req, seen, saved, fail = run_keyword_mode(conn, session, req_key, args.keyword or [], args)

    print(f"[DONE] requests={req} seen={seen} saved={saved} fail={fail} at={utc_now()} key={req_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
