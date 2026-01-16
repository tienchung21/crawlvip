import argparse
import asyncio
import json
import random
import time
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple

import aiohttp

from craw.database import Database


BASE = "https://cenhomes.vn/api/manage/get-source-buy-rent"


def _type_source(trade_type: str) -> str:
    return "buy" if trade_type == "mua" else "rent"


def _build_url(slug: str, trade_type: str) -> str:
    return f"{BASE}/{slug}?typeSource={_type_source(trade_type)}"


def _referer_path(trade_type: str) -> str:
    return "mua-nha" if trade_type == "mua" else "thue-nha"


def _headers(slug: str, trade_type: str) -> Dict[str, str]:
    return {
        "accept": "application/json",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
        "referer": f"https://cenhomes.vn/{_referer_path(trade_type)}/{slug}",
    }


class RateLimiter:
    def __init__(self, rps: float):
        self.rps = rps
        self._lock = asyncio.Lock()
        self._last = 0.0
        self._min_rps = 0.5

    async def wait(self):
        async with self._lock:
            interval = 1.0 / max(self.rps, 0.1)
            now = time.monotonic()
            wait_s = max(0.0, interval - (now - self._last))
            self._last = now + wait_s
        jitter = random.randint(30, 200) / 1000.0
        await asyncio.sleep(wait_s + jitter)

    async def slow_down(self, factor: float = 0.7, cooldown_s: float = 2.0):
        async with self._lock:
            self.rps = max(self._min_rps, self.rps * factor)
        await asyncio.sleep(cooldown_s + random.uniform(0.2, 1.0))


def ensure_crawl_columns(db: Database):
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        columns = {
            "crawl": "TINYINT(1) NULL",
            "crawl_status": "TINYINT(1) NULL",
            "crawl_http": "INT NULL",
            "crawl_time": "DATETIME NULL",
            "crawl_error": "TEXT NULL",
        }
        for col, col_type in columns.items():
            cur.execute("SHOW COLUMNS FROM cenhomes_ads LIKE %s", (col,))
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE cenhomes_ads ADD COLUMN {col} {col_type}")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def fetch_table_columns(db: Database, table: str) -> Tuple[List[str], Dict[str, str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW COLUMNS FROM {table}")
        rows = cur.fetchall()
        columns = []
        col_types = {}
        for row in rows:
            if isinstance(row, tuple):
                col_name = row[0]
                col_type = row[1]
            else:
                col_name = row.get("Field")
                col_type = row.get("Type")
            columns.append(col_name)
            col_types[col_name] = col_type or ""
        return columns, col_types
    finally:
        cur.close()
        conn.close()


def to_snake(value: str) -> str:
    out = []
    for ch in value:
        if ch.isupper():
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out)


def _parse_datetime(value: str) -> Optional[str]:
    try:
        dt = parsedate_to_datetime(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def coerce_value(col: str, val, col_type: str):
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, str):
        if "datetime" in col_type.lower() or "timestamp" in col_type.lower():
            parsed = _parse_datetime(val)
            return parsed if parsed is not None else None
    return val


def save_detail(db: Database, table: str, data: Dict, columns: List[str], col_types: Dict[str, str]):
    data_snake = {to_snake(k): v for k, v in data.items()}
    row = {}
    for col in columns:
        if col in data:
            row[col] = coerce_value(col, data.get(col), col_types.get(col, ""))
        elif col in data_snake:
            row[col] = coerce_value(col, data_snake.get(col), col_types.get(col, ""))
        else:
            row[col] = None

    if "id" in columns and row.get("id") is None:
        raise ValueError("Missing id in response data.")

    cols_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_cols = [c for c in columns if c != "id"]
    update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols])

    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_sql}",
            [row[c] for c in columns],
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def load_slugs(db: Database, only_new: bool) -> List[Tuple[int, str, str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if only_new:
            cur.execute(
                """
                SELECT id, slug, trade_type
                FROM cenhomes_ads
                WHERE slug IS NOT NULL AND slug <> ''
                  AND (crawl IS NULL OR crawl = 0)
                """
            )
        else:
            cur.execute(
                """
                SELECT id, slug, trade_type
                FROM cenhomes_ads
                WHERE slug IS NOT NULL AND slug <> ''
                """
            )
        rows = cur.fetchall()
        result = []
        for row in rows:
            if isinstance(row, tuple):
                result.append((row[0], row[1], row[2]))
            else:
                result.append((row.get("id"), row.get("slug"), row.get("trade_type")))
        return result
    finally:
        cur.close()
        conn.close()


def update_status(
    db: Database,
    row_id: int,
    status: int,
    http_status: Optional[int],
    error: Optional[str],
):
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE cenhomes_ads
            SET crawl=%s,
                crawl_status=%s,
                crawl_http=%s,
                crawl_time=NOW(),
                crawl_error=%s
            WHERE id=%s
            """,
            (status, status, http_status, error, row_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


async def fetch_one(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    row_id: int,
    slug: str,
    trade_type: str,
    db: Database,
    max_retry: int,
    detail_table: str,
    detail_columns: List[str],
    detail_col_types: Dict[str, str],
):
    url = _build_url(slug, trade_type)
    headers = _headers(slug, trade_type)
    attempt = 0
    while attempt <= max_retry:
        attempt += 1
        await limiter.wait()
        try:
            async with session.get(url, headers=headers) as resp:
                status = resp.status
                ct = (resp.headers.get("content-type") or "").lower()
                if status == 200 and "application/json" in ct:
                    payload = await resp.json()
                    data = payload.get("data") if isinstance(payload, dict) else None
                    if not isinstance(data, dict):
                        update_status(db, row_id, 0, status, "missing_data")
                        return "missing_data", status
                    save_detail(db, detail_table, data, detail_columns, detail_col_types)
                    update_status(db, row_id, 1, status, None)
                    return "ok", status
                if "application/json" not in ct:
                    text = await resp.text()
                    await limiter.slow_down()
                    if attempt > max_retry:
                        update_status(db, row_id, 0, status, "non_json")
                        return "non_json", status
                    await asyncio.sleep((0.5 * (2 ** attempt)) + random.random())
                    continue
                if status == 404:
                    update_status(db, row_id, 0, status, "not_found")
                    return "not_found", status
                if status in (403, 429, 500, 502, 503, 504):
                    if status in (403, 429):
                        await limiter.slow_down()
                    if attempt > max_retry:
                        update_status(db, row_id, 0, status, f"retry_exhausted_{status}")
                        return "retry_exhausted", status
                    await asyncio.sleep((0.5 * (2 ** attempt)) + random.random())
                    continue
                # Other status
                text = await resp.text()
                update_status(db, row_id, 0, status, text[:500])
                return "error", status
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt > max_retry:
                update_status(db, row_id, 0, None, str(exc))
                return "fetch_error", None
            await asyncio.sleep((0.5 * (2 ** attempt)) + random.random())


async def run(slugs: List[Tuple[int, str, str]], rps: float, concurrency: int, max_retry: int):
    limiter = RateLimiter(rps)
    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=concurrency)
    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    ensure_crawl_columns(db)
    detail_table = "cenhomedetail"
    detail_columns, detail_col_types = fetch_table_columns(db, detail_table)

    total = len(slugs)
    done = 0
    q: asyncio.Queue[Optional[Tuple[int, str, str]]] = asyncio.Queue(maxsize=concurrency * 4)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def _worker(worker_id: int):
            nonlocal done
            while True:
                item = await q.get()
                if item is None:
                    q.task_done()
                    return
                row_id, slug, trade_type = item
                status, http = await fetch_one(
                    session,
                    limiter,
                    row_id,
                    slug,
                    trade_type,
                    db,
                    max_retry,
                    detail_table,
                    detail_columns,
                    detail_col_types,
                )
                done += 1
                print(
                    f"[{done}/{total}] {trade_type} {slug} -> {status} "
                    f"http={http} rps={limiter.rps:.2f}"
                )
                if done % 50 == 0 or done == total:
                    print(f"Progress: {done}/{total}")
                q.task_done()

        workers = [asyncio.create_task(_worker(i)) for i in range(concurrency)]
        for item in slugs:
            await q.put(item)
        for _ in workers:
            await q.put(None)
        await q.join()
        await asyncio.gather(*workers)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rps", type=float, default=6.0)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--max-retry", type=int, default=6)
    parser.add_argument("--only-new", action="store_true")
    args = parser.parse_args()

    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    slugs = load_slugs(db, args.only_new)
    if not slugs:
        print("No slugs to process.")
        return

    asyncio.run(run(slugs, args.rps, args.concurrency, args.max_retry))


if __name__ == "__main__":
    main()
