#!/usr/bin/env python3
"""
Update MeeyMap rows in scraped_details_flat using detail API.

Flow:
- select rows from scraped_details_flat where domain='meeymap.com' and status is null/empty
- fetch GET /api/article/detail/{matin}
- update selected detail fields
- save media URLs into scraped_detail_images
- set scraped_details_flat.status = 'done_detail'
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import threading
from queue import Empty, Queue
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from curl_cffi import requests as cffi_requests
except Exception:
    cffi_requests = None

from database import Database


DETAIL_URL_TMPL = "https://apiv3.meeymap.com/api/article/detail/{code}"
DOMAIN = "meeymap.com"
DEFAULT_DELAY = 1.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_STOP_ERROR_STREAK = 10
DEFAULT_WORKERS = 3
DEFAULT_CLAIM_BATCH = 100

AUTH_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://meeymap.com",
    "referer": "https://meeymap.com/",
    "authorization": (
        "Bearer "
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJ1c2VyIjp7ImlkIjoiNjlkMzg1MWI2ZTAyN2YxY2QyZTc5NjRhIiwiY2xpZW50X2lkIjoi"
        "bWVleW1hcHYzIn0sImtleSI6ImFjdC1iMTk1YTU5Yi03ZDg5LTQzYTAtYjM0Zi03MTA4ZGVh"
        "NjY0ODUiLCJpYXQiOjE3NzU1NTE1MDQsImV4cCI6MTc3NTgxMDcwNH0."
        "fpSWaARM6LinRIM4o4Ycd8e_TlQhJ0-TYxOseWRAsmc"
    ),
    "expiretoken": "1775810704",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
    ),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log(message: str) -> None:
    print(message, flush=True)


def ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def write_line(path: str, line: str) -> None:
    ensure_parent_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def log_both(args: argparse.Namespace, message: str) -> None:
    log(message)
    write_line(args.log_file, message)


def save_checkpoint(args: argparse.Namespace, payload: Dict[str, Any]) -> None:
    ensure_parent_dir(args.checkpoint_file)
    with open(args.checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_checkpoint(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update MeeyMap detail into scraped_details_flat")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Legacy fixed sleep seconds between links")
    parser.add_argument("--min-delay", type=float, default=0.5, help="Min sleep seconds between links per worker")
    parser.add_argument("--max-delay", type=float, default=0.9, help="Max sleep seconds between links per worker")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max retries per detail request")
    parser.add_argument(
        "--stop-error-streak",
        type=int,
        default=DEFAULT_STOP_ERROR_STREAK,
        help="Stop whole job after this many consecutive record errors",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_CLAIM_BATCH, help="Rows to claim per DB batch")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel worker threads")
    parser.add_argument("--limit-records", type=int, default=0, help="Optional cap on total rows to process")
    parser.add_argument("--trade-type", type=str, default="", choices=["", "s", "u"], help="Optional trade_type filter")
    parser.add_argument(
        "--only-missing-price",
        action="store_true",
        help="Only process rows where khoanggia is NULL/empty/0 (supports refresh of done_detail rows)",
    )
    parser.add_argument("--detail-url-template", type=str, default=DETAIL_URL_TMPL, help="Detail API template, use {code}")
    parser.add_argument("--domain", type=str, default=DOMAIN, help="Domain to read/write in scraped_details_flat")
    parser.add_argument("--origin", type=str, default="", help="Override Origin header")
    parser.add_argument("--referer", type=str, default="", help="Override Referer header")
    parser.add_argument("--x-tenant", type=str, default="", help="Optional x-tenant header")
    parser.add_argument("--auth-token", type=str, default="", help="Override authorization token (with or without 'Bearer ')")
    parser.add_argument("--expire-token", type=str, default="", help="Override expiretoken header")
    parser.add_argument("--id", type=int, default=0, help="Process exactly this scraped_details_flat.id")
    parser.add_argument("--matin", type=str, default="", help="Process exactly this matin")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument(
        "--log-file",
        default="/home/chungnt/crawlvip/craw/logs/meeymap_detail.log",
        help="Plain text log path",
    )
    parser.add_argument(
        "--checkpoint-file",
        default="/home/chungnt/crawlvip/craw/logs/meeymap_detail_checkpoint.json",
        help="Checkpoint path",
    )
    return parser.parse_args()


def make_session(args: argparse.Namespace):
    if cffi_requests is None:
        raise RuntimeError("curl_cffi is required for meeymap_detail_updater.py")
    headers = dict(AUTH_HEADERS)
    if args.origin:
        headers["origin"] = args.origin
    if args.referer:
        headers["referer"] = args.referer
    if args.x_tenant:
        headers["x-tenant"] = args.x_tenant
    session = cffi_requests.Session()
    session.headers.update(headers)
    return session


def apply_auth_overrides(args: argparse.Namespace) -> None:
    if args.auth_token:
        token = args.auth_token.strip()
        if token and not token.lower().startswith("bearer "):
            token = f"Bearer {token}"
        AUTH_HEADERS["authorization"] = token
    if args.expire_token:
        AUTH_HEADERS["expiretoken"] = args.expire_token.strip()


def fetch_detail(session: Any, matin: str, args: argparse.Namespace) -> Dict[str, Any]:
    url = args.detail_url_template.format(code=matin)
    last_error: Optional[Exception] = None
    for attempt in range(1, args.max_retries + 1):
        try:
            response = session.get(url, impersonate="chrome136", timeout=30)
            response.raise_for_status()
            obj = response.json()
            if not isinstance(obj, dict):
                raise RuntimeError("invalid non-object response")
            data = obj.get("data")
            if not isinstance(data, dict):
                raise RuntimeError("missing detail data")
            return data
        except Exception as exc:
            last_error = exc
            if attempt >= args.max_retries:
                break
            sleep_s = min(2 * attempt, 10)
            time.sleep(sleep_s)
    raise RuntimeError(f"fetch_detail_failed matin={matin}: {last_error}")


def first_label(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        return value.get("label")
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item.get("label"):
                return item.get("label")
    return None


def first_code_or_id(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        code = value.get("code")
        if code is not None:
            return str(code)
        obj_id = value.get("_id")
        if obj_id is not None:
            return str(obj_id)
    return None


def coords_to_lat_lng(value: Any) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(value, dict):
        return None, None
    coords = value.get("coordinates")
    if not isinstance(coords, list) or len(coords) < 2:
        return None, None
    lng = coords[0]
    lat = coords[1]
    return (str(lat) if lat is not None else None, str(lng) if lng is not None else None)


def media_urls(detail: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    seen = set()
    for item in detail.get("media") or []:
        if not isinstance(item, dict):
            continue
        if item.get("mediaType") not in (1, None):
            continue
        url = item.get("url")
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def map_detail(detail: Dict[str, Any]) -> Dict[str, Any]:
    project = detail.get("project") or {}
    total_rent = detail.get("totalRentalPrice")
    total_sale = detail.get("totalPrice")
    price_value = None
    if total_rent is not None:
        price_value = str(total_rent)
    elif total_sale is not None:
        price_value = str(total_sale)
    else:
        price_label = detail.get("priceLabel") or {}
        price_value = price_label.get("totalRentalPrice") or price_label.get("totalPrice")
    return {
        "khoanggia": price_value,
        "mota": detail.get("description"),
        "sophongngu": detail.get("totalBedroom"),
        "sophongvesinh": detail.get("totalBathroom"),
        "huongnha": first_label(detail.get("directions")),
        "huongbancong": first_label(detail.get("balconyDirection")),
        "sotang": detail.get("totalFloor"),
        "thuocduan": project.get("_id"),
        "street_ext": first_code_or_id(detail.get("street")),
        "ward_ext": first_code_or_id(detail.get("ward")),
        "lat": coords_to_lat_lng(detail.get("location"))[0],
        "lng": coords_to_lat_lng(detail.get("location"))[1],
        "img_count": len(media_urls(detail)) or None,
        "email": (detail.get("contact") or {}).get("email") or None,
    }


def get_candidates(args: argparse.Namespace, last_id: int = 0) -> List[Dict[str, Any]]:
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if args.id:
            cur.execute(
                """
                SELECT id, matin, url, status
                FROM scraped_details_flat
                WHERE id = %s
                LIMIT 1
                """,
                (args.id,),
            )
            return cur.fetchall()
        if args.matin:
            cur.execute(
                """
                SELECT id, matin, url, status
                FROM scraped_details_flat
                WHERE domain = %s AND matin = %s
                LIMIT 1
                """,
                (args.domain, args.matin),
            )
            return cur.fetchall()

        cur.execute(
            """
            SELECT id, matin, url, status
            FROM scraped_details_flat
            WHERE domain = %s
              AND id > %s
              AND (status IS NULL OR status = '')
              AND (%s = '' OR trade_type = %s)
            ORDER BY id ASC
            LIMIT %s
            """,
            (args.domain, last_id, args.trade_type, args.trade_type, args.batch_size),
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def claim_batch(args: argparse.Namespace, claim_status: str, batch_size: Optional[int] = None) -> List[Dict[str, Any]]:
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        take_n = batch_size or args.batch_size
        if args.only_missing_price:
            cur.execute(
                """
                SELECT id, matin, url, status
                FROM scraped_details_flat
                WHERE domain = %s
                  AND matin IS NOT NULL
                  AND (%s = '' OR trade_type = %s)
                  AND (khoanggia IS NULL OR TRIM(khoanggia) = '' OR TRIM(khoanggia) = '0')
                  AND (status IS NULL OR status = '' OR status = 'done_detail' OR status = 'detail_error')
                ORDER BY id ASC
                LIMIT %s
                """,
                (args.domain, args.trade_type, args.trade_type, take_n),
            )
        else:
            cur.execute(
                """
                SELECT id, matin, url, status
                FROM scraped_details_flat
                WHERE domain = %s
                  AND (status IS NULL OR status = '')
                  AND (%s = '' OR trade_type = %s)
                ORDER BY id ASC
                LIMIT %s
                """,
                (args.domain, args.trade_type, args.trade_type, take_n),
            )
        rows = cur.fetchall()
        if not rows:
            return []
        ids = [int(row["id"]) for row in rows]
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(
            f"UPDATE scraped_details_flat SET status = %s WHERE id IN ({placeholders})",
            [claim_status] + ids,
        )
        conn.commit()
        return rows
    finally:
        cur.close()
        conn.close()


def update_row(sdf_id: int, mapped: Dict[str, Any], status_value: str) -> None:
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE scraped_details_flat
            SET khoanggia = %s,
                mota = %s,
                sophongngu = %s,
                sophongvesinh = %s,
                huongnha = %s,
                huongbancong = %s,
                sotang = %s,
                thuocduan = %s,
                street_ext = %s,
                ward_ext = %s,
                lat = %s,
                lng = %s,
                img_count = %s,
                email = %s,
                status = %s
            WHERE id = %s
            """,
            (
                mapped["khoanggia"],
                mapped["mota"],
                str(mapped["sophongngu"]) if mapped["sophongngu"] is not None else None,
                str(mapped["sophongvesinh"]) if mapped["sophongvesinh"] is not None else None,
                mapped["huongnha"],
                mapped["huongbancong"],
                str(mapped["sotang"]) if mapped["sotang"] is not None else None,
                mapped["thuocduan"],
                mapped["street_ext"],
                mapped["ward_ext"],
                mapped["lat"],
                mapped["lng"],
                mapped["img_count"],
                mapped["email"],
                status_value,
                sdf_id,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def mark_error(sdf_id: int, status_value: str) -> None:
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE scraped_details_flat SET status = %s WHERE id = %s", (status_value, sdf_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def fetch_row(sdf_id: int) -> Dict[str, Any]:
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM scraped_details_flat WHERE id = %s", (sdf_id,))
        row = cur.fetchone()
        return row or {}
    finally:
        cur.close()
        conn.close()


def reset_processing_status(args: argparse.Namespace, claim_status: str, batch_size: int = 1000) -> int:
    db = Database()
    total_affected = 0
    while True:
        conn = db.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id
                FROM scraped_details_flat
                WHERE domain = %s AND status = %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (args.domain, claim_status, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                return total_affected
            ids = [int(row["id"]) for row in rows]
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(
                f"UPDATE scraped_details_flat SET status = NULL WHERE id IN ({placeholders})",
                ids,
            )
            affected = cur.rowcount
            conn.commit()
            total_affected += affected
        finally:
            cur.close()
            conn.close()


def reset_claimed_ids(ids: List[int], claim_status: str) -> int:
    if not ids:
        return 0
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(
            f"UPDATE scraped_details_flat SET status = NULL WHERE id IN ({placeholders}) AND status = %s",
            ids + [claim_status],
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        cur.close()
        conn.close()


def drain_queue_ids(task_queue: Queue) -> List[int]:
    drained_ids: List[int] = []
    while True:
        try:
            item = task_queue.get_nowait()
        except Empty:
            break
        try:
            if isinstance(item, dict):
                sdf_id = item.get("id")
                if sdf_id is not None:
                    drained_ids.append(int(sdf_id))
        finally:
            task_queue.task_done()
    return drained_ids


def worker_loop(
    worker_id: int,
    args: argparse.Namespace,
    task_queue: Queue,
    stop_event: threading.Event,
    state: Dict[str, Any],
    state_lock: threading.Lock,
) -> None:
    session = make_session(args)
    db = Database()
    worker_name = f"w{worker_id}"

    while not stop_event.is_set():
        try:
            row = task_queue.get(timeout=1.0)
        except Empty:
            continue

        if row is None:
            task_queue.task_done()
            break

        sdf_id = int(row["id"])
        matin = str(row["matin"])
        url = row.get("url")
        log_both(args, f"[FETCH] worker={worker_name} sdf_id={sdf_id} matin={matin} url={url}")

        try:
            detail = fetch_detail(session, matin, args)
            mapped = map_detail(detail)
            images = media_urls(detail)
            update_row(sdf_id, mapped, "done_detail")
            if images:
                db.add_detail_images(sdf_id, images)
            with state_lock:
                state["done_count"] += 1
                state["error_streak"] = 0
                state["last_id"] = sdf_id
                state["last_matin"] = matin
                done_count = state["done_count"]
                error_count = state["error_count"]
                error_streak = state["error_streak"]
            save_checkpoint(
                args,
                {
                    "status": "done",
                    "last_id": sdf_id,
                    "last_matin": matin,
                    "done_count": done_count,
                    "error_count": error_count,
                    "error_streak": error_streak,
                    "updated_at": utc_now(),
                },
            )
            log_both(args, f"[DONE_DETAIL] worker={worker_name} sdf_id={sdf_id} matin={matin} images={len(images)}")
        except Exception as exc:
            mark_error(sdf_id, "detail_error")
            with state_lock:
                state["error_count"] += 1
                state["error_streak"] += 1
                state["last_matin"] = matin
                done_count = state["done_count"]
                error_count = state["error_count"]
                error_streak = state["error_streak"]
            save_checkpoint(
                args,
                {
                    "status": "error",
                    "last_id": state.get("last_id", 0),
                    "last_matin": matin,
                    "done_count": done_count,
                    "error_count": error_count,
                    "error_streak": error_streak,
                    "error": str(exc),
                    "updated_at": utc_now(),
                },
            )
            log_both(args, f"[ERROR] worker={worker_name} sdf_id={sdf_id} matin={matin} error={str(exc)[:300]}")
            if error_streak >= args.stop_error_streak:
                log_both(
                    args,
                    f"[STOP_ERROR_STREAK] worker={worker_name} streak={error_streak} threshold={args.stop_error_streak} last_matin={matin}",
                )
                save_checkpoint(
                    args,
                    {
                        "status": "stop_error_streak",
                        "last_id": state.get("last_id", 0),
                        "last_matin": matin,
                        "done_count": done_count,
                        "error_count": error_count,
                        "error_streak": error_streak,
                        "updated_at": utc_now(),
                    },
                )
                stop_event.set()
        finally:
            task_queue.task_done()

        if stop_event.is_set():
            break

        sleep_s = random.uniform(args.min_delay, args.max_delay) if args.max_delay >= args.min_delay else args.min_delay
        if sleep_s > 0:
            log_both(args, f"[SLEEP] worker={worker_name} sdf_id={sdf_id} seconds={sleep_s:.2f}")
            time.sleep(sleep_s)


def main() -> int:
    args = parse_args()
    if args.max_delay < args.min_delay:
        raise SystemExit("--max-delay must be >= --min-delay")
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    apply_auth_overrides(args)
    log_both(
        args,
        f"[CONFIG] domain={args.domain} detail_api={args.detail_url_template} trade_type={args.trade_type or '-'} "
        f"workers={args.workers} batch={args.batch_size}",
    )

    claim_status = "processing_detail"
    if not args.id and not args.matin:
        reset_n = reset_processing_status(args, claim_status)
        if reset_n:
            log_both(args, f"[RESET_PROCESSING] rows={reset_n} status={claim_status}")

    if args.resume:
        cp = load_checkpoint(args.checkpoint_file)
        if cp:
            log_both(args, f"[RESUME] checkpoint_status={cp.get('status')} last_id={cp.get('last_id')}")

    state: Dict[str, Any] = {
        "done_count": 0,
        "error_count": 0,
        "error_streak": 0,
        "last_id": 0,
        "last_matin": "",
        "processed_total": 0,
    }
    state_lock = threading.Lock()
    stop_event = threading.Event()
    task_queue: Queue = Queue(maxsize=max(args.batch_size * 2, args.workers * 20))

    workers = []
    for worker_id in range(1, args.workers + 1):
        t = threading.Thread(
            target=worker_loop,
            args=(worker_id, args, task_queue, stop_event, state, state_lock),
            daemon=True,
        )
        t.start()
        workers.append(t)

    try:
        if args.id or args.matin:
            rows = get_candidates(args, 0)
            for row in rows:
                task_queue.put(row)
                with state_lock:
                    state["processed_total"] += 1
        else:
            while not stop_event.is_set():
                with state_lock:
                    processed_total = state["processed_total"]
                if args.limit_records and processed_total >= args.limit_records:
                    break

                remaining = args.limit_records - processed_total if args.limit_records else args.batch_size
                claim_n = min(args.batch_size, remaining) if remaining > 0 else 0
                if claim_n <= 0:
                    break

                rows = claim_batch(args, claim_status, claim_n)
                if not rows:
                    break
                log_both(args, f"[CLAIM] batch={len(rows)} status={claim_status}")
                for row in rows:
                    with state_lock:
                        if args.limit_records and state["processed_total"] >= args.limit_records:
                            stop_event.set()
                            break
                        state["processed_total"] += 1
                    task_queue.put(row)
                if stop_event.is_set():
                    break

        if stop_event.is_set():
            drained_ids = drain_queue_ids(task_queue)
            if drained_ids:
                reset_n = reset_claimed_ids(drained_ids, claim_status)
                log_both(
                    args,
                    f"[RESET_UNPROCESSED] drained={len(drained_ids)} reset_to_null={reset_n} status={claim_status}",
                )
        else:
            task_queue.join()
    finally:
        stop_event.set()
        for _ in workers:
            task_queue.put(None)
        for t in workers:
            t.join(timeout=2)

    last_id = int(state.get("last_id") or 0)
    if last_id:
        print(json.dumps(fetch_row(last_id), ensure_ascii=False, default=str, indent=2))
    log_both(
        args,
        f"[ALL_DONE] done_count={state['done_count']} error_count={state['error_count']} error_streak={state['error_streak']}",
    )
    return 1 if state["error_streak"] >= args.stop_error_streak else 0


if __name__ == "__main__":
    raise SystemExit(main())
