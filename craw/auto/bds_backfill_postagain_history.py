#!/usr/bin/env python3
"""
Backfill POSTAGAIN + history for batdongsan collected_links.

Rules:
- Domain fixed: batdongsan.com.vn
- Process rows with history_flag IN (NULL, 1) for re-run with new logic
- For each row:
  - if same prj_id already appeared in an older row: mark row status=POSTAGAIN
  - append repost row id to original row's history CSV
- set history_flag=2 (processed by this script version), so never re-process this row
"""

import argparse
import hashlib
import time
from datetime import datetime

import pymysql
from pymysql.err import OperationalError


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

DOMAIN = "batdongsan.com.vn"


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def column_exists(conn, table, column):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """,
            (table, column),
        )
        return cur.fetchone() is not None


def index_exists(conn, table, index_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = %s
              AND index_name = %s
            LIMIT 1
            """,
            (table, index_name),
        )
        return cur.fetchone() is not None


def ensure_schema_and_indexes(conn):
    changed = False

    if not column_exists(conn, "collected_links", "history"):
        print("[SCHEMA] Adding column collected_links.history TEXT NULL ...")
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE collected_links ADD COLUMN history TEXT NULL")
        conn.commit()
        changed = True
        print("[SCHEMA] Added history.")
    else:
        print("[SCHEMA] history already exists.")

    if not column_exists(conn, "collected_links", "history_flag"):
        print("[SCHEMA] Adding column collected_links.history_flag TINYINT(1) NULL DEFAULT NULL ...")
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE collected_links ADD COLUMN history_flag TINYINT(1) NULL DEFAULT NULL")
        conn.commit()
        changed = True
        print("[SCHEMA] Added history_flag.")
    else:
        print("[SCHEMA] history_flag already exists.")

    if not column_exists(conn, "collected_links", "url_base"):
        print("[SCHEMA] Adding column collected_links.url_base VARCHAR(700) NULL ...")
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE collected_links ADD COLUMN url_base VARCHAR(700) NULL")
        conn.commit()
        changed = True
        print("[SCHEMA] Added url_base.")
    else:
        print("[SCHEMA] url_base already exists.")

    if not column_exists(conn, "collected_links", "url_base_md5"):
        print("[SCHEMA] Adding column collected_links.url_base_md5 CHAR(32) NULL ...")
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE collected_links ADD COLUMN url_base_md5 CHAR(32) NULL")
        conn.commit()
        changed = True
        print("[SCHEMA] Added url_base_md5.")
    else:
        print("[SCHEMA] url_base_md5 already exists.")

    # Index 1: fast scan rows pending backfill by domain + history_flag + time order
    if not index_exists(conn, "collected_links", "idx_bds_history_scan"):
        print("[INDEX] Adding idx_bds_history_scan(domain, history_flag, batch_date, id) ...")
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE collected_links
                ADD INDEX idx_bds_history_scan (domain, history_flag, batch_date, id)
                """
            )
        conn.commit()
        changed = True
        print("[INDEX] Added idx_bds_history_scan.")
    else:
        print("[INDEX] idx_bds_history_scan already exists.")

    # Index 2: fast lookup older/original row by prj_id
    if not index_exists(conn, "collected_links", "idx_bds_prj_lookup"):
        print("[INDEX] Adding idx_bds_prj_lookup(domain, prj_id, batch_date, id) ...")
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE collected_links
                ADD INDEX idx_bds_prj_lookup (domain, prj_id, batch_date, id)
                """
            )
        conn.commit()
        changed = True
        print("[INDEX] Added idx_bds_prj_lookup.")
    else:
        print("[INDEX] idx_bds_prj_lookup already exists.")

    if not index_exists(conn, "collected_links", "idx_bds_urlbase_lookup"):
        print("[INDEX] Adding idx_bds_urlbase_lookup(domain, url_base_md5, batch_date, id) ...")
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE collected_links
                ADD INDEX idx_bds_urlbase_lookup (domain, url_base_md5, batch_date, id)
                """
            )
        conn.commit()
        changed = True
        print("[INDEX] Added idx_bds_urlbase_lookup.")
    else:
        print("[INDEX] idx_bds_urlbase_lookup already exists.")

    return changed


def normalize_history_ids(history_text):
    if not history_text:
        return []
    out = []
    seen = set()
    for x in str(history_text).split(","):
        s = x.strip()
        if not s or not s.isdigit() or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def url_base_from_url(url):
    s = (url or "").strip()
    if not s:
        return ""
    s = s.split("?", 1)[0].strip()
    return s.rsplit("-pr", 1)[0] if "-pr" in s else s


def url_base_md5(url_base):
    s = (url_base or "").strip()
    if not s:
        return None
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def fetch_pending_rows(conn, limit_rows):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, prj_id, batch_date, status, url, url_base, url_base_md5
            FROM collected_links
            WHERE domain = %s
              AND prj_id IS NOT NULL
              AND prj_id > 0
              AND (history_flag IS NULL OR history_flag = 1)
            ORDER BY batch_date ASC, id ASC
            LIMIT %s
            """,
            (DOMAIN, limit_rows),
        )
        return cur.fetchall()


def get_original_row(conn, prj_id, url_base):
    ub_md5 = url_base_md5(url_base)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, batch_date, history
            FROM collected_links
            WHERE domain = %s
              AND (
                    prj_id = %s
                    OR (url_base_md5 = %s AND url_base = %s)
                  )
            ORDER BY batch_date ASC, id ASC
            LIMIT 1
            """,
            (DOMAIN, prj_id, ub_md5, url_base),
        )
        return cur.fetchone()


def backfill_url_base_metadata(conn, limit_rows=50000):
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                UPDATE collected_links
                SET
                  url_base = REGEXP_REPLACE(SUBSTRING_INDEX(url, '?', 1), '-pr[0-9]+$', ''),
                  url_base_md5 = MD5(REGEXP_REPLACE(SUBSTRING_INDEX(url, '?', 1), '-pr[0-9]+$', ''))
                WHERE domain = %s
                  AND (
                        url_base IS NULL OR url_base = ''
                        OR url_base_md5 IS NULL OR url_base_md5 = ''
                      )
                ORDER BY id
                LIMIT %s
                """,
                (DOMAIN, int(limit_rows)),
            )
            affected = cur.rowcount
        except OperationalError as e:
            if e.args and e.args[0] == 1205:
                conn.rollback()
                print("[WARN] backfill_url_base_metadata lock wait timeout (1205), skip this cycle.")
                return 0
            raise
    if affected > 0:
        conn.commit()
    return affected


def process_once(conn, batch_size=3, dry_run=False, verbose_first_seen=False):
    rows = fetch_pending_rows(conn, batch_size)
    if not rows:
        print("[RUN] No rows with history_flag in (NULL,1).")
        return {"scanned": 0, "repost": 0, "history_updated": 0}

    print(f"[RUN] fetched={len(rows)} rows (history_flag in NULL/1)")
    repost_count = 0
    hist_update_count = 0
    commit_every = 500
    writes_since_commit = 0
    scanned_since_log = 0

    for r in rows:
        cid = int(r["id"])
        prj_id = int(r["prj_id"])
        c_batch = r.get("batch_date")
        c_status = r.get("status")
        c_url = r.get("url") or ""
        c_base = (r.get("url_base") or "").strip() or url_base_from_url(c_url)
        c_base_md5 = (r.get("url_base_md5") or "").strip() or url_base_md5(c_base)

        # Single lookup per row:
        # earliest row matching same prj_id OR same URL-base.
        # If earliest row is itself -> first seen, else -> repost.
        orig = get_original_row(conn, prj_id, c_base)
        if orig and int(orig["id"]) != cid:
                oid = int(orig["id"])
                o_batch = orig.get("batch_date")
                hist_ids = normalize_history_ids(orig.get("history"))
                sid = str(cid)
                history_changed = False
                if sid not in hist_ids:
                    hist_ids.append(sid)
                    history_changed = True
                new_history = ",".join(hist_ids)

                print(
                    f"[ITEM] repost_id={cid} prj_id={prj_id} batch={c_batch} status={c_status} "
                    f"-> original_id={oid} original_batch={o_batch}"
                )
                if history_changed:
                    print(f"       history update: '{orig.get('history')}' -> '{new_history}'")
                else:
                    print(f"       history unchanged: '{orig.get('history')}'")

                if not dry_run:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE collected_links
                            SET status='POSTAGAIN',
                                history_flag=2,
                                url_base=%s,
                                url_base_md5=%s
                            WHERE id=%s AND domain=%s
                            """,
                            (c_base, c_base_md5, cid, DOMAIN),
                        )
                        if history_changed:
                            cur.execute(
                                "UPDATE collected_links SET history=%s WHERE id=%s AND domain=%s",
                                (new_history, oid, DOMAIN),
                            )
                    writes_since_commit += 1
                repost_count += 1
                if history_changed:
                    hist_update_count += 1
        else:
            if verbose_first_seen:
                print(f"[ITEM] id={cid} prj_id={prj_id} batch={c_batch} -> first seen (keep status={c_status})")
            if not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE collected_links
                        SET history_flag=2,
                            url_base=%s,
                            url_base_md5=%s
                        WHERE id=%s AND domain=%s
                        """,
                        (c_base, c_base_md5, cid, DOMAIN),
                    )
                writes_since_commit += 1

        scanned_since_log += 1
        if not dry_run and writes_since_commit >= commit_every:
            conn.commit()
            writes_since_commit = 0
        if scanned_since_log >= commit_every:
            print(
                f"[PROGRESS] scanned_in_cycle={scanned_since_log} "
                f"repost={repost_count} history_updated={hist_update_count}"
            )
            scanned_since_log = 0

    if not dry_run and writes_since_commit > 0:
        conn.commit()

    print(
        f"[RUN] scanned={len(rows)} repost={repost_count} history_updated={hist_update_count} dry_run={dry_run}"
    )
    return {"scanned": len(rows), "repost": repost_count, "history_updated": hist_update_count}


def main():
    parser = argparse.ArgumentParser(description="Backfill POSTAGAIN/history using history_flag for batdongsan.")
    parser.add_argument("--batch-size", type=int, default=3, help="Rows per run (default 3).")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no DB update.")
    parser.add_argument(
        "--loop-until-done",
        action="store_true",
        help="Repeat batch runs until no history_flag IS NULL rows left.",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="Sleep between cycles when looping.")
    parser.add_argument(
        "--verbose-first-seen",
        action="store_true",
        help="Log each first-seen row (very noisy, slower). Default: off.",
    )
    args = parser.parse_args()

    batch_size = max(1, int(args.batch_size))
    sleep_seconds = max(0.0, float(args.sleep_seconds))

    print(
        "=== START "
        f"{now_str()} | domain={DOMAIN} | batch_size={batch_size} "
        f"| dry_run={args.dry_run} | loop_until_done={args.loop_until_done} ==="
    )

    conn = pymysql.connect(**DB_CONFIG)
    try:
        ensure_schema_and_indexes(conn)
        filled = backfill_url_base_metadata(conn, limit_rows=max(5000, batch_size))
        if filled:
            print(f"[BACKFILL] url_base rows filled: {filled}")

        cycle = 0
        while True:
            cycle += 1
            print(f"\n[CYCLE {cycle}] {now_str()}")
            try:
                stats = process_once(
                    conn,
                    batch_size=batch_size,
                    dry_run=args.dry_run,
                    verbose_first_seen=args.verbose_first_seen,
                )
            except OperationalError as e:
                # 1205 = lock wait timeout, 1213 = deadlock; keep loop alive and retry next cycle
                if e.args and e.args[0] in (1205, 1213):
                    conn.rollback()
                    print(
                        f"[WARN] db lock conflict ({e.args[0]}) on cycle {cycle}, "
                        f"retry after {sleep_seconds}s"
                    )
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                    continue
                raise
            if stats["scanned"] == 0:
                print("[DONE] No more rows to process.")
                break
            if not args.loop_until_done:
                break
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    finally:
        conn.close()

    print(f"=== END {now_str()} ===")


if __name__ == "__main__":
    main()
