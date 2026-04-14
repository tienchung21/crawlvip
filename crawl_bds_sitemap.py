
import argparse
import hashlib
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse

import pymysql
from pymysql.err import OperationalError
from seleniumbase import SB

os.environ["no_proxy"] = "*"
if "http_proxy" in os.environ:
    del os.environ["http_proxy"]
if "https_proxy" in os.environ:
    del os.environ["https_proxy"]

INDEX_URL = "https://batdongsan.com.vn/sitemap/detailed-listings.xml"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_db_conn():
    return pymysql.connect(**DB_CONFIG)


def parse_batch_date_from_url(url: str):
    m = re.search(r"detailed-listings-(\d{8})-", url or "")
    return m.group(1) if m else None


def get_target_batch_date(offset_days: int):
    return (datetime.now() - timedelta(days=offset_days)).strftime("%Y%m%d")


def extract_prj_id_from_url(url: str):
    m = re.search(r"pr(\d+)", url or "")
    return int(m.group(1)) if m else None


def extract_url_base(url: str):
    s = (url or "").strip()
    if not s:
        return ""
    s = s.split("?", 1)[0].strip()
    return re.sub(r"-pr\d+$", "", s)


def url_base_md5(url_base: str):
    s = (url_base or "").strip()
    if not s:
        return None
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def ensure_bds_urlbase_schema():
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'collected_links'
              AND column_name = 'url_base'
            LIMIT 1
            """
        )
        if not cur.fetchone():
            try:
                cur.execute("ALTER TABLE collected_links ADD COLUMN url_base VARCHAR(700) NULL")
                conn.commit()
                print("[SCHEMA] Added collected_links.url_base")
            except OperationalError as e:
                # 1060: duplicate column (race with another process)
                if not (e.args and e.args[0] == 1060):
                    raise
                conn.rollback()

        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'collected_links'
              AND column_name = 'url_base_md5'
            LIMIT 1
            """
        )
        if not cur.fetchone():
            try:
                cur.execute("ALTER TABLE collected_links ADD COLUMN url_base_md5 CHAR(32) NULL")
                conn.commit()
                print("[SCHEMA] Added collected_links.url_base_md5")
            except OperationalError as e:
                if not (e.args and e.args[0] == 1060):
                    raise
                conn.rollback()

        cur.execute(
            """
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'collected_links'
              AND index_name = 'idx_bds_urlbase_lookup'
            LIMIT 1
            """
        )
        if not cur.fetchone():
            try:
                cur.execute(
                    """
                    ALTER TABLE collected_links
                    ADD INDEX idx_bds_urlbase_lookup (domain, url_base_md5, batch_date, id)
                    """
                )
                conn.commit()
                print("[INDEX] Added idx_bds_urlbase_lookup(domain,url_base_md5,batch_date,id)")
            except OperationalError as e:
                # 1061: duplicate key name
                if not (e.args and e.args[0] == 1061):
                    raise
                conn.rollback()
    finally:
        conn.close()


def backfill_bds_urlbase(limit_rows: int = 30000):
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE collected_links
                SET
                  url_base = REGEXP_REPLACE(SUBSTRING_INDEX(url, '?', 1), '-pr[0-9]+$', ''),
                  url_base_md5 = MD5(REGEXP_REPLACE(SUBSTRING_INDEX(url, '?', 1), '-pr[0-9]+$', ''))
                WHERE domain='batdongsan.com.vn'
                  AND (
                        url_base IS NULL OR url_base = ''
                        OR url_base_md5 IS NULL OR url_base_md5 = ''
                      )
                ORDER BY id
                LIMIT %s
                """,
                (int(limit_rows),),
            )
            affected = cur.rowcount
            if affected > 0:
                conn.commit()
            return affected
        except OperationalError as e:
            if e.args and e.args[0] == 1205:
                conn.rollback()
                print("[WARN] backfill_bds_urlbase lock wait timeout (1205), skip this cycle.")
                return 0
            raise
    finally:
        conn.close()


def get_existing_prj_ids(prj_ids):
    if not prj_ids:
        return set()
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(prj_ids))
        cur.execute(
            f"""
            SELECT DISTINCT prj_id
            FROM collected_links
            WHERE domain='batdongsan.com.vn'
              AND prj_id IN ({placeholders})
            """,
            tuple(prj_ids),
        )
        return {int(r["prj_id"]) for r in cur.fetchall() if r.get("prj_id") is not None}
    finally:
        conn.close()


def get_existing_url_bases(url_bases):
    if not url_bases:
        return set()
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        out = set()
        vals = [u for u in url_bases if u]
        chunk_size = 500
        for i in range(0, len(vals), chunk_size):
            chunk = vals[i : i + chunk_size]
            md5s = [url_base_md5(x) for x in chunk if url_base_md5(x)]
            if not md5s:
                continue
            placeholders = ",".join(["%s"] * len(md5s))
            cur.execute(
                f"""
                SELECT DISTINCT url_base
                FROM collected_links
                WHERE domain='batdongsan.com.vn'
                  AND url_base_md5 IN ({placeholders})
                """,
                tuple(md5s),
            )
            for r in cur.fetchall():
                ub = (r.get("url_base") or "").strip()
                if ub:
                    out.add(ub)
        return out
    finally:
        conn.close()


def insert_links(links, batch_date):
    if not links:
        return {"inserted_total": 0, "attempted_pending": 0, "attempted_postagain": 0}

    unique_links = []
    seen = set()
    for link in links:
        s = (link or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        unique_links.append(s)

    prj_by_url = {u: extract_prj_id_from_url(u) for u in unique_links}
    base_by_url = {u: extract_url_base(u) for u in unique_links}
    base_md5_by_url = {u: url_base_md5(base_by_url.get(u, "")) for u in unique_links}
    prj_candidates = sorted({p for p in prj_by_url.values() if p is not None})
    base_candidates = sorted({b for b in base_by_url.values() if b})
    existing_prj_ids = get_existing_prj_ids(prj_candidates)
    existing_url_bases = get_existing_url_bases(base_candidates)

    conn = get_db_conn()
    try:
        cur = conn.cursor()
        sql = """
            INSERT IGNORE INTO collected_links (url, domain, status, batch_date, prj_id, url_base, url_base_md5)
            VALUES (%s, 'batdongsan.com.vn', %s, %s, %s, %s, %s)
        """
        values = []
        for u in unique_links:
            prj_id = prj_by_url.get(u)
            ub = base_by_url.get(u, "")
            is_old_pr = prj_id is not None and prj_id in existing_prj_ids
            is_old_base = bool(ub) and ub in existing_url_bases
            status = "POSTAGAIN" if (is_old_pr or is_old_base) else "PENDING"
            values.append((u, status, batch_date, prj_id, ub, base_md5_by_url.get(u)))

        chunk_size = 1000
        total_inserted = 0
        attempted_pending = 0
        attempted_postagain = 0
        for i in range(0, len(values), chunk_size):
            chunk = values[i : i + chunk_size]
            cur.executemany(sql, chunk)
            conn.commit()
            affected = cur.rowcount
            total_inserted += affected
            for item in chunk:
                st = item[1]
                if st == "POSTAGAIN":
                    attempted_postagain += 1
                else:
                    attempted_pending += 1
        return {
            "inserted_total": total_inserted,
            "attempted_pending": attempted_pending,
            "attempted_postagain": attempted_postagain,
        }
    finally:
        conn.close()


def extract_links_in_child_sitemap(sb):
    links = sb.execute_script(
        "return Array.from(document.querySelectorAll('loc')).map(x => x.textContent || x.innerText)"
    )
    if not links:
        links = sb.execute_script(
            "return Array.from(document.querySelectorAll('url loc')).map(x => x.textContent || x.innerText)"
        )
    if not links:
        return []
    cleaned = []
    for x in links:
        s = str(x).strip()
        if s:
            cleaned.append(s)
    return cleaned


def get_bds_mapping():
    mapping = {
        "ban-can-ho-chung-cu-mini": ("Bán", "Căn hộ chung cư mini"),
        "ban-can-ho-chung-cu": ("Bán", "Căn hộ chung cư"),
        "ban-nha-rieng": ("Bán", "Nhà riêng"),
        "ban-nha-biet-thu-lien-ke": ("Bán", "Biệt thự liền kề"),
        "ban-nha-mat-pho": ("Bán", "Nhà mặt phố"),
        "ban-shophouse-nha-pho-thuong-mai": ("Bán", "Shophouse"),
        "ban-dat-nen-du-an": ("Bán", "Đất nền dự án"),
        "ban-dat": ("Bán", "Đất"),
        "ban-trang-trai-khu-nghi-duong": ("Bán", "Trang trại/Khu nghỉ dưỡng"),
        "ban-condotel": ("Bán", "Condotel"),
        "ban-kho-nha-xuong": ("Bán", "Kho, nhà xưởng"),
        "ban-loai-bat-dong-san-khac": ("Bán", "BĐS khác"),
        "cho-thue-can-ho-chung-cu-mini": ("Thuê", "Căn hộ chung cư mini"),
        "cho-thue-can-ho-chung-cu": ("Thuê", "Căn hộ chung cư"),
        "cho-thue-nha-rieng": ("Thuê", "Nhà riêng"),
        "cho-thue-nha-biet-thu-lien-ke": ("Thuê", "Biệt thự liền kề"),
        "cho-thue-nha-mat-pho": ("Thuê", "Nhà mặt phố"),
        "cho-thue-shophouse-nha-pho-thuong-mai": ("Thuê", "Shophouse"),
        "cho-thue-nha-tro-phong-tro": ("Thuê", "Nhà trọ, phòng trọ"),
        "cho-thue-van-phong": ("Thuê", "Văn phòng"),
        "cho-thue-sang-nhuong-cua-hang-ki-ot": ("Thuê", "Cửa hàng, Ki-ốt"),
        "cho-thue-kho-nha-xuong-dat": ("Thuê", "Kho, nhà xưởng, đất"),
        "cho-thue-loai-bat-dong-san-khac": ("Thuê", "BĐS khác"),
    }
    return dict(sorted(mapping.items(), key=lambda x: len(x[0]), reverse=True))


def update_prj_id_for_batch(batch_date: str):
    conn = get_db_conn()
    updated = 0
    failed_to_zero = 0
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, url
            FROM collected_links
            WHERE domain='batdongsan.com.vn'
              AND batch_date=%s
              AND prj_id IS NULL
            """,
            (batch_date,),
        )
        rows = cur.fetchall()
        if not rows:
            return {"scanned": 0, "updated": 0, "marked_zero": 0}

        ok_updates = []
        zero_updates = []
        pattern = re.compile(r"pr(\d+)")
        for r in rows:
            lid = r["id"]
            url = r["url"] or ""
            m = pattern.search(url)
            if m:
                ok_updates.append((int(m.group(1)), lid))
            else:
                zero_updates.append((0, lid))

        sql = "UPDATE collected_links SET prj_id=%s WHERE id=%s"
        if ok_updates:
            cur.executemany(sql, ok_updates)
            conn.commit()
            updated += len(ok_updates)
        if zero_updates:
            cur.executemany(sql, zero_updates)
            conn.commit()
            failed_to_zero += len(zero_updates)

        return {"scanned": len(rows), "updated": updated, "marked_zero": failed_to_zero}
    finally:
        conn.close()


def classify_batch(batch_date: str):
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, url
            FROM collected_links
            WHERE domain='batdongsan.com.vn'
              AND batch_date=%s
              AND (loaihinh IS NULL OR loaihinh='')
            """,
            (batch_date,),
        )
        rows = cur.fetchall()
        if not rows:
            return {"scanned": 0, "matched": 0, "updated": 0}

        mapping = get_bds_mapping()
        updates = []
        for r in rows:
            lid = r["id"]
            url = r["url"] or ""
            try:
                p = urlparse(url)
                path = (p.path or "").lstrip("/")
            except Exception:
                path = url.replace("https://batdongsan.com.vn/", "").replace("https://www.batdongsan.com.vn/", "")

            for prefix, (trade, ptype) in mapping.items():
                if path.startswith(prefix + "-") or path == prefix:
                    updates.append((ptype, trade, lid))
                    break

        if updates:
            cur.executemany(
                """
                UPDATE collected_links
                SET loaihinh=%s, trade_type=%s, updated_at=NOW()
                WHERE id=%s
                """,
                updates,
            )
            conn.commit()
        return {"scanned": len(rows), "matched": len(updates), "updated": len(updates)}
    finally:
        conn.close()


def run_once(target_batch_date: str, headless: bool, wait_index_sec: float, wait_child_sec: float):
    print(f"\n=== RUN ONCE | target_batch_date={target_batch_date} ===")
    ensure_bds_urlbase_schema()
    backfilled = backfill_bds_urlbase(limit_rows=30000)
    if backfilled:
        print(f"[BACKFILL] url_base/url_base_md5 filled: {backfilled}")
    total_inserted = 0
    total_child = 0
    total_links_seen = 0

    with SB(uc=True, headless=headless, page_load_strategy="eager") as sb:
        print(f"Fetching index: {INDEX_URL}")
        sb.open(INDEX_URL)
        sb.sleep(wait_index_sec)

        content_index = sb.get_page_source()
        child_urls = re.findall(r"<loc>(https://.*?)</loc>", content_index)
        child_urls = sorted(set(child_urls))
        print(f"Found child sitemaps in index: {len(child_urls)}")

        target_children = [u for u in child_urls if parse_batch_date_from_url(u) == target_batch_date]
        target_children.sort(reverse=True)
        print(f"Children for batch {target_batch_date}: {len(target_children)}")

        for idx, child_url in enumerate(target_children, start=1):
            print(f"[{idx}/{len(target_children)}] Processing {child_url}")
            sb.open(child_url)
            sb.sleep(wait_child_sec)

            links = extract_links_in_child_sitemap(sb)
            count = len(links)
            total_links_seen += count
            total_child += 1
            print(f"  -> found links={count}")

            if count > 0:
                insert_stats = insert_links(links, target_batch_date)
                total_inserted += insert_stats["inserted_total"]
                print(
                    "  -> inserted new="
                    f"{insert_stats['inserted_total']} "
                    f"(attempted PENDING={insert_stats['attempted_pending']}, "
                    f"attempted POSTAGAIN={insert_stats['attempted_postagain']})"
                )
            else:
                print("  -> no links extracted")

    print("=== RUN DONE ===")
    print(
        f"target_batch_date={target_batch_date} | child_processed={total_child} "
        f"| links_seen={total_links_seen} | inserted_total={total_inserted}"
    )
    prj_stats = update_prj_id_for_batch(target_batch_date)
    print(
        f"prj_id_update: scanned={prj_stats['scanned']} "
        f"updated={prj_stats['updated']} marked_zero={prj_stats['marked_zero']}"
    )
    cls_stats = classify_batch(target_batch_date)
    print(
        f"classify: scanned={cls_stats['scanned']} matched={cls_stats['matched']} "
        f"updated={cls_stats['updated']}"
    )


def run_postagain_backfill(batch_size: int = 30000):
    script_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "craw",
        "auto",
        "bds_backfill_postagain_history.py",
    )
    cmd = [
        sys.executable,
        "-u",
        script_path,
        "--batch-size",
        str(int(batch_size)),
        "--loop-until-done",
    ]
    print(f"[POSTAGAIN] RUN: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print("[POSTAGAIN] DONE")


def main():
    parser = argparse.ArgumentParser(description="Batdongsan sitemap crawler (daily yesterday batch).")
    parser.add_argument("--loop", action="store_true", help="Run forever in loop mode.")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=86400,
        help="Sleep seconds between cycles in loop mode (default 86400).",
    )
    parser.add_argument(
        "--offset-days",
        type=int,
        default=1,
        help="Target batch date = today - offset_days (default 1 => yesterday).",
    )
    parser.add_argument(
        "--target-date",
        default="",
        help="Manual target batch date in YYYYMMDD. If provided, overrides offset-days for one run/cycle.",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser headless.")
    parser.add_argument("--wait-index-sec", type=float, default=10.0, help="Sleep after loading index.")
    parser.add_argument("--wait-child-sec", type=float, default=8.0, help="Sleep after loading child sitemap.")
    parser.add_argument(
        "--postagain-batch-size",
        type=int,
        default=30000,
        help="Batch size passed to bds_backfill_postagain_history.py after sitemap run (default 30000).",
    )
    args = parser.parse_args()

    def resolve_target_date():
        if args.target_date:
            return args.target_date.strip()
        return get_target_batch_date(max(0, int(args.offset_days)))

    if not args.loop:
        run_once(
            target_batch_date=resolve_target_date(),
            headless=args.headless,
            wait_index_sec=args.wait_index_sec,
            wait_child_sec=args.wait_child_sec,
        )
        run_postagain_backfill(batch_size=max(1, int(args.postagain_batch_size)))
        return

    cycle = 0
    print(f"Loop mode enabled. interval_seconds={args.interval_seconds}")
    while True:
        cycle += 1
        started = datetime.now()
        target_date = resolve_target_date()
        print(f"\n===== CYCLE {cycle} START {started.strftime('%Y-%m-%d %H:%M:%S')} | target={target_date} =====")
        try:
            run_once(
                target_batch_date=target_date,
                headless=args.headless,
                wait_index_sec=args.wait_index_sec,
                wait_child_sec=args.wait_child_sec,
            )
            run_postagain_backfill(batch_size=max(1, int(args.postagain_batch_size)))
        except Exception as e:
            print(f"[ERROR] cycle={cycle} failed: {e}")
        ended = datetime.now()
        print(f"===== CYCLE {cycle} END {ended.strftime('%Y-%m-%d %H:%M:%S')} =====")
        time.sleep(max(1, int(args.interval_seconds)))


if __name__ == "__main__":
    main()
