#!/usr/bin/env python3
"""
Guland daily crawl job (API district-based):
1) Link phase:
   - Read province_id/district_id from location_guland
   - Call /listing/generate-url per district
   - Crawl listing pages for sale + rent
   - Stop current district trade when duplicate links reach threshold
2) Detail phase:
   - Run guland_detail_crawler.py --full
"""

from __future__ import annotations

import argparse
import json
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from curl_cffi import requests
except Exception as exc:
    raise RuntimeError("curl_cffi is required for guland_daily_api_job.py") from exc

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from database import Database  # noqa: E402


BASE_URL = "https://guland.vn"
DOMAIN = "guland.vn"
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"

API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://guland.vn",
    "Referer": "https://guland.vn/",
}

HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Referer": "https://guland.vn/",
}


@dataclass
class LocationRow:
    province_id: int
    province_name: str
    district_id: int
    district_name: str


class Logger:
    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def log(self, msg: str) -> None:
        line = msg.rstrip()
        print(line, flush=True)
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def parse_int_csv(v: str) -> List[int]:
    if not v:
        return []
    out: List[int] = []
    for p in v.split(","):
        p = p.strip()
        if not p:
            continue
        out.append(int(p))
    return out


def make_session() -> requests.Session:
    s = requests.Session()
    return s


def is_blocked(html_text: str) -> bool:
    t = (html_text or "").lower()
    markers = [
        "attention required!",
        "verify you are human",
        "please verify you are a human",
        "/cdn-cgi/challenge-platform/",
        "cf-chl-bypass",
        "g-recaptcha",
        "hcaptcha",
        "captcha",
    ]
    return any(m in t for m in markers)


def extract_links(html_text: str) -> List[str]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    links: List[str] = []
    for a in soup.select(".c-sdb-card__tle a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        u = urljoin(BASE_URL, href)
        if "/post/" not in u:
            continue
        links.append(u)
    seen = set()
    out = []
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def build_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page}"


def generate_path(
    session: requests.Session,
    province_id: int,
    district_id: int,
    bds_method: str,
    timeout: int,
    retries: int,
) -> Optional[str]:
    params = {
        "sort": "",
        "status": "",
        "project_id": "",
        "bds_type_new": "",
        "province_id": str(province_id),
        "district_id": str(district_id),
        "ward_id": "",
        "road_id": "",
        "bds_method": bds_method,
        "bds_type": "",
        "size": "",
        "price": "",
        "number_floor": "",
        "number_bedrooms": "",
        "only-coord": "0",
    }
    url = BASE_URL + "/listing/generate-url"
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                url,
                params=params,
                headers=API_HEADERS,
                impersonate="chrome124",
                timeout=timeout,
            )
            resp.raise_for_status()
            obj = resp.json()
            if int(obj.get("status") or 0) != 1:
                return None
            data = obj.get("data")
            if not data:
                return None
            path = str(data).strip()
            if not path.startswith("/"):
                path = "/" + path
            return path
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 * attempt, 8))
                continue
    raise RuntimeError(
        f"generate_path failed province_id={province_id} district_id={district_id} "
        f"bds_method={bds_method}: {last_error}"
    )


def to_rent_path(sale_path: str) -> str:
    p = sale_path or ""
    p = p.replace("/mua-ban-bat-dong-san-", "/cho-thue-bat-dong-san-")
    p = p.replace("/mua-ban-", "/cho-thue-")
    return p


def fetch_html(session: requests.Session, url: str, timeout: int, retries: int) -> Optional[str]:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                url,
                headers=HTML_HEADERS,
                impersonate="chrome124",
                timeout=timeout,
            )
            if resp.status_code != 200:
                if attempt < retries:
                    time.sleep(min(2 * attempt, 8))
                    continue
                return None
            return resp.text or ""
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 * attempt, 8))
                continue
    if last_error:
        return None
    return None


def get_locations(
    db: Database,
    province_ids: Sequence[int],
    district_ids: Sequence[int],
) -> List[LocationRow]:
    where = []
    params: List[object] = []
    if province_ids:
        where.append("province_id IN ({})".format(",".join(["%s"] * len(province_ids))))
        params.extend(province_ids)
    if district_ids:
        where.append("district_id IN ({})".format(",".join(["%s"] * len(district_ids))))
        params.extend(district_ids)
    sql = """
        SELECT DISTINCT province_id, province_name, district_id, district_name
        FROM location_guland
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY province_id ASC, district_id ASC"

    conn = db.get_connection(True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    out: List[LocationRow] = []
    for r in rows:
        if not r.get("province_id") or not r.get("district_id"):
            continue
        out.append(
            LocationRow(
                province_id=int(r["province_id"]),
                province_name=str(r.get("province_name") or ""),
                district_id=int(r["district_id"]),
                district_name=str(r.get("district_name") or ""),
            )
        )
    return out


def run_link_phase(args: argparse.Namespace, logger: Logger) -> Tuple[int, int]:
    db = Database()
    session = make_session()
    locations = get_locations(
        db=db,
        province_ids=parse_int_csv(args.province_ids),
        district_ids=parse_int_csv(args.district_ids),
    )
    if args.max_districts > 0:
        locations = locations[: args.max_districts]

    logger.log(
        f"[LINK_START] districts={len(locations)} page_max={args.max_pages_per_district} "
        f"dup_threshold={args.dup_stop_threshold}"
    )

    total_added = 0
    total_seen_links = 0

    for idx, loc in enumerate(locations, start=1):
        logger.log(
            f"[DISTRICT] {idx}/{len(locations)} province_id={loc.province_id} "
            f"province={loc.province_name} district_id={loc.district_id} district={loc.district_name}"
        )

        sale_path = generate_path(
            session=session,
            province_id=loc.province_id,
            district_id=loc.district_id,
            bds_method="mua-ban",
            timeout=args.timeout,
            retries=args.max_retries,
        )
        if not sale_path:
            logger.log("  [SKIP_DISTRICT] sale_path_empty")
            continue

        rent_path = to_rent_path(sale_path)
        trade_paths = [("s", sale_path), ("u", rent_path)]

        for trade_type, base_path in trade_paths:
            base_url = urljoin(BASE_URL, base_path)
            logger.log(f"  [TRADE] type={trade_type} base_url={base_url}")

            dup_consecutive = 0
            empty_pages = 0

            for page in range(1, args.max_pages_per_district + 1):
                page_url = build_page_url(base_url, page)
                html = fetch_html(
                    session=session,
                    url=page_url,
                    timeout=args.timeout,
                    retries=args.max_retries,
                )
                if not html:
                    logger.log(f"    [PAGE_FAIL] page={page} url={page_url}")
                    break
                if is_blocked(html):
                    logger.log(f"    [BLOCKED] page={page} url={page_url}")
                    break

                links = extract_links(html)
                found = len(links)
                logger.log(f"    [PAGE] page={page} found={found}")
                if found == 0:
                    empty_pages += 1
                    if empty_pages >= args.stop_empty_pages:
                        logger.log(f"    [STOP_EMPTY] empty_pages={empty_pages}")
                        break
                    continue

                empty_pages = 0
                total_seen_links += found
                added = db.add_collected_links(
                    links,
                    domain=DOMAIN,
                    loaihinh="Guland",
                    trade_type=trade_type,
                    city_name=loc.province_name or str(loc.province_id),
                )
                total_added += added
                logger.log(
                    f"      [ADD] added={added} dup={found - added} "
                    f"dup_consecutive={dup_consecutive}/{args.dup_stop_threshold}"
                )
                if added == 0:
                    dup_consecutive += found
                else:
                    dup_consecutive = 0

                if args.dup_stop_threshold > 0 and dup_consecutive >= args.dup_stop_threshold:
                    logger.log(
                        f"    [STOP_DUP] reached={dup_consecutive} "
                        f"threshold={args.dup_stop_threshold}"
                    )
                    break

                if args.delay_min > 0 or args.delay_max > 0:
                    sleep_s = random.uniform(args.delay_min, args.delay_max)
                    logger.log(f"      [SLEEP] {sleep_s:.2f}s")
                    time.sleep(sleep_s)

    logger.log(f"[LINK_DONE] total_seen_links={total_seen_links} total_added={total_added}")
    return total_seen_links, total_added


def run_detail_phase(args: argparse.Namespace, logger: Logger) -> int:
    detail_script = Path(__file__).resolve().parents[2] / "guland_detail_crawler.py"
    cmd = [
        sys.executable,
        str(detail_script),
        "--full",
        "--batch-limit",
        str(args.detail_batch_limit),
        "--delay-min-seconds",
        str(args.detail_delay_min),
        "--delay-max-seconds",
        str(args.detail_delay_max),
        "--max-consecutive-block",
        str(args.max_consecutive_block),
    ]
    logger.log("[DETAIL_START] " + " ".join(cmd))
    rc = subprocess.call(cmd)
    logger.log(f"[DETAIL_DONE] return_code={rc}")
    return rc


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Daily Guland API crawl job")
    ap.add_argument("--mode", choices=["all", "link", "detail"], default="all")
    ap.add_argument("--province-ids", default="", help="Comma list, empty=all from location_guland")
    ap.add_argument("--district-ids", default="", help="Comma list, empty=all from location_guland")
    ap.add_argument("--max-districts", type=int, default=0, help="Limit districts for testing")
    ap.add_argument("--max-pages-per-district", type=int, default=300)
    ap.add_argument("--dup-stop-threshold", type=int, default=200)
    ap.add_argument("--stop-empty-pages", type=int, default=2)
    ap.add_argument("--delay-min", type=float, default=0.8)
    ap.add_argument("--delay-max", type=float, default=1.6)
    ap.add_argument("--timeout", type=int, default=40)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--detail-batch-limit", type=int, default=200)
    ap.add_argument("--detail-delay-min", type=float, default=0.5)
    ap.add_argument("--detail-delay-max", type=float, default=1.5)
    ap.add_argument("--max-consecutive-block", type=int, default=3)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    log_file = LOG_DIR / f"guland_daily_api_{datetime.now().strftime('%Y-%m-%d')}.log"
    logger = Logger(log_file)
    logger.log(
        f"[START] mode={args.mode} max_pages={args.max_pages_per_district} "
        f"dup_stop={args.dup_stop_threshold}"
    )
    if args.mode in ("all", "link"):
        run_link_phase(args, logger)
    if args.mode in ("all", "detail"):
        rc = run_detail_phase(args, logger)
        if rc != 0:
            return rc
    logger.log("[DONE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
