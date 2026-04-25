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
import fcntl
import json
import os
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
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
LOCK_FILE = LOG_DIR / "guland_daily_api.lock"

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


def extract_total_results(html_text: str) -> Optional[int]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    node = soup.select_one("#total")
    if not node:
        return None
    raw = node.get_text(" ", strip=True)
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return None
    return int(digits)


def build_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page}"


def format_district_id_for_api(district_id: int, district_name: str = "") -> str:
    """
    Guland location API has mixed id formats:
    - old district ids: usually <= 3 digits (must keep as-is, e.g. 271)
    - ward/xa ids: often need zero-padding to 5 (e.g. 136 -> 00136, 9634 -> 09634)
    - 5+ digits: keep as-is
    """
    name = (district_name or "").strip().lower()
    if name.startswith("xã ") or name.startswith("phường "):
        return f"{district_id:05d}" if district_id < 100000 else str(district_id)
    # Old district-level ids (quận/huyện/thị xã/thành phố) are expected as 3-digit
    # for small ids in this endpoint: 3 -> 003, 16 -> 016, 271 -> 271.
    if district_id > 0 and district_id < 1000:
        return f"{district_id:03d}"
    if district_id >= 1000 and district_id <= 9999:
        return f"{district_id:05d}"
    return str(district_id)


def format_province_id_for_api(province_id: int) -> str:
    # generate-link expects 2-digit legacy ids for many provinces (e.g. Hanoi -> "01").
    if province_id > 0 and province_id < 100:
        return f"{province_id:02d}"
    return str(province_id)


def generate_path(
    session: requests.Session,
    province_id: int,
    district_id: int,
    district_name: str,
    bds_method: str,
    timeout: int,
    retries: int,
) -> Optional[str]:
    trade = "sell" if bds_method == "mua-ban" else "rent"
    district_fmt = format_district_id_for_api(district_id, district_name)
    province_fmt = format_province_id_for_api(province_id)
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            # Primary endpoint (canonical slug includes province suffix when available).
            params_link = {
                "province_id": province_fmt,
                "district_id": district_fmt,
                "ward_id": "",
                "road_id": "0",
                "user_map_id": "",
                "prev_url": urljoin(BASE_URL, "/mua-ban-bat-dong-san-tp-ho-chi-minh"),
                "current_url": urljoin(BASE_URL, "/mua-ban-bat-dong-san"),
            }
            resp = session.get(
                BASE_URL + "/generate-link",
                params=params_link,
                headers=API_HEADERS,
                impersonate="chrome124",
                timeout=timeout,
            )
            resp.raise_for_status()
            obj = resp.json()
            if int(obj.get("status") or 0) == 1:
                data = obj.get("data") or {}
                if isinstance(data, dict):
                    path = (data.get(trade) or "").strip()
                    if path:
                        if not path.startswith("/"):
                            path = "/" + path
                        return path

            # Fallback endpoint (legacy behavior).
            params_old = {
                "sort": "",
                "status": "",
                "project_id": "",
                "bds_type_new": "",
                "province_id": str(province_id),
                "district_id": district_fmt,
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
            resp_old = session.get(
                BASE_URL + "/listing/generate-url",
                params=params_old,
                headers=API_HEADERS,
                impersonate="chrome124",
                timeout=timeout,
            )
            resp_old.raise_for_status()
            obj_old = resp_old.json()
            if int(obj_old.get("status") or 0) != 1:
                return None
            data_old = obj_old.get("data")
            if not data_old:
                return None
            path_old = str(data_old).strip()
            if not path_old.startswith("/"):
                path_old = "/" + path_old
            return path_old
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 * attempt, 8))
                continue
    raise RuntimeError(
        f"generate_path failed province_id={province_id} district_id={district_id} "
        f"bds_method={bds_method}: {last_error}"
    )


def run_slug_test_phase(args: argparse.Namespace, logger: Logger) -> int:
    db = Database()
    session = make_session()
    locations = get_locations(
        db=db,
        province_ids=parse_int_csv(args.province_ids),
        district_ids=parse_int_csv(args.district_ids),
        start_province_id=args.start_province_id,
        start_district_id=args.start_district_id,
    )
    if args.max_districts > 0:
        locations = locations[: args.max_districts]
    if args.district_name_prefix:
        prefix = args.district_name_prefix.strip().lower()
        locations = [x for x in locations if (x.district_name or "").strip().lower().startswith(prefix)]
    if args.slug_test_limit > 0:
        locations = locations[: args.slug_test_limit]

    logger.log(
        f"[SLUG_TEST_START] districts={len(locations)} province_filter={args.province_ids or 'all'} "
        f"name_prefix={args.district_name_prefix or '(none)'}"
    )

    ok = 0
    failed = 0
    for idx, loc in enumerate(locations, start=1):
        try:
            sale_path = generate_path(
                session=session,
                province_id=loc.province_id,
                district_id=loc.district_id,
                district_name=loc.district_name,
                bds_method="mua-ban",
                timeout=args.timeout,
                retries=args.max_retries,
            )
            rent_path = generate_path(
                session=session,
                province_id=loc.province_id,
                district_id=loc.district_id,
                district_name=loc.district_name,
                bds_method="cho-thue",
                timeout=args.timeout,
                retries=args.max_retries,
            )
            logger.log(
                f"[SLUG] {idx}/{len(locations)} province_id={loc.province_id} "
                f"district_id={loc.district_id} district={loc.district_name} "
                f"sale={sale_path} rent={rent_path}"
            )
            ok += 1
        except Exception as exc:
            logger.log(
                f"[SLUG_ERR] {idx}/{len(locations)} province_id={loc.province_id} "
                f"district_id={loc.district_id} district={loc.district_name} error={exc}"
            )
            failed += 1

        if args.delay_min > 0 or args.delay_max > 0:
            sleep_s = random.uniform(args.delay_min, args.delay_max)
            logger.log(f"  [SLEEP] {sleep_s:.2f}s")
            time.sleep(sleep_s)

    logger.log(f"[SLUG_TEST_DONE] ok={ok} failed={failed} total={len(locations)}")
    return 0


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
    start_province_id: int,
    start_district_id: int,
) -> List[LocationRow]:
    where = []
    params: List[object] = []
    if province_ids:
        where.append("province_id IN ({})".format(",".join(["%s"] * len(province_ids))))
        params.extend(province_ids)
    if district_ids:
        where.append("district_id IN ({})".format(",".join(["%s"] * len(district_ids))))
        params.extend(district_ids)
    if start_province_id > 0:
        if start_district_id > 0:
            where.append("(province_id > %s OR (province_id = %s AND district_id >= %s))")
            params.extend([start_province_id, start_province_id, start_district_id])
        else:
            where.append("province_id >= %s")
            params.append(start_province_id)
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


def crawl_trade_pages(
    *,
    db: Database,
    session: requests.Session,
    logger: Logger,
    args: argparse.Namespace,
    base_url: str,
    trade_type: str,
    city_name: str,
) -> Tuple[int, int]:
    dup_consecutive = 0
    no_new_page_streak = 0
    empty_pages = 0
    seen_links = 0
    added_links = 0

    logger.log(f"  [TRADE] type={trade_type} base_url={base_url}")
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
            no_new_page_streak += 1
            if empty_pages >= args.stop_empty_pages:
                logger.log(f"    [STOP_EMPTY] empty_pages={empty_pages}")
                break
            if args.stop_no_new_pages > 0 and no_new_page_streak >= args.stop_no_new_pages:
                logger.log(
                    f"    [STOP_NO_NEW] pages={no_new_page_streak} "
                    f"threshold={args.stop_no_new_pages}"
                )
                break
            continue

        empty_pages = 0
        seen_links += found
        added = db.add_collected_links(
            links,
            domain=DOMAIN,
            loaihinh="Guland",
            trade_type=trade_type,
            city_name=city_name,
        )
        added_links += added
        logger.log(
            f"      [ADD] added={added} dup={found - added} "
            f"dup_consecutive={dup_consecutive}/{args.dup_stop_threshold}"
        )
        if added == 0:
            dup_consecutive += found
            no_new_page_streak += 1
        else:
            dup_consecutive = 0
            no_new_page_streak = 0

        if args.dup_stop_threshold > 0 and dup_consecutive >= args.dup_stop_threshold:
            logger.log(
                f"    [STOP_DUP] reached={dup_consecutive} "
                f"threshold={args.dup_stop_threshold}"
            )
            break
        if args.stop_no_new_pages > 0 and no_new_page_streak >= args.stop_no_new_pages:
            logger.log(
                f"    [STOP_NO_NEW] pages={no_new_page_streak} "
                f"threshold={args.stop_no_new_pages}"
            )
            break

        if args.delay_min > 0 or args.delay_max > 0:
            sleep_s = random.uniform(args.delay_min, args.delay_max)
            logger.log(f"      [SLEEP] {sleep_s:.2f}s")
            time.sleep(sleep_s)

    return seen_links, added_links


def run_link_phase(args: argparse.Namespace, logger: Logger) -> Tuple[int, int]:
    db = Database()
    session = make_session()
    locations = get_locations(
        db=db,
        province_ids=parse_int_csv(args.province_ids),
        district_ids=parse_int_csv(args.district_ids),
        start_province_id=args.start_province_id,
        start_district_id=args.start_district_id,
    )
    if args.max_districts > 0:
        locations = locations[: args.max_districts]

    logger.log(
        f"[LINK_START] districts={len(locations)} page_max={args.max_pages_per_district} "
        f"dup_threshold={args.dup_stop_threshold} province_only_threshold={args.province_only_total_threshold}"
    )

    total_added = 0
    total_seen_links = 0
    # Prevent re-crawling the same listing path returned for many districts
    # in the same province/trade (common when API falls back to province path).
    seen_paths_by_province_trade: Dict[Tuple[int, str], set] = {}

    by_province: Dict[int, List[LocationRow]] = {}
    for loc in locations:
        by_province.setdefault(loc.province_id, []).append(loc)

    province_ids_sorted = sorted(by_province.keys())
    district_idx_global = 0

    for province_idx, province_id in enumerate(province_ids_sorted, start=1):
        province_rows = by_province[province_id]
        province_name = province_rows[0].province_name
        logger.log(
            f"[PROVINCE] {province_idx}/{len(province_ids_sorted)} "
            f"province_id={province_id} province={province_name} districts={len(province_rows)}"
        )

        # Detect if this province should be crawled at province-level only.
        province_sale_path = generate_path(
            session=session,
            province_id=province_id,
            district_id=0,
            district_name="",
            bds_method="mua-ban",
            timeout=args.timeout,
            retries=args.max_retries,
        )
        if not province_sale_path:
            # Fallback to first district path when district_id=0 is unsupported.
            province_sale_path = generate_path(
                session=session,
                province_id=province_id,
                district_id=province_rows[0].district_id,
                district_name=province_rows[0].district_name,
                bds_method="mua-ban",
                timeout=args.timeout,
                retries=args.max_retries,
            )
        province_rent_path = generate_path(
            session=session,
            province_id=province_id,
            district_id=0,
            district_name="",
            bds_method="cho-thue",
            timeout=args.timeout,
            retries=args.max_retries,
        ) or to_rent_path(province_sale_path or "")

        province_only_trade: Dict[str, bool] = {"s": False, "u": False}
        province_zero_trade: Dict[str, bool] = {"s": False, "u": False}
        if province_sale_path:
            for trade_type, p in [("s", province_sale_path), ("u", province_rent_path)]:
                probe_html = fetch_html(
                    session=session,
                    url=urljoin(BASE_URL, p),
                    timeout=args.timeout,
                    retries=args.max_retries,
                )
                total_results = extract_total_results(probe_html or "")
                province_only = (
                    total_results is not None
                    and total_results < args.province_only_total_threshold
                )
                province_only_trade[trade_type] = province_only
                if total_results == 0:
                    province_zero_trade[trade_type] = True
                logger.log(
                    f"  [PROVINCE_CHECK] type={trade_type} total={total_results} "
                    f"threshold={args.province_only_total_threshold} province_only={province_only}"
                )
                if total_results == 0:
                    logger.log(
                        f"  [PROVINCE_SKIP_ZERO] type={trade_type} total=0 -> skip trade"
                    )
                    continue
                if province_only:
                    seen, added = crawl_trade_pages(
                        db=db,
                        session=session,
                        logger=logger,
                        args=args,
                        base_url=urljoin(BASE_URL, p),
                        trade_type=trade_type,
                        city_name=province_name or str(province_id),
                    )
                    total_seen_links += seen
                    total_added += added

        if province_only_trade["s"] and province_only_trade["u"]:
            logger.log(
                f"[PROVINCE_SKIP_DISTRICTS] province_id={province_id} "
                f"reason=province_only_both_trades"
            )
            continue

        for loc in province_rows:
            district_idx_global += 1
            if province_only_trade["s"] and province_only_trade["u"]:
                logger.log(
                    f"[DISTRICT_SKIP_PROVINCE_MODE] {district_idx_global}/{len(locations)} "
                    f"province_id={loc.province_id} district_id={loc.district_id}"
                )
                continue

            logger.log(
                f"[DISTRICT] {district_idx_global}/{len(locations)} province_id={loc.province_id} "
                f"province={loc.province_name} district_id={loc.district_id} district={loc.district_name}"
            )

            sale_path = generate_path(
                session=session,
                province_id=loc.province_id,
                district_id=loc.district_id,
                district_name=loc.district_name,
                bds_method="mua-ban",
                timeout=args.timeout,
                retries=args.max_retries,
            )
            if not sale_path:
                logger.log("  [SKIP_DISTRICT] sale_path_empty")
                continue

            rent_path = generate_path(
                session=session,
                province_id=loc.province_id,
                district_id=loc.district_id,
                district_name=loc.district_name,
                bds_method="cho-thue",
                timeout=args.timeout,
                retries=args.max_retries,
            ) or to_rent_path(sale_path)
            trade_paths = [("s", sale_path), ("u", rent_path)]

            for trade_type, base_path in trade_paths:
                if province_only_trade.get(trade_type):
                    logger.log(
                        f"  [SKIP_DISTRICT_PROVINCE_MODE] type={trade_type} "
                        f"province_id={loc.province_id} district_id={loc.district_id}"
                    )
                    continue
                if province_zero_trade.get(trade_type):
                    logger.log(
                        f"  [SKIP_DISTRICT_ZERO_MODE] type={trade_type} "
                        f"province_id={loc.province_id} district_id={loc.district_id}"
                    )
                    continue

                base_url = urljoin(BASE_URL, base_path)
                dedup_key = (loc.province_id, trade_type)
                already_seen = seen_paths_by_province_trade.setdefault(dedup_key, set())
                if base_path in already_seen:
                    logger.log(
                        f"  [SKIP_DISTRICT_DUP_PATH] type={trade_type} "
                        f"province_id={loc.province_id} district_id={loc.district_id} "
                        f"path={base_path}"
                    )
                    continue
                already_seen.add(base_path)
                seen, added = crawl_trade_pages(
                    db=db,
                    session=session,
                    logger=logger,
                    args=args,
                    base_url=base_url,
                    trade_type=trade_type,
                    city_name=loc.province_name or str(loc.province_id),
                )
                total_seen_links += seen
                total_added += added

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
    ap.add_argument("--mode", choices=["all", "link", "detail", "slug-test"], default="all")
    ap.add_argument("--province-ids", default="", help="Comma list, empty=all from location_guland")
    ap.add_argument("--district-ids", default="", help="Comma list, empty=all from location_guland")
    ap.add_argument("--start-province-id", type=int, default=0, help="Resume from this province_id")
    ap.add_argument(
        "--start-district-id",
        type=int,
        default=0,
        help="Resume from this district_id within start-province-id",
    )
    ap.add_argument("--max-districts", type=int, default=0, help="Limit districts for testing")
    ap.add_argument(
        "--district-name-prefix",
        type=str,
        default="",
        help="Only for slug-test: filter district_name startswith this prefix (e.g. 'Xã ')",
    )
    ap.add_argument(
        "--slug-test-limit",
        type=int,
        default=0,
        help="Only for slug-test: cap number of district rows to print",
    )
    ap.add_argument("--max-pages-per-district", type=int, default=300)
    ap.add_argument("--dup-stop-threshold", type=int, default=200)
    ap.add_argument(
        "--stop-no-new-pages",
        type=int,
        default=6,
        help="Stop current trade when consecutive pages have no new links",
    )
    ap.add_argument(
        "--province-only-total-threshold",
        type=int,
        default=1000,
        help="If province totalResults on page is below this threshold, crawl province only",
    )
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
    lock_fh = None
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        lock_fh = open(LOCK_FILE, "w", encoding="utf-8")
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            logger.log(f"[LOCKED] Another guland_daily_api_job is running. lock={LOCK_FILE}")
            return 0

        lock_fh.write(f"pid={os.getpid()}\n")
        lock_fh.flush()

        logger.log(
            f"[START] mode={args.mode} max_pages={args.max_pages_per_district} "
            f"dup_stop={args.dup_stop_threshold}"
        )
        if args.mode == "slug-test":
            return run_slug_test_phase(args, logger)
        if args.mode in ("all", "link"):
            run_link_phase(args, logger)
        if args.mode in ("all", "detail"):
            rc = run_detail_phase(args, logger)
            if rc != 0:
                return rc
        logger.log("[DONE]")
        return 0
    finally:
        if lock_fh is not None:
            try:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            lock_fh.close()


if __name__ == "__main__":
    raise SystemExit(main())
