#!/usr/bin/env python3
"""
Enrich file vinhomes_secondary_search_thue.json bang HTML chi tiet cua tung URL.
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi import requests
from curl_cffi.requests import exceptions as req_exc


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def save_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_html(url: str, timeout: int, max_retries: int) -> str | None:
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, impersonate="chrome124", timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except req_exc.HTTPError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code and 400 <= int(status_code) < 500:
                return None
            last_error = exc
        except Exception as exc:
            last_error = exc
        if attempt < max_retries:
            time.sleep(min(2 * attempt, 8))
    raise RuntimeError(f"fetch_html_failed url={url}: {last_error}")


def parse_page(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    location_span = soup.select_one('span[data-scrollnav-target="section-location-around"]')
    overview_div = soup.select_one("div.block.section-overview-summary")
    center_match = re.search(r"center=([0-9.\-]+),([0-9.\-]+)", html)

    overview_map = {}
    if overview_div:
        for row in overview_div.select("div.single-line"):
            label_el = row.select_one("span.single-line-item.is-label")
            value_el = row.select_one("span.single-line-item.is-value")
            label = label_el.get_text(" ", strip=True) if label_el else ""
            value = value_el.get_text(" ", strip=True) if value_el else ""
            if label:
                overview_map[label] = value

    return {
        "location_span_html": str(location_span) if location_span else None,
        "location_text": location_span.get_text(" ", strip=True) if location_span else None,
        "overview_summary_html": str(overview_div) if overview_div else None,
        "overview_summary_map": overview_map,
        "lat": center_match.group(1) if center_match else None,
        "lng": center_match.group(2) if center_match else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Enrich vinhomes secondary search JSON with detail HTML")
    ap.add_argument(
        "--input",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_secondary_search_thue.json",
    )
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--limit-items", type=int, default=0)
    args = ap.parse_args()

    path = Path(args.input)
    obj = json.loads(path.read_text(encoding="utf-8"))
    items = obj.get("items") or []
    target_count = min(len(items), args.limit_items) if args.limit_items > 0 else len(items)

    for idx, item in enumerate(items[:target_count], start=1):
        url = (item.get("url") or "").strip()
        if not url:
            item["detail_html_error"] = "missing_url"
            item["detail_html_fetched_at"] = utc_now_iso()
            print(f"[SKIP] idx={idx}/{target_count} missing_url", flush=True)
            save_json(path, obj)
            continue
        if (
            item.get("detail_html_fetched_at")
            and item.get("location_span_html")
            and item.get("overview_summary_html")
            and item.get("lat") is not None
            and item.get("lng") is not None
        ):
            print(f"[SKIP] idx={idx}/{target_count} already_enriched url={url}", flush=True)
            continue

        html = fetch_html(url, args.timeout, args.max_retries)
        if html is None:
            item["detail_html_error"] = "http_4xx"
            item["detail_html_fetched_at"] = utc_now_iso()
            print(f"[MISS] idx={idx}/{target_count} url={url}", flush=True)
            save_json(path, obj)
            if idx < target_count and args.delay > 0:
                time.sleep(args.delay)
            continue

        parsed = parse_page(html)
        item["location_span_html"] = parsed["location_span_html"]
        item["location_text"] = parsed["location_text"]
        item["overview_summary_html"] = parsed["overview_summary_html"]
        item["overview_summary_map"] = parsed["overview_summary_map"]
        item["lat"] = parsed["lat"]
        item["lng"] = parsed["lng"]
        item["detail_html_fetched_at"] = utc_now_iso()
        item.pop("detail_html_error", None)
        print(f"[DONE] idx={idx}/{target_count} url={url}", flush=True)
        save_json(path, obj)

        if idx < target_count and args.delay > 0:
            time.sleep(args.delay)

    obj["detail_html_enriched_at"] = utc_now_iso()
    save_json(path, obj)
    print(f"[SAVED] file={path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
