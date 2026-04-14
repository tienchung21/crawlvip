#!/usr/bin/env python3
"""
Enrich vinhomes_market_thue.json with lat/lng parsed from Google Maps iframe in listing HTML.
"""

import argparse
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright


BASE_LEASING_URL = "https://market.vinhomes.vn/leasing/estate-for-rent/{slug}"
BASE_THU_CAP_URL = "https://market.vinhomes.vn/thu-cap/{slug}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_leasing_url(slug: str, checkin: str, checkout: str) -> str:
    return f"{BASE_LEASING_URL.format(slug=slug)}?checkInDate={checkin}&checkOutDate={checkout}&SourceFunds=1"


def fetch_iframe_src(page, url: str, timeout_ms: int, max_retries: int) -> str | None:
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            iframe = page.locator("div.location-map.nearby-map-container iframe.nearby-map-embed")
            if iframe.count() == 0:
                iframe = page.locator("iframe.nearby-map-embed")
            if iframe.count() == 0:
                return None
            return iframe.first.get_attribute("src")
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(min(2 * attempt, 8))
    raise RuntimeError(f"fetch_iframe_failed url={url}: {last_error}")


def parse_latlng_from_src(src: str | None) -> tuple[str | None, str | None]:
    # Matches origin=lat,lng or destination=lat,lng in Google Maps embed URL
    if not src:
        return None, None
    match = re.search(r"(?:origin|destination)=([0-9.\-]+),([0-9.\-]+)", src)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def save_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Enrich vinhomes market JSON with lat/lng")
    ap.add_argument(
        "--input",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_market_thue.json",
    )
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--limit-items", type=int, default=0)
    args = ap.parse_args()

    path = Path(args.input)
    obj = json.loads(path.read_text(encoding="utf-8"))
    items = obj.get("items") or obj.get("data") or []
    if args.limit_items and args.limit_items > 0:
        items = items[: args.limit_items]

    today = datetime.now().date()
    checkin = today.strftime("%Y-%m-%d")
    checkout = (today + timedelta(days=30)).strftime("%Y-%m-%d")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        for idx, item in enumerate(items, start=1):
            if item.get("lat") is not None and item.get("lng") is not None:
                print(f"[SKIP] idx={idx} already_has_latlng", flush=True)
                continue
            slug = (item.get("slug") or "").strip()
            if not slug:
                item["latlng_error"] = "missing_slug"
                item["latlng_fetched_at"] = utc_now_iso()
                save_json(path, obj)
                print(f"[SKIP] idx={idx} missing_slug", flush=True)
                continue

            leasing_url = build_leasing_url(slug, checkin, checkout)
            src = fetch_iframe_src(page, leasing_url, args.timeout * 1000, args.max_retries)
            lat, lng = parse_latlng_from_src(src)
            source_url = leasing_url

            if not lat or not lng:
                fallback_url = BASE_THU_CAP_URL.format(slug=slug)
                src2 = fetch_iframe_src(page, fallback_url, args.timeout * 1000, args.max_retries)
                lat, lng = parse_latlng_from_src(src2)
                source_url = fallback_url

            if lat and lng:
                item["lat"] = lat
                item["lng"] = lng
                item.pop("latlng_error", None)
                item["latlng_source_url"] = source_url
                item["latlng_fetched_at"] = utc_now_iso()
                print(f"[DONE] idx={idx} slug={slug} latlng={lat},{lng}", flush=True)
            else:
                item["latlng_error"] = "not_found"
                item["latlng_source_url"] = source_url
                item["latlng_fetched_at"] = utc_now_iso()
                print(f"[MISS] idx={idx} slug={slug}", flush=True)

            save_json(path, obj)
            if idx < len(items) and args.delay > 0:
                time.sleep(args.delay)
        browser.close()

    obj["latlng_enriched_at"] = utc_now_iso()
    save_json(path, obj)
    print(f"[SAVED] file={path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
