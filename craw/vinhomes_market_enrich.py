#!/usr/bin/env python3
"""
Enrich vinhomes_market_thue.json bang API detail theo slug.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests
from curl_cffi.requests import exceptions as req_exc


DETAIL_URL = "https://apigw.vinhomes.vn/leasing/v1/market/get-detail-ls-market/{slug}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def fetch_detail(slug: str, timeout: int, max_retries: int) -> dict:
    last_error = None
    url = DETAIL_URL.format(slug=slug)
    headers = {
        "Accept": "application/json",
        "Accept-Language": "vi",
        "Origin": "https://market.vinhomes.vn",
        "Referer": "https://market.vinhomes.vn/",
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, impersonate="chrome124", timeout=timeout, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except req_exc.HTTPError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code and 400 <= int(status_code) < 500:
                return {"_http_error": int(status_code)}
            last_error = exc
            if attempt < max_retries:
                time.sleep(min(2 * attempt, 8))
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(min(2 * attempt, 8))
    raise RuntimeError(f"fetch_detail_failed slug={slug}: {last_error}")


def save_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Enrich vinhomes_market_thue.json by detail API")
    ap.add_argument(
        "--input",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_market_thue.json",
    )
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--force", action="store_true", help="Refetch detail even if already enriched")
    args = ap.parse_args()

    path = Path(args.input)
    obj = json.loads(path.read_text(encoding="utf-8"))
    items = obj.get("items") or []

    for idx, item in enumerate(items, start=1):
        slug = (item.get("slug") or "").strip()
        if not slug:
            print(f"[SKIP] idx={idx} missing_slug", flush=True)
            continue
        if item.get("detail_fetched_at") and not args.force:
            print(f"[SKIP] idx={idx}/{len(items)} slug={slug} already_enriched", flush=True)
            continue
        result = fetch_detail(slug, args.timeout, args.max_retries)
        if result.get("_http_error"):
            item["detail_error"] = f"http_{result['_http_error']}"
            item["detail_fetched_at"] = utc_now_iso()
            print(
                f"[MISS] idx={idx}/{len(items)} slug={slug} status={result['_http_error']}",
                flush=True,
            )
            obj["detail_enriched_at"] = utc_now_iso()
            obj["detail_source_api"] = DETAIL_URL
            save_json(path, obj)
            if idx < len(items) and args.delay > 0:
                time.sleep(args.delay)
            continue
        try:
            detail = result.get("data") or {}
        except Exception as exc:
            item["detail_error"] = f"parse_error:{exc}"
            item["detail_fetched_at"] = utc_now_iso()
            print(f"[ERROR] idx={idx}/{len(items)} slug={slug} parse_error={exc}", flush=True)
            obj["detail_enriched_at"] = utc_now_iso()
            obj["detail_source_api"] = DETAIL_URL
            save_json(path, obj)
            if idx < len(items) and args.delay > 0:
                time.sleep(args.delay)
            continue
        for key in [
            "name",
            "projectName",
            "projectAddress",
            "projectCode",
            "info",
            "projectLinkLocation",
            "projectInfo",
            "projectNearbyLocation",
        ]:
            if key in detail:
                item[key] = detail.get(key)
        item["detail_fetched_at"] = utc_now_iso()
        item.pop("detail_error", None)
        print(f"[DONE] idx={idx}/{len(items)} slug={slug}", flush=True)
        obj["detail_enriched_at"] = utc_now_iso()
        obj["detail_source_api"] = DETAIL_URL
        save_json(path, obj)
        if idx < len(items) and args.delay > 0:
            time.sleep(args.delay)

    obj["detail_enriched_at"] = utc_now_iso()
    obj["detail_source_api"] = DETAIL_URL
    save_json(path, obj)
    print(f"[SAVED] file={path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
