#!/usr/bin/env python3
"""
Dump Vinhomes market API ra file JSON.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlencode

from curl_cffi import requests


BASE_URL = "https://apigw.vinhomes.vn/leasing/v1/market/get-list-ls-market-filter"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_url(page: int, page_size: int) -> str:
    params = {
        "SourceFunds": 1,
        "PageNumber": page,
        "OrderBy": "Price",
        "OrderByDirection": "asc",
        "IsPaged": "true",
        "PageSize": page_size,
        "Lang": "VN",
        "locale": "vi",
    }
    return f"{BASE_URL}?{urlencode(params)}"


def fetch_page(page: int, page_size: int, timeout: int, max_retries: int) -> Dict:
    last_error = None
    url = build_url(page, page_size)
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, impersonate="chrome124", timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            if attempt == max_retries:
                break
    raise RuntimeError(f"fetch_page_failed page={page}: {last_error}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump Vinhomes market API to JSON")
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument(
        "--output",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_market_thue.json",
    )
    args = ap.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    first = fetch_page(1, args.page_size, args.timeout, args.max_retries)
    data = first.get("data", {})
    total_pages = int(data.get("totalPages") or 0)
    total_count = int(data.get("totalCount") or 0)
    items: List[Dict] = list(data.get("items") or [])

    print(f"[PAGE] page=1/{total_pages} items={len(data.get('items') or [])}", flush=True)

    for page in range(2, total_pages + 1):
        obj = fetch_page(page, args.page_size, args.timeout, args.max_retries)
        page_items = obj.get("data", {}).get("items") or []
        items.extend(page_items)
        print(f"[PAGE] page={page}/{total_pages} items={len(page_items)}", flush=True)

    seen = set()
    dedup_items = []
    for item in items:
        item_id = item.get("id")
        if item_id in seen:
            continue
        seen.add(item_id)
        dedup_items.append(item)

    payload = {
        "fetched_at": utc_now_iso(),
        "source_url": build_url(1, args.page_size),
        "total_count": total_count,
        "total_pages": total_pages,
        "page_size": args.page_size,
        "items_count_raw": len(items),
        "items_count_dedup": len(dedup_items),
        "items": dedup_items,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] saved={len(dedup_items)} file={out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
