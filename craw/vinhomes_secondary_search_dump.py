#!/usr/bin/env python3
"""
Dump du lieu Vinhomes secondary search ra 1 file JSON.
"""

import argparse
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests


API_URL = "https://market.vinhomes.vn/api/v1/properties/search"
DEFAULT_COOKIE = (
    "_gcl_au=1.1.405940912.1775616443; "
    "vap_anonymous_id=19d6afcc61e9c2-01d7b9ea532448-4c657b58-1fa400-19d6afcc61e9c2; "
    "_ga=GA1.1.936921103.1775616444; "
    "_fbp=fb.1.1775616444541.732675261380206987; "
    "hubspotutk=ce7dab78f19a1faf7d76e08e578fb7c3; "
    "__hssrc=1; "
    "_clck=106vdbx%5E2%5Eg51%5E0%5E2289; "
    "i18next=vi; "
    "__hstc=20637479.ce7dab78f19a1faf7d76e08e578fb7c3.1775616445545.1775616445545.1775628603793.2; "
    "_ga_BTFWGQMXKD=GS2.1.s1775628620$o2$g1$t1775628620$j60$l0$h0; "
    "_cfuvid=va0p6mD2C5Tif9kzD0WDFmic0UCcctg_d4ij1mUlQiA-1775628699367-0.0.1.1-604800000; "
    "__hssc=20637479.3.1775628603793; "
    "__cf_bm=1vrpOz8ez4umVPFTW_Qo.NLhtldd7ncJ09jV__Mgfwc-1775629648-1.0.1.1-U2yDLuYlfkKlas3gNy35dpjMP2VdtdiHx8O.GKuoy6xAzq5bRP152uBsQWVSJWneBix9HOwvrdovzUZ4oIV2Vk6v4xAIQV.qDSf0kRPzv04; "
    "_clsk=1ckc9mm%5E1775629848124%5E8%5E1%5Ea.clarity.ms%2Fcollect; "
    "_ga_6G502HZNT0=GS2.1.s1775628600$o2$g1$t1775629848$j60$l0$h0"
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_headers(cookie: str, page_size: int) -> dict:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "vi",
        "content-type": "application/json",
        "origin": "https://market.vinhomes.vn",
        "referer": f"https://market.vinhomes.vn/thu-cap?size={page_size}",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
        ),
        "cookie": cookie,
    }


def build_payload(page: int, page_size: int) -> dict:
    return {
        "page": page,
        "size": page_size,
        "location": "",
        "service_type": 2,
        "search_type": "secondary",
        "business_type": 2,
        "area": {},
        "balcony_directions": [],
        "bathrooms": [],
        "bedrooms": [],
        "delivery_type_groups": [],
        "directions": [],
        "height_types": [],
        "house_styles": [],
        "map_bound": {
            "top_left": [94.84991859253489, 30.317501398500468],
            "bottom_right": [120.36460255140088, 5.148482912652],
        },
        "map_zoom": 5.0997410505923755,
        "no_list_photo": False,
        "order_by": 0,
        "order_by_label": "",
        "price": {},
        "project_alias": [],
        "project_area_clusters": [],
        "project_towers": [],
        "property_types": [],
        "view_mode": 1,
    }


def fetch_page(page: int, page_size: int, timeout: int, max_retries: int, cookie: str) -> dict:
    headers = build_headers(cookie, page_size)
    payload = build_payload(page, page_size)
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, impersonate="chrome124", timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(min(2 * attempt, 10))
    raise RuntimeError(f"fetch_page_failed page={page}: {last_error}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump Vinhomes secondary search API ra 1 file JSON")
    ap.add_argument("--page-size", type=int, default=16)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--cookie", default=DEFAULT_COOKIE)
    ap.add_argument(
        "--output",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_secondary_search_thue.json",
    )
    ap.add_argument(
        "--checkpoint",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_secondary_search_thue.checkpoint.json",
    )
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path = Path(args.checkpoint)

    items = []
    total = 0
    total_pages = 0
    start_page = 1
    if args.resume and checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        items = checkpoint.get("items") or []
        total = int(checkpoint.get("total") or 0)
        total_pages = int(checkpoint.get("total_pages") or 0)
        start_page = int(checkpoint.get("next_page") or 1)
    if start_page == 1:
        first = fetch_page(1, args.page_size, args.timeout, args.max_retries, args.cookie)
        first_data = first.get("data", {})
        total = int(first_data.get("total") or 0)
        total_pages = math.ceil(total / args.page_size) if total else 0
        items = list(first_data.get("data") or [])
        start_page = 2
        print(f"[PAGE] page=1/{total_pages} items={len(first_data.get('data') or [])} total={total}", flush=True)
        checkpoint_path.write_text(json.dumps({
            "updated_at": utc_now_iso(),
            "total": total,
            "total_pages": total_pages,
            "next_page": start_page,
            "items": items,
        }, ensure_ascii=False), encoding="utf-8")

    for page in range(start_page, total_pages + 1):
        obj = fetch_page(page, args.page_size, args.timeout, args.max_retries, args.cookie)
        page_items = obj.get("data", {}).get("data") or []
        items.extend(page_items)
        print(f"[PAGE] page={page}/{total_pages} items={len(page_items)}", flush=True)
        checkpoint_path.write_text(json.dumps({
            "updated_at": utc_now_iso(),
            "total": total,
            "total_pages": total_pages,
            "next_page": page + 1,
            "items": items,
        }, ensure_ascii=False), encoding="utf-8")
        if page < total_pages and args.delay > 0:
            time.sleep(args.delay)

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
        "api_url": API_URL,
        "page_size": args.page_size,
        "total": total,
        "total_pages": total_pages,
        "items_count_raw": len(items),
        "items_count_dedup": len(dedup_items),
        "items": dedup_items,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    print(f"[DONE] saved={len(dedup_items)} file={out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
