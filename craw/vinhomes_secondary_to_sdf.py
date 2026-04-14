#!/usr/bin/env python3
"""
Import vinhomes_secondary_search_thue.json into scraped_details_flat.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

from database import Database


DOMAIN = "vinhome"


def overview_map_to_text(overview_map: dict) -> str | None:
    if not isinstance(overview_map, dict) or not overview_map:
        return None
    lines = []
    for key, value in overview_map.items():
        key = str(key).strip()
        value = str(value).strip()
        if not key and not value:
            continue
        if key:
            lines.append(f"{key}: {value}".strip())
        else:
            lines.append(value)
    return "\n".join(lines) if lines else None


def pick_best_photo_url(photo: dict) -> str | None:
    if not isinstance(photo, dict):
        return None
    for key in ["w1200h800", "w900h600", "w750h500", "w450h300", "w225h150", "large", "medium", "small"]:
        val = photo.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def pick_photo_urls(item: dict) -> list[str]:
    photos = item.get("list_photo")
    if not isinstance(photos, list) or not photos:
        return []
    out = []
    seen = set()
    for photo in photos:
        url = pick_best_photo_url(photo)
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def exists_url(conn, url: str, domain: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM scraped_details_flat WHERE url = %s AND domain = %s LIMIT 1",
            (url, domain),
        )
        row = cur.fetchone()
        return bool(row)
    finally:
        cur.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Import Vinhomes secondary search JSON to scraped_details_flat")
    ap.add_argument(
        "--input",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_secondary_search_thue.json",
    )
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    path = Path(args.input)
    obj = json.loads(path.read_text(encoding="utf-8"))
    items = obj.get("items") or []
    if args.limit and args.limit > 0:
        items = items[: args.limit]

    db = Database()
    conn = db.get_connection()
    inserted = 0
    skipped = 0
    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        for idx, item in enumerate(items, start=1):
            url = (item.get("url") or "").strip()
            if not url:
                print(f"[SKIP] idx={idx} missing_url", flush=True)
                skipped += 1
                continue
            if exists_url(conn, url, DOMAIN):
                print(f"[SKIP] idx={idx} exists url={url}", flush=True)
                skipped += 1
                continue

            mota = overview_map_to_text(item.get("overview_summary_map"))
            img_urls = pick_photo_urls(item)

            data = {
                "title": item.get("title"),
                "mota": mota,
                "khoanggia": item.get("price_origin"),
                "dientich": item.get("area"),
                "huongnha": item.get("balcony_direction"),
                "thuocduan": item.get("project_name"),
                "matin": item.get("id"),
                "diachi": item.get("location_text"),
                "lat": item.get("lat"),
                "lng": item.get("lng"),
                "ngaydang": today_str,
            }

            if img_urls:
                data["img"] = img_urls

            if args.dry_run:
                print(f"[DRY] idx={idx} url={url}", flush=True)
                inserted += 1
                continue

            detail_id = db.add_scraped_detail_flat(
                url=url,
                data=data,
                domain=DOMAIN,
                loaihinh=item.get("property_type"),
                trade_type="s",
            )
            if detail_id and img_urls:
                db.add_detail_images(detail_id, img_urls)
            print(f"[SAVE] idx={idx} detail_id={detail_id} url={url}", flush=True)
            inserted += 1
    finally:
        conn.close()

    print(f"[DONE] inserted={inserted} skipped={skipped} dry_run={args.dry_run}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
