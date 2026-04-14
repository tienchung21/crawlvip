#!/usr/bin/env python3
"""
Update images for existing vinhomes secondary rows in scraped_details_flat.
"""

import argparse
import json
from pathlib import Path

from database import Database


DOMAIN = "vinhome"


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


def main() -> int:
    ap = argparse.ArgumentParser(description="Update vinhomes secondary images to last photo")
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
    updated = 0
    skipped = 0
    try:
        cur = conn.cursor()
        for idx, item in enumerate(items, start=1):
            url = (item.get("url") or "").strip()
            if not url:
                skipped += 1
                continue
            img_urls = pick_photo_urls(item)
            if not img_urls:
                skipped += 1
                continue
            cur.execute(
                "SELECT id FROM scraped_details_flat WHERE url = %s AND domain = %s LIMIT 1",
                (url, DOMAIN),
            )
            row = cur.fetchone()
            if not row:
                skipped += 1
                continue
            detail_id = row["id"] if isinstance(row, dict) else row[0]
            if args.dry_run:
                print(f"[DRY] idx={idx} detail_id={detail_id} url={url}", flush=True)
                updated += 1
                continue
            cur.execute("DELETE FROM scraped_detail_images WHERE detail_id = %s", (detail_id,))
            for photo_idx, img_url in enumerate(img_urls):
                cur.execute(
                    "INSERT IGNORE INTO scraped_detail_images (detail_id, image_url, idx, status) VALUES (%s, %s, %s, 'PENDING')",
                    (detail_id, img_url, photo_idx),
                )
            cur.execute(
                "UPDATE scraped_details_flat SET img_count = %s WHERE id = %s",
                (len(img_urls), detail_id),
            )
            conn.commit()
            updated += 1
            if idx % 50 == 0:
                print(f"[PROGRESS] {idx}/{len(items)} updated={updated} skipped={skipped}", flush=True)
    finally:
        conn.close()

    print(f"[DONE] updated={updated} skipped={skipped} dry_run={args.dry_run}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
