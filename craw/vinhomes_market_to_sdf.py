#!/usr/bin/env python3
"""
Import vinhomes_market_thue.json into scraped_details_flat.
"""

import argparse
import json
from pathlib import Path

from database import Database


DOMAIN = "vinhome"
URL_PREFIX = "https://market.vinhomes.vn/thu-cap/"


def pick_images(item: dict) -> list[str]:
    imgs = []
    for entry in item.get("images") or []:
        paths = entry.get("path") if isinstance(entry, dict) else None
        if isinstance(paths, list):
            for p in paths:
                if isinstance(p, str) and p.strip():
                    imgs.append(p.strip())
    # dedup keep order
    seen = set()
    out = []
    for u in imgs:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def exists_url(conn, url: str, domain: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM scraped_details_flat WHERE url = %s AND domain = %s LIMIT 1",
            (url, domain),
        )
        return bool(cur.fetchone())
    finally:
        cur.close()

def get_detail_id(conn, url: str, domain: str):
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM scraped_details_flat WHERE url = %s AND domain = %s LIMIT 1",
            (url, domain),
        )
        row = cur.fetchone()
        return row["id"] if row else None
    finally:
        cur.close()

def main() -> int:
    ap = argparse.ArgumentParser(description="Import Vinhomes market JSON to scraped_details_flat")
    ap.add_argument(
        "--input",
        default="/home/chungnt/crawlvip/craw/logs/vinhomes_market_thue.json",
    )
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--update-existing", action="store_true", help="Update existing rows by url")
    args = ap.parse_args()

    path = Path(args.input)
    obj = json.loads(path.read_text(encoding="utf-8"))
    items = obj.get("items") or obj.get("data") or []
    if args.limit and args.limit > 0:
        items = items[: args.limit]

    db = Database()
    conn = db.get_connection()
    inserted = 0
    skipped = 0
    try:
        for idx, item in enumerate(items, start=1):
            slug = (item.get("slug") or "").strip()
            if not slug:
                print(f"[SKIP] idx={idx} missing_slug", flush=True)
                skipped += 1
                continue
            url = f"{URL_PREFIX}{slug}"
            existing_id = get_detail_id(conn, url, DOMAIN)
            if existing_id and not args.update_existing:
                print(f"[SKIP] idx={idx} exists url={url}", flush=True)
                skipped += 1
                continue

            data = {
                "title": item.get("name") or item.get("houseName"),
                "mota": item.get("info"),
                "khoanggia": item.get("bestPrice"),
                "dientich": item.get("netArea"),
                "sophongngu": item.get("numberBedroom"),
                "sophongvesinh": item.get("numberBathroom"),
                "thuocduan": item.get("projectName"),
                "matin": item.get("id"),
                "diachi": item.get("projectAddress"),
                "lat": item.get("lat"),
                "lng": item.get("lng"),
            }

            img_list = pick_images(item)
            if img_list:
                data["img"] = img_list

            if args.dry_run:
                print(f"[DRY] idx={idx} url={url}", flush=True)
                inserted += 1
                continue

            if existing_id:
                cur = conn.cursor()
                try:
                    cur.execute(
                        """
                        UPDATE scraped_details_flat
                        SET title=%s,
                            mota=%s,
                            khoanggia=%s,
                            dientich=%s,
                            sophongngu=%s,
                            sophongvesinh=%s,
                            thuocduan=%s,
                            matin=%s,
                            diachi=%s,
                            lat=%s,
                            lng=%s
                        WHERE id=%s
                        """,
                        (
                            data.get("title"),
                            data.get("mota"),
                            data.get("khoanggia"),
                            data.get("dientich"),
                            data.get("sophongngu"),
                            data.get("sophongvesinh"),
                            data.get("thuocduan"),
                            data.get("matin"),
                            data.get("diachi"),
                            data.get("lat"),
                            data.get("lng"),
                            existing_id,
                        ),
                    )
                    # Keep existing images to avoid re-upload/reset status
                    conn.commit()
                finally:
                    cur.close()
                print(f"[UPDATE] idx={idx} detail_id={existing_id} url={url}", flush=True)
                inserted += 1
            else:
                detail_id = db.add_scraped_detail_flat(
                    url=url,
                    data=data,
                    domain=DOMAIN,
                    loaihinh=None,
                    trade_type="u",
                )
                if detail_id and img_list:
                    db.add_detail_images(detail_id, img_list)
                print(f"[SAVE] idx={idx} detail_id={detail_id} url={url}", flush=True)
                inserted += 1
    finally:
        conn.close()

    print(f"[DONE] inserted={inserted} skipped={skipped} dry_run={args.dry_run}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
