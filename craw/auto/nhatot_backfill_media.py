import time
import argparse
from typing import Dict, Optional, Set

from daily_nhatot_api import (
    BASE_URL,
    BATCH_SIZE,
    DELAY_SECONDS,
    MEDIA_FIELDS,
    RECURSIVE_THRESHOLD,
    STOP_ON_DUP,
    AD_SKIP_FIELDS_LIGHT,
    _ad_fetch_ads,
    _ad_fetch_existing_ids,
    _sync_scraped_detail_images,
    _ad_upsert_ads_update_existing,
    _fetch_regions,
    _fetch_wards_by_region,
    _get_db_count_scope,
    get_connection,
)
from urllib.parse import urlencode


def _parse_skip_regions(raw: str) -> Set[str]:
    if not raw:
        return set()
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def crawl_scope_media(conn, region_id, area_id, ward_id, rname, aname, wname):
    scope_label = f"{rname}"
    if aname:
        scope_label += f" - {aname}"
    if wname:
        scope_label += f" - {wname}"

    params = {
        "cg": 1000,
        "limit": 1,
        "o": 0,
        "region_v2": region_id,
        "key_param_included": "true",
        "include_expired_ads": "true",
    }
    if area_id:
        params["area_v2"] = area_id
    if ward_id:
        params["ward"] = ward_id

    check_url = f"{BASE_URL}?{urlencode(params)}"
    _, total_api, status, _ = _ad_fetch_ads(check_url)
    if status != 200:
        print(f"[{scope_label}] Failed to check total. Status {status}")
        return False
    if total_api is None:
        total_api = 0

    if ward_id is None and total_api > RECURSIVE_THRESHOLD:
        print(f"[{scope_label}] Total {total_api} > {RECURSIVE_THRESHOLD}. Drilling down...")
        return "DRILL_DOWN"

    db_count = _get_db_count_scope(conn, region_id, area_id, ward_id)
    print(f"[{scope_label}] API Total: {total_api}, DB Count: {db_count}")

    limit = 50
    offset = 0
    dup_streak = 0
    total_fetched = 0
    total_new = 0
    total_dup_seen = 0
    total_media_write = 0

    print(f"  -> Media backfill {scope_label}...")
    while True:
        params["limit"] = limit
        params["o"] = offset
        crawl_url = f"{BASE_URL}?{urlencode(params)}"

        ads, _, status, _ = _ad_fetch_ads(crawl_url)
        if status != 200 or not ads:
            break

        ad_ids = [ad.get("ad_id") for ad in ads if ad.get("ad_id") is not None]
        existing = _ad_fetch_existing_ids(conn, "ad_listing_detail", ad_ids)

        affected = _ad_upsert_ads_update_existing(
            conn,
            "ad_listing_detail",
            ads,
            "nhatot_media_backfill",
            offset,
            batch_size=BATCH_SIZE,
            skip_fields=AD_SKIP_FIELDS_LIGHT,
            update_fields=MEDIA_FIELDS,
        )
        images_inserted = _sync_scraped_detail_images(conn, ads)

        batch_new = 0
        batch_dup = 0
        for aid in ad_ids:
            if aid in existing:
                dup_streak += 1
                batch_dup += 1
            else:
                dup_streak = 0
                batch_new += 1

        total_fetched += len(ads)
        total_new += batch_new
        total_dup_seen += batch_dup
        total_media_write += affected

        print(
            f"    offset={offset} fetched={len(ads)} new_candidates={batch_new} "
            f"dup_candidates={batch_dup} affected={affected} images_synced={images_inserted}"
        )

        if dup_streak >= STOP_ON_DUP:
            print(f"    -> Stopped {scope_label} due to {dup_streak} consecutive duplicates.")
            break

        offset += limit
        time.sleep(DELAY_SECONDS)

        if offset > 10000:
            print("    -> Reached safety offset limit 10000.")
            break

    return {
        "fetched": total_fetched,
        "new_candidates": total_new,
        "dup_candidates": total_dup_seen,
        "affected": total_media_write,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-regions",
        default="",
        help="Comma-separated region names or region_id values to skip, e.g. 'Tp Hồ Chí Minh,13000'",
    )
    args = parser.parse_args()
    skip_regions = _parse_skip_regions(args.skip_regions)

    conn = get_connection()
    try:
        regions = _fetch_regions(conn)
        print(f"Found {len(regions)} regions.")
        if skip_regions:
            print(f"Skip regions: {sorted(skip_regions)}")
        g_fetched = 0
        g_new = 0
        g_dup = 0
        g_affected = 0

        for reg in regions:
            rid = reg["region_id"]
            rname = reg["name"]
            if str(rid).lower() in skip_regions or str(rname).strip().lower() in skip_regions:
                print(f"[SKIP] {rname} ({rid})")
                continue
            res_reg = crawl_scope_media(conn, rid, None, None, rname, None, None)
            if res_reg == "DRILL_DOWN":
                wards = _fetch_wards_by_region(conn, rid)
                print(f"  -> Direct ward drill for {rname}: {len(wards)} wards (skip level-2)")
                for ward in wards:
                    wid = ward["ward_id"]
                    wname = ward["name"]
                    aid = ward.get("area_id")
                    stats = crawl_scope_media(conn, rid, aid, wid, rname, f"area_{aid}" if aid else None, wname)
                    if isinstance(stats, dict):
                        g_fetched += stats.get("fetched", 0)
                        g_new += stats.get("new_candidates", 0)
                        g_dup += stats.get("dup_candidates", 0)
                        g_affected += stats.get("affected", 0)
            elif isinstance(res_reg, dict):
                g_fetched += res_reg.get("fetched", 0)
                g_new += res_reg.get("new_candidates", 0)
                g_dup += res_reg.get("dup_candidates", 0)
                g_affected += res_reg.get("affected", 0)

        print("=== SUMMARY ===")
        print(f"Total Fetched: {g_fetched}")
        print(f"New Candidates: {g_new}")
        print(f"Duplicate Candidates: {g_dup}")
        print(f"Rows Affected: {g_affected}")
    finally:
        conn.close()


if __name__ == "__main__":
    print("=== Starting Nhatot Media Backfill ===")
    main()
    print("=== Finished ===")
