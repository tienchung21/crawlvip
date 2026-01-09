import argparse
import json
import random
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Allow importing craw/database.py when running from repo root
ROOT_DIR = Path(__file__).resolve().parent
CRAW_DIR = ROOT_DIR / "craw"
if str(CRAW_DIR) not in sys.path:
    sys.path.insert(0, str(CRAW_DIR))

from database import Database  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


API_URL = "https://gateway.chotot.com/api-uni-mf/public/locations/legacy_wards/{area_id}?is_sort=true"
HEADERS = {
    "accept": "application/json;version=1",
    "origin": "https://www.nhatot.com",
    "referer": "https://www.nhatot.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}


def fetch_wards(area_id: int) -> Tuple[Optional[List[Dict[str, Any]]], int, Optional[Dict[str, Any]]]:
    url = API_URL.format(area_id=area_id)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
    except Exception as exc:
        print(f"[ERROR] area_id={area_id} request failed: {exc}")
        return None, 0, None

    status = resp.status_code
    if status != 200:
        print(f"[WARN] area_id={area_id} HTTP {status}")
        return None, status, None

    try:
        data = resp.json()
    except Exception as exc:
        print(f"[ERROR] area_id={area_id} invalid JSON: {exc}")
        return None, status, None

    wards = extract_wards(data)
    return wards, status, data


def extract_wards(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in ("data", "result", "wards", "items"):
        val = data.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            for subkey in ("wards", "items", "data", "result"):
                sub = val.get(subkey)
                if isinstance(sub, list):
                    return sub
    return []


def get_districts_missing_wards(db: Database, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = db.get_connection(use_database=True)
    try:
        cur = conn.cursor()
        sql = """
            SELECT region_id, area_id, name
            FROM location_detail
            WHERE level = 2
              AND area_id IS NOT NULL
              AND area_id NOT IN (
                SELECT DISTINCT area_id
                FROM location_detail
                WHERE level = 3 AND area_id IS NOT NULL
              )
            ORDER BY area_id ASC
        """
        if limit:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        results = []
        for row in rows:
            if isinstance(row, tuple):
                results.append({"region_id": row[0], "area_id": row[1], "name": row[2]})
            else:
                results.append(row)
        return results
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def upsert_wards(
    db: Database,
    region_id: int,
    area_id: int,
    wards: List[Dict[str, Any]],
    source: str = "legacy_wards",
    batch_size: int = 500,
) -> int:
    rows = []
    for ward in wards:
        ward_id = ward.get("ward_id") or ward.get("id")
        if ward_id is None:
            continue
        ward_name = ward.get("ward_name") or ward.get("name") or ""
        name_url = ward.get("name_url") or ward.get("slug")
        unit_type = ward.get("unit_type") or ward.get("type")
        rows.append((
            region_id,
            area_id,
            int(ward_id),
            3,
            ward_name,
            name_url,
            unit_type,
            source,
            json.dumps(ward, ensure_ascii=False),
        ))

    if not rows:
        return 0

    sql = """
    INSERT INTO location_detail
      (region_id, area_id, ward_id, level, name, name_url, unit_type, source, raw_payload)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      level = VALUES(level),
      name = VALUES(name),
      name_url = VALUES(name_url),
      unit_type = VALUES(unit_type),
      source = VALUES(source),
      raw_payload = VALUES(raw_payload),
      updated_at = CURRENT_TIMESTAMP
    """

    total = 0
    conn = db.get_connection(use_database=True)
    try:
        cur = conn.cursor()
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            cur.executemany(sql, chunk)
            conn.commit()
            total += len(chunk)
        return total
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--area-id", type=int, help="Test a single area_id")
    parser.add_argument("--limit", type=int, help="Limit districts when running batch")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    args = parser.parse_args()

    db = Database(
        host="localhost",
        user="root",
        password="",
        database="craw_db",
        port=3306,
    )

    if args.area_id:
        wards, status, data = fetch_wards(args.area_id)
        if data:
            print(f"[DEBUG] Response keys: {list(data.keys())}")
        if wards is None:
            print(f"[FAIL] area_id={args.area_id} status={status}")
            return 1
        print(f"[OK] area_id={args.area_id} wards={len(wards)}")
        if wards:
            print(f"[DEBUG] First ward keys: {list(wards[0].keys())}")
        if args.dry_run:
            print("[DRY RUN] No DB changes.")
            return 0
        # Resolve region_id from DB
        conn = db.get_connection(use_database=True)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT region_id FROM location_detail WHERE level=2 AND area_id=%s LIMIT 1",
                (args.area_id,),
            )
            row = cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            conn.close()
        if not row:
            print(f"[WARN] area_id={args.area_id} not found in location_detail (level=2)")
            return 1
        region_id = row[0] if isinstance(row, tuple) else row.get("region_id")
        inserted = upsert_wards(db, region_id, args.area_id, wards)
        print(f"[DB] Upserted wards: {inserted}")
        return 0

    districts = get_districts_missing_wards(db, limit=args.limit)
    if not districts:
        print("[INFO] No districts missing wards.")
        return 0

    print(f"[INFO] Districts missing wards: {len(districts)}")
    for idx, dist in enumerate(districts, 1):
        region_id = dist["region_id"]
        area_id = dist["area_id"]
        name = dist.get("name") or ""
        wards, status, _ = fetch_wards(area_id)
        safe_name = name.encode("utf-8", "replace").decode("utf-8")
        if wards is None:
            print(f"[{idx}/{len(districts)}] area_id={area_id} {safe_name} -> HTTP {status}")
        else:
            print(f"[{idx}/{len(districts)}] area_id={area_id} {safe_name} -> wards={len(wards)}")
            if not args.dry_run and wards:
                inserted = upsert_wards(db, region_id, area_id, wards)
                print(f"[{idx}/{len(districts)}] area_id={area_id} upserted={inserted}")

        time.sleep(random.uniform(1.0, 2.0))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
