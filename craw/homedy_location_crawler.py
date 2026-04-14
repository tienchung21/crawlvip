#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from typing import Dict, List, Optional, Tuple

import pymysql
from curl_cffi import requests

CITY_API = "https://homedy.com/Common/CityAC"
DISTRICT_API = "https://homedy.com/Common/DistrictAC"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Crawl Homedy city/district/ward -> location_homedy")
    p.add_argument("--db-host", default="localhost")
    p.add_argument("--db-port", type=int, default=3306)
    p.add_argument("--db-user", default="root")
    p.add_argument("--db-pass", default="")
    p.add_argument("--db-name", default="craw_db")
    p.add_argument("--delay", type=float, default=0.15)
    p.add_argument("--city-id", type=int, default=0, help="Only crawl 1 city if >0")
    p.add_argument("--recreate-table", action="store_true")
    return p.parse_args()


def db_conn(args: argparse.Namespace):
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def ensure_table(conn, recreate: bool = False) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS location_homedy (
      id BIGINT NOT NULL AUTO_INCREMENT,
      level_type ENUM('city','district','ward') NOT NULL,
      location_id BIGINT NOT NULL,
      city_id BIGINT DEFAULT NULL,
      district_id BIGINT DEFAULT NULL,
      parent_id BIGINT DEFAULT NULL,
      prefix VARCHAR(50) DEFAULT NULL,
      name VARCHAR(255) DEFAULT NULL,
      full_name VARCHAR(255) DEFAULT NULL,
      full_address VARCHAR(500) DEFAULT NULL,
      latitude DOUBLE DEFAULT NULL,
      longitude DOUBLE DEFAULT NULL,
      product_sell_count INT DEFAULT NULL,
      product_lease_count INT DEFAULT NULL,
      created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_level_location (level_type, location_id),
      KEY idx_city_district (city_id, district_id),
      KEY idx_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        if recreate:
            cur.execute("DROP TABLE IF EXISTS location_homedy")
        cur.execute(ddl)
    conn.commit()


def fetch_json(url: str, params: Optional[Dict] = None, retries: int = 3) -> Dict:
    session = requests.Session()
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=30, impersonate="chrome124")
            if r.status_code == 200:
                return r.json()
            last_err = f"http_{r.status_code}"
        except Exception as ex:
            last_err = f"{type(ex).__name__}: {ex}"
        if attempt < retries:
            time.sleep(min(attempt, 2))
    raise RuntimeError(f"fetch failed {url} params={params} err={last_err}")


def upsert_location(
    conn,
    level_type: str,
    location_id: int,
    city_id: Optional[int],
    district_id: Optional[int],
    parent_id: Optional[int],
    prefix: Optional[str],
    name: Optional[str],
    full_name: Optional[str],
    full_address: Optional[str],
    latitude: Optional[float],
    longitude: Optional[float],
    product_sell_count: Optional[int],
    product_lease_count: Optional[int],
) -> None:
    sql = """
    INSERT INTO location_homedy (
      level_type, location_id, city_id, district_id, parent_id, prefix, name, full_name, full_address,
      latitude, longitude, product_sell_count, product_lease_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      city_id = VALUES(city_id),
      district_id = VALUES(district_id),
      parent_id = VALUES(parent_id),
      prefix = VALUES(prefix),
      name = VALUES(name),
      full_name = VALUES(full_name),
      full_address = VALUES(full_address),
      latitude = VALUES(latitude),
      longitude = VALUES(longitude),
      product_sell_count = VALUES(product_sell_count),
      product_lease_count = VALUES(product_lease_count),
      updated_at = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                level_type,
                int(location_id),
                city_id,
                district_id,
                parent_id,
                prefix,
                name,
                full_name,
                full_address,
                latitude,
                longitude,
                product_sell_count,
                product_lease_count,
            ),
        )


def as_int(v) -> Optional[int]:
    try:
        if v in (None, "", "null"):
            return None
        return int(v)
    except Exception:
        return None


def as_float(v) -> Optional[float]:
    try:
        if v in (None, "", "null"):
            return None
        return float(v)
    except Exception:
        return None


def main() -> int:
    args = parse_args()
    conn = db_conn(args)
    ensure_table(conn, recreate=args.recreate_table)

    obj = fetch_json(CITY_API)
    cities: List[Dict] = obj.get("Data") or []
    if args.city_id > 0:
        cities = [c for c in cities if as_int(c.get("Id")) == args.city_id]

    total_city = total_district = total_ward = 0

    for c in cities:
        cid = as_int(c.get("Id"))
        if not cid:
            continue
        upsert_location(
            conn=conn,
            level_type="city",
            location_id=cid,
            city_id=cid,
            district_id=None,
            parent_id=None,
            prefix=None,
            name=(c.get("Name") or "").strip() or None,
            full_name=(c.get("Name") or "").strip() or None,
            full_address=None,
            latitude=None,
            longitude=None,
            product_sell_count=None,
            product_lease_count=None,
        )
        total_city += 1
        print(f"[CITY] {cid} {c.get('Name')}")

        d_obj = fetch_json(DISTRICT_API, params={"CityId": cid})
        districts: List[Dict] = d_obj.get("Data") or []
        print(f"  [DISTRICTS] city_id={cid} count={len(districts)}")
        for d in districts:
            did = as_int(d.get("Id"))
            if not did:
                continue
            upsert_location(
                conn=conn,
                level_type="district",
                location_id=did,
                city_id=cid,
                district_id=did,
                parent_id=cid,
                prefix=(d.get("Pre") or "").strip() or None,
                name=(d.get("Name") or "").strip() or None,
                full_name=(d.get("FullName") or "").strip() or None,
                full_address=(d.get("FullAddress") or "").strip() or None,
                latitude=as_float(d.get("Latitude")),
                longitude=as_float(d.get("Longitude")),
                product_sell_count=as_int(d.get("ProductSellCount")),
                product_lease_count=as_int(d.get("ProductLeaseCount")),
            )
            total_district += 1

            wards = d.get("Wards") or []
            for w in wards:
                wid = as_int(w.get("Id"))
                if not wid:
                    continue
                upsert_location(
                    conn=conn,
                    level_type="ward",
                    location_id=wid,
                    city_id=cid,
                    district_id=did,
                    parent_id=did,
                    prefix=(w.get("Pre") or "").strip() or None,
                    name=(w.get("Name") or "").strip() or None,
                    full_name=None,
                    full_address=(w.get("FullAddress") or "").strip() or None,
                    latitude=as_float(w.get("Latitude")),
                    longitude=as_float(w.get("Longitude")),
                    product_sell_count=as_int(w.get("ProductSellCount")),
                    product_lease_count=as_int(w.get("ProductLeaseCount")),
                )
                total_ward += 1

        conn.commit()
        if args.delay > 0:
            time.sleep(args.delay)

    conn.commit()
    print(f"[DONE] cities={total_city} districts={total_district} wards={total_ward}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

