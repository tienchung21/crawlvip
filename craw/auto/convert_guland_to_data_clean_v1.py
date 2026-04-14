import argparse
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter

import pymysql


DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

SCRIPT_NAME = "convert_guland_to_data_clean_v1.py"
DOMAIN = "guland.vn"

RAW_KEEP = {
    "Nhà riêng",
    "Đất",
    "Căn hộ chung cư",
    "Kho, nhà xưởng",
}

RAW_KEEP_SQL = ", ".join(f"'{x}'" for x in sorted(RAW_KEEP))


def connect():
    return pymysql.connect(**DB_CONFIG)


def ensure_columns(conn) -> None:
    alters = [
        "ALTER TABLE data_clean_v1 ADD COLUMN price_land BIGINT NULL",
        "ALTER TABLE data_clean_v1 ADD COLUMN land_price_status VARCHAR(20) NULL",
    ]
    with conn.cursor() as cur:
        for sql in alters:
            try:
                cur.execute(sql)
                conn.commit()
            except Exception:
                conn.rollback()


def make_ad_id(source_post_id: Any) -> str:
    return f"guland_{source_post_id}"


def to_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(value)
    except Exception:
        return None


def to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def median_group_for(row: Dict[str, Any]) -> Optional[int]:
    trade_type = (row.get("trade_type") or "").strip()
    loaibds = (row.get("loaibds") or "").strip()
    if trade_type == "u":
        return 4
    if trade_type != "s":
        return None
    if loaibds == "Nhà riêng":
        return 1
    if loaibds == "Căn hộ chung cư":
        return 2
    if loaibds in ("Đất", "Kho, nhà xưởng"):
        return 3
    return None


def price_land_for(row: Dict[str, Any]) -> Optional[int]:
    trade_type = (row.get("trade_type") or "").strip()
    loaibds = (row.get("loaibds") or "").strip()
    price_vnd = to_int(row.get("price"))
    if trade_type != "s" or not price_vnd or price_vnd <= 0:
        return None
    if loaibds == "Đất":
        return price_vnd
    if loaibds in ("Nhà riêng", "Kho, nhà xưởng"):
        return int(price_vnd * 0.85)
    return None


def land_price_status_for(row: Dict[str, Any]) -> str:
    trade_type = (row.get("trade_type") or "").strip()
    loaibds = (row.get("loaibds") or "").strip()
    if trade_type == "s" and loaibds in ("Đất", "Nhà riêng", "Kho, nhà xưởng"):
        return "DONE"
    return "SKIP"


def process_status_for(row: Dict[str, Any]) -> int:
    median_group = median_group_for(row)
    std_date = std_date_for(row)
    if median_group is not None and std_date is not None:
        return 6
    if median_group is not None:
        return 5
    return 0


def orig_list_time_for(row: Dict[str, Any]) -> Optional[int]:
    posted_at = row.get("posted_at")
    if not posted_at:
        return None
    return int(posted_at.strftime("%Y%m%d"))


def update_time_for(row: Dict[str, Any]) -> Optional[int]:
    created_at = row.get("created_at")
    if not created_at:
        return None
    return int(created_at.timestamp())


def std_date_for(row: Dict[str, Any]):
    posted_at = row.get("posted_at")
    if not posted_at:
        return None
    try:
        return posted_at.date()
    except Exception:
        return posted_at


def build_payload(row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    loaibds = (row.get("loaibds") or "").strip()
    trade_type = (row.get("trade_type") or "").strip()
    if loaibds not in RAW_KEEP:
        return None, "skip_type"
    if trade_type not in ("s", "u"):
        return None, "skip_type"
    if not row.get("province_id") or not row.get("ward_id"):
        return None, "skip_region"
    if not row.get("price") or not row.get("area"):
        return None, "skip_price_or_area"

    std_area = to_float(row.get("area"))
    price_vnd = to_int(row.get("price"))
    if not std_area or std_area <= 0 or not price_vnd or price_vnd <= 0:
        return None, "skip_price_or_area"

    median_group = median_group_for(row)
    if median_group is None:
        return None, "skip_group"

    payload = {
        "ad_id": make_ad_id(row["source_post_id"]),
        "src_province_id": None,
        "src_district_id": None,
        "src_ward_id": None,
        "cf_province_id": to_int(row.get("province_id")),
        "cf_district_id": to_int(row.get("district_id")),
        "cf_ward_id": to_int(row.get("ward_id")),
        "cf_street_id": to_int(row.get("street_id")),
        "project_id": to_int(row.get("project_id")),
        "src_size": row.get("dientich"),
        "unit": row.get("unit"),
        "src_price": row.get("khoanggia"),
        "src_category_id": row.get("loaibds"),
        "src_type": trade_type,
        "std_area": std_area,
        "std_category": row.get("loaibds"),
        "std_trans_type": trade_type,
        "std_date": std_date_for(row),
        "price_vnd": price_vnd,
        "price_m2": round(price_vnd / std_area, 2) if std_area > 0 else None,
        "price_land": price_land_for(row),
        "orig_list_time": orig_list_time_for(row),
        "update_time": update_time_for(row),
        "url": row.get("url"),
        "domain": DOMAIN,
        "last_script": SCRIPT_NAME,
        "process_status": process_status_for(row),
        "median_group": median_group,
        "land_price_status": land_price_status_for(row),
    }
    return payload, None


def fetch_rows(conn, matin_list: Optional[List[str]], limit: int) -> List[Dict[str, Any]]:
    if matin_list:
        placeholders = ",".join(["%s"] * len(matin_list))
        sql = f"""
            SELECT
                df.id AS data_full_id,
                df.source_post_id,
                df.province_id,
                df.district_id,
                df.ward_id,
                df.street_id,
                df.project_id,
                df.price,
                df.area,
                df.unit,
                df.type_id,
                df.cat_id,
                df.property_type,
                df.posted_at,
                df.time_converted_at,
                sdf.id AS detail_id,
                sdf.matin,
                sdf.loaibds,
                sdf.trade_type,
                sdf.diachi,
                sdf.ngaydang,
                sdf.khoanggia,
                sdf.dientich,
                sdf.url,
                sdf.created_at
            FROM scraped_details_flat sdf
            JOIN data_full df
              ON df.source = %s
             AND df.source_post_id = sdf.matin
            WHERE sdf.domain = %s
              AND df.price > 0
              AND df.area > 0
              AND sdf.trade_type IN ('s', 'u')
              AND sdf.loaibds IN ({RAW_KEEP_SQL})
              AND sdf.matin IN ({placeholders})
            ORDER BY sdf.id DESC
        """
        sql_params: List[Any] = [DOMAIN, DOMAIN, *matin_list]
    else:
        sql = f"""
            SELECT
                df.id AS data_full_id,
                df.source_post_id,
                df.province_id,
                df.district_id,
                df.ward_id,
                df.street_id,
                df.project_id,
                df.price,
                df.area,
                df.unit,
                df.type_id,
                df.cat_id,
                df.property_type,
                df.posted_at,
                df.time_converted_at,
                sdf.id AS detail_id,
                sdf.matin,
                sdf.loaibds,
                sdf.trade_type,
                sdf.diachi,
                sdf.ngaydang,
                sdf.khoanggia,
                sdf.dientich,
                sdf.url,
                sdf.created_at
            FROM data_full df
            JOIN scraped_details_flat sdf
              ON df.source_post_id = sdf.matin
             AND sdf.domain = %s
            WHERE df.source = %s
              AND COALESCE(sdf.cleanv1_converted, 0) = 0
              AND COALESCE(sdf.datafull_converted, 0) = 1
              AND df.price > 0
              AND df.area > 0
              AND sdf.trade_type IN ('s', 'u')
              AND sdf.loaibds IN ({RAW_KEEP_SQL})
            ORDER BY df.id DESC
            LIMIT %s
        """
        sql_params = [DOMAIN, DOMAIN, limit]
    with conn.cursor() as cur:
        cur.execute(sql, sql_params)
        return cur.fetchall()


def upsert_payload(cur, payload: Dict[str, Any]) -> int:
    cols = [
        "ad_id",
        "src_province_id",
        "src_district_id",
        "src_ward_id",
        "cf_province_id",
        "cf_district_id",
        "cf_ward_id",
        "cf_street_id",
        "project_id",
        "src_size",
        "unit",
        "src_price",
        "src_category_id",
        "src_type",
        "std_area",
        "std_category",
        "std_trans_type",
        "std_date",
        "price_vnd",
        "price_m2",
        "price_land",
        "orig_list_time",
        "update_time",
        "url",
        "domain",
        "last_script",
        "process_status",
        "median_group",
        "land_price_status",
    ]
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(
        [
            "cf_province_id=VALUES(cf_province_id)",
            "cf_district_id=VALUES(cf_district_id)",
            "cf_ward_id=VALUES(cf_ward_id)",
            "cf_street_id=VALUES(cf_street_id)",
            "project_id=VALUES(project_id)",
            "src_size=VALUES(src_size)",
            "unit=VALUES(unit)",
            "src_price=VALUES(src_price)",
            "src_category_id=VALUES(src_category_id)",
            "src_type=VALUES(src_type)",
            "std_area=VALUES(std_area)",
            "std_category=VALUES(std_category)",
            "std_trans_type=VALUES(std_trans_type)",
            "std_date=VALUES(std_date)",
            "price_vnd=VALUES(price_vnd)",
            "price_m2=VALUES(price_m2)",
            "orig_list_time=VALUES(orig_list_time)",
            "update_time=VALUES(update_time)",
            "url=VALUES(url)",
            "last_script=VALUES(last_script)",
            "process_status=VALUES(process_status)",
            "median_group=VALUES(median_group)",
            "price_land=COALESCE(price_land, VALUES(price_land))",
            "land_price_status=COALESCE(land_price_status, VALUES(land_price_status))",
        ]
    )
    sql = f"""
        INSERT INTO data_clean_v1 ({", ".join(cols)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates}
    """
    values = [payload[c] for c in cols]
    cur.execute(sql, values)
    return cur.rowcount


def mark_source_converted(cur, matin: Any) -> None:
    cur.execute(
        """
        UPDATE scraped_details_flat
        SET cleanv1_converted = 1,
            cleanv1_converted_at = NOW()
        WHERE domain = %s
          AND matin = %s
        """,
        (DOMAIN, str(matin)),
    )


def main():
    parser = argparse.ArgumentParser(description="Convert Guland data_full + detail_flat -> data_clean_v1")
    parser.add_argument("--matin", default="", help="Comma separated matin list")
    parser.add_argument("--preview-limit", type=int, default=20)
    parser.add_argument("--insert", action="store_true")
    args = parser.parse_args()

    matin_list = [x.strip() for x in args.matin.split(",") if x.strip()] or None

    conn = connect()
    try:
        ensure_columns(conn)
        rows = fetch_rows(conn, matin_list, args.preview_limit)
        print(f"selected={len(rows)}")
        inserted = 0
        skipped = 0
        skip_counts: Counter[str] = Counter()
        with conn.cursor() as cur:
            for row in rows:
                payload, skip_reason = build_payload(row)
                if skip_reason:
                    skipped += 1
                    skip_counts[skip_reason] += 1
                    continue
                if args.insert:
                    inserted += upsert_payload(cur, payload)
                    mark_source_converted(cur, row["matin"])
            if args.insert:
                conn.commit()
        print(f"insert_mode={args.insert} inserted={inserted} skipped={skipped}")
        if skip_counts:
            print(f"skip_summary={dict(sorted(skip_counts.items()))}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
