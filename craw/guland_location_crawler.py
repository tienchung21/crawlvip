#!/usr/bin/env python3
"""Crawl Guland location districts into MySQL.

Bảng lưu:
  location_guland

Mỗi dòng chứa:
  - province_id, province_name
  - district_id, district_name

API sử dụng:
  https://guland.vn/get-sub-location?id={province_id}&is_bds=1
"""

import argparse
import os
import sys
import time
import re
import html

try:
    from curl_cffi import requests
except Exception as exc:
    raise RuntimeError("curl_cffi is required for guland_location_crawler.py") from exc

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    from database import Database

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://guland.vn/",
    "Origin": "https://guland.vn",
}

PROVINCES = [
    (1, "Hà Nội"),
    (2, "Hà Giang"),
    (4, "Cao Bằng"),
    (6, "Bắc Kạn"),
    (8, "Tuyên Quang"),
    (10, "Lào Cai"),
    (11, "Điện Biên"),
    (12, "Lai Châu"),
    (14, "Sơn La"),
    (15, "Yên Bái"),
    (17, "Hòa Bình"),
    (19, "Thái Nguyên"),
    (20, "Lạng Sơn"),
    (22, "Quảng Ninh"),
    (24, "Bắc Giang"),
    (25, "Phú Thọ"),
    (26, "Vĩnh Phúc"),
    (27, "Bắc Ninh"),
    (30, "Hải Dương"),
    (31, "Hải Phòng"),
    (33, "Hưng Yên"),
    (34, "Thái Bình"),
    (35, "Hà Nam"),
    (36, "Nam Định"),
    (37, "Ninh Bình"),
    (38, "Thanh Hóa"),
    (40, "Nghệ An"),
    (42, "Hà Tĩnh"),
    (44, "Quảng Bình"),
    (45, "Quảng Trị"),
    (46, "Thừa Thiên Huế"),
    (48, "Đà Nẵng"),
    (49, "Quảng Nam"),
    (51, "Quảng Ngãi"),
    (52, "Bình Định"),
    (54, "Phú Yên"),
    (56, "Khánh Hòa"),
    (58, "Ninh Thuận"),
    (60, "Bình Thuận"),
    (62, "Kon Tum"),
    (64, "Gia Lai"),
    (66, "Đắk Lắk"),
    (67, "Đắk Nông"),
    (68, "Lâm Đồng"),
    (70, "Bình Phước"),
    (72, "Tây Ninh"),
    (74, "Bình Dương"),
    (75, "Đồng Nai"),
    (77, "Bà Rịa - Vũng Tàu"),
    (79, "TP. Hồ Chí Minh"),
    (80, "Long An"),
    (82, "Tiền Giang"),
    (83, "Bến Tre"),
    (84, "Trà Vinh"),
    (86, "Vĩnh Long"),
    (87, "Đồng Tháp"),
    (89, "An Giang"),
    (91, "Kiên Giang"),
    (92, "Cần Thơ"),
    (93, "Hậu Giang"),
    (94, "Sóc Trăng"),
    (95, "Bạc Liêu"),
    (96, "Cà Mau"),
]

API_URL = "https://guland.vn/get-sub-location"

OPTION_RE = re.compile(r"<option[^>]*value\s*=\s*\"?(\d+)\"?[^>]*>(.*?)</option>", re.I | re.S)


def ensure_table(db: Database):
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS location_guland (
                id INT AUTO_INCREMENT PRIMARY KEY,
                province_id INT NOT NULL,
                province_name VARCHAR(255) NOT NULL,
                district_id INT NOT NULL,
                district_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_location_guland (province_id, district_id),
                KEY idx_location_guland_province_id (province_id),
                KEY idx_location_guland_district_id (district_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def parse_districts(html_text: str):
    districts = []
    for match in OPTION_RE.finditer(html_text):
        district_id = int(match.group(1))
        name = html.unescape(match.group(2).strip())
        if district_id == 0 or not name or name.lower().startswith("- chọn"):
            continue
        districts.append((district_id, name))
    return districts


def fetch_districts(province_id: int):
    # Guland expects zero-padded ids for single-digit provinces (e.g. "01", "02").
    province_id_param = f"{province_id:02d}" if province_id < 10 else str(province_id)
    params = {"id": province_id_param, "is_bds": 1}
    resp = requests.get(API_URL, params=params, headers=HEADERS, impersonate="chrome124", timeout=40)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for province_id={province_id}")
    return parse_districts(resp.text or "")


def save_locations(db: Database, province_id: int, province_name: str, districts):
    if not districts:
        return 0
    conn = db.get_connection()
    cur = conn.cursor()
    inserted = 0
    try:
        for district_id, district_name in districts:
            cur.execute(
                """
                INSERT IGNORE INTO location_guland (
                    province_id, province_name, district_id, district_name
                ) VALUES (%s, %s, %s, %s)
                """,
                (province_id, province_name, district_id, district_name),
            )
            inserted += cur.rowcount
        conn.commit()
        return inserted
    finally:
        cur.close()
        conn.close()


def run(province_ids=None, delay_s=1.0):
    db = Database()
    ensure_table(db)

    total_inserted = 0
    total_districts = 0

    for province_id, province_name in PROVINCES:
        if province_ids and province_id not in province_ids:
            continue

        print(f"-> Fetching province {province_id} - {province_name}")
        try:
            districts = fetch_districts(province_id)
        except Exception as exc:
            print(f"   [x] Failed province {province_id}: {exc}")
            continue

        print(f"   Found {len(districts)} districts/wards")
        total_districts += len(districts)
        inserted = save_locations(db, province_id, province_name, districts)
        total_inserted += inserted
        print(f"   Inserted {inserted} new rows")
        time.sleep(delay_s)

    print("\nFinished.")
    print(f"Total districts fetched: {total_districts}")
    print(f"Total rows inserted: {total_inserted}")


def parse_args():
    ap = argparse.ArgumentParser(description="Fetch Guland province/district list into location_guland")
    ap.add_argument("--province-ids", type=str, default=None, help="Comma-separated province ids to fetch, e.g. 79,38")
    ap.add_argument("--delay", type=float, default=1.0, help="Delay between requests")
    return ap.parse_args()


def main():
    args = parse_args()
    province_ids = None
    if args.province_ids:
        province_ids = {int(x.strip()) for x in args.province_ids.split(",") if x.strip().isdigit()}
    run(province_ids=province_ids, delay_s=args.delay)


if __name__ == "__main__":
    main()
