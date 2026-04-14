#!/usr/bin/env python3
"""
Bao cao so tin data_clean_v1 theo tinh, tu Chu nhat gan nhat den hien tai.

Thu tu tinh duoc lay tu file TSV mau do nguoi dung cung cap.
Ngay goc uu tien theo cột created_at:
- nhatot/batdongsan/mogi/alonhadat/guland -> scraped_details_flat.created_at
- nhadat                                  -> nhadat_data.orig_list_time (khong co URL de join sang scraped_details_flat)
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pymysql


TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DEFAULT_ORDER_FILE = Path("/home/chungnt/crawlvip/uploaded_listing_province_2026-03-15_to_now.tsv")
DEFAULT_OUTPUT_DIR = Path("/home/chungnt/crawlvip")


@dataclass
class ProvinceRow:
    province_id: int
    province_name: str


def most_recent_sunday(today: date) -> date:
    days_back = (today.weekday() + 1) % 7
    return today - timedelta(days=days_back)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report cleanv1 counts by province since a start date")
    parser.add_argument("--start-date", help="YYYY-MM-DD, default = most recent Sunday in Asia/Ho_Chi_Minh")
    parser.add_argument("--order-file", default=str(DEFAULT_ORDER_FILE), help="TSV file that defines province order")
    parser.add_argument("--output", help="Optional explicit output TSV path")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-user", default="root")
    parser.add_argument("--db-password", default="")
    parser.add_argument("--db-name", default="craw_db")
    return parser.parse_args()


def resolve_start_date(raw: str | None) -> date:
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    return most_recent_sunday(datetime.now(TZ).date())


def load_province_order(path: Path) -> list[ProvinceRow]:
    rows: list[ProvinceRow] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            province_id = int(row["province_id"])
            province_name = (row["province_name"] or "").strip()
            rows.append(ProvinceRow(province_id=province_id, province_name=province_name))
    return rows


def get_conn(args: argparse.Namespace):
    return pymysql.connect(
        host=args.db_host,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def add_counts(counter: Counter, rows: list[dict]) -> None:
    for row in rows:
        province_id = row.get("province_id")
        if province_id is None:
            continue
        counter[int(province_id)] += int(row.get("c") or 0)


def fetch_sdf_counts_by_created_at(cur, clean_domain: str, sdf_domain: str, start_date_str: str) -> list[dict]:
    cur.execute(
        """
        SELECT d.cf_province_id AS province_id, COUNT(*) AS c
        FROM data_clean_v1 d
        JOIN (
            SELECT DISTINCT url
            FROM scraped_details_flat
            WHERE domain = %s
              AND DATE(created_at) >= %s
        ) src ON src.url = d.url
        WHERE d.domain = %s
          AND d.cf_province_id IS NOT NULL
        GROUP BY d.cf_province_id
        """
        ,
        (sdf_domain, start_date_str, clean_domain),
    )
    return cur.fetchall()


def fetch_nhadat_counts(cur, start_date_str: str) -> list[dict]:
    cur.execute(
        """
        SELECT d.cf_province_id AS province_id, COUNT(*) AS c
        FROM data_clean_v1 d
        JOIN nhadat_data n
          ON d.ad_id = CONCAT('nhadat_', n.realestate_id)
        WHERE d.domain = 'nhadat'
          AND d.cf_province_id IS NOT NULL
          AND STR_TO_DATE(n.orig_list_time, '%%d/%%m/%%Y') >= %s
        GROUP BY d.cf_province_id
        """
        ,
        (start_date_str,),
    )
    return cur.fetchall()


def build_counts(args: argparse.Namespace, start_date: date) -> Counter:
    counter: Counter = Counter()
    start_date_str = start_date.isoformat()

    conn = get_conn(args)
    try:
        with conn.cursor() as cur:
            add_counts(counter, fetch_sdf_counts_by_created_at(cur, "nhatot", "nhatot", start_date_str))
            add_counts(counter, fetch_nhadat_counts(cur, start_date_str))
            add_counts(counter, fetch_sdf_counts_by_created_at(cur, "batdongsan.com.vn", "batdongsan.com.vn", start_date_str))
            add_counts(counter, fetch_sdf_counts_by_created_at(cur, "mogi", "mogi", start_date_str))
            add_counts(counter, fetch_sdf_counts_by_created_at(cur, "alonhadat.com.vn", "alonhadat.com.vn", start_date_str))
            add_counts(counter, fetch_sdf_counts_by_created_at(cur, "guland.vn", "guland.vn", start_date_str))
    finally:
        conn.close()
    return counter


def write_report(rows: list[ProvinceRow], counts: Counter, output_path: Path) -> int:
    total = 0
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["province_id", "province_name", "cleanv1_count"])
        for row in rows:
            count = int(counts.get(row.province_id, 0))
            total += count
            writer.writerow([row.province_id, row.province_name, count])
    return total


def main() -> int:
    args = parse_args()
    start_date = resolve_start_date(args.start_date)
    province_rows = load_province_order(Path(args.order_file))
    counts = build_counts(args, start_date)

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = DEFAULT_OUTPUT_DIR / f"cleanv1_province_{start_date.isoformat()}_to_now.tsv"

    total = write_report(province_rows, counts, output_path)

    print(f"start_date={start_date.isoformat()}")
    print(f"output={output_path}")
    print(f"province_rows={len(province_rows)}")
    print(f"total_cleanv1_count={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
