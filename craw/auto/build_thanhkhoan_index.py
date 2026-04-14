#!/usr/bin/env python3
"""
Build Liquidity/Vitality index from data_clean_v1 (+data_median as median source).

Rules implemented from craw/thanhkhoan.md:
- Group by scope (ward, region, street) + median_group + period (month/quarter/year)
- Filter outliers by trimming lowest/highest 10% of metric price
- CV = stddev_pop(trimmed_prices) / median_price
- Vitality Score = N / CV (N = trimmed rows)
- Liquidity level (ward/street scope): ranking by vitality within each province, period, median_group
  * TOP 25%   -> THANH_KHOAN_CAO
  * 25-75%    -> THANH_KHOAN_TRUNG_BINH
  * BOTTOM25% -> THANH_KHOAN_THAP
  * N < 10    -> KHONG_DU_DU_LIEU
"""

import argparse
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import median, pstdev
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import SSDictCursor


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
}


@dataclass
class AggRow:
    period_type: str
    period_value: str
    scope: str
    province_id: Optional[int]
    ward_id: Optional[int]
    street_id: Optional[int]
    median_group: int
    raw_rows: int
    trimmed_rows: int
    median_price_m2: Optional[float]
    stddev_price_m2: Optional[float]
    cv: Optional[float]
    vitality_score: Optional[float]
    liquidity_level: Optional[str]
    median_source: str


def get_conn():
    return pymysql.connect(**DB_CONFIG)


def ensure_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS thanhkhoan_index (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        period_type ENUM('month','quarter','year') NOT NULL,
        period_value VARCHAR(10) NOT NULL,
        scope ENUM('ward','region','street') NOT NULL,
        province_id INT NULL,
        ward_id INT NULL,
        street_id INT NULL,
        median_group TINYINT NOT NULL,
        raw_rows INT NOT NULL,
        trimmed_rows INT NOT NULL,
        median_price_m2 DECIMAL(20,2) NULL,
        stddev_price_m2 DECIMAL(20,2) NULL,
        cv DECIMAL(20,6) NULL,
        vitality_score DECIMAL(20,6) NULL,
        liquidity_level VARCHAR(32) NULL,
        median_source VARCHAR(16) NOT NULL DEFAULT 'python',
        computed_at DATETIME NOT NULL,
        INDEX idx_period (period_type, period_value),
        INDEX idx_scope (scope, province_id, ward_id, street_id),
        INDEX idx_group (median_group),
        UNIQUE KEY uq_tk (
            period_type, period_value, scope,
            province_id, ward_id, street_id, median_group
        )
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        try:
            cur.execute("ALTER TABLE thanhkhoan_index MODIFY COLUMN scope ENUM('ward','region','street') NOT NULL")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE thanhkhoan_index ADD COLUMN street_id INT NULL AFTER ward_id")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE thanhkhoan_index DROP INDEX uq_tk")
        except Exception:
            pass
        try:
            cur.execute(
                """
                ALTER TABLE thanhkhoan_index
                ADD UNIQUE KEY uq_tk (
                    period_type, period_value, scope,
                    province_id, ward_id, street_id, median_group
                )
                """
            )
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE thanhkhoan_index DROP INDEX idx_scope")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE thanhkhoan_index ADD INDEX idx_scope (scope, province_id, ward_id, street_id)")
        except Exception:
            pass


def quarter_of(month_str: str) -> str:
    year, mm = month_str.split("-")
    q = (int(mm) - 1) // 3 + 1
    return f"{year}-Q{q}"


def year_of(month_str: str) -> str:
    return month_str.split("-")[0]


def safe_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def load_monthly_median_map(conn, from_month: str, to_month: str) -> Dict[Tuple[str, int, int, str], float]:
    """
    Key: (scope, area_id, median_group, month) => median_price_m2
    scope=ward -> area_id = new_ward_id
    scope=region -> area_id = new_region_id
    scope=street -> area_id = street_id
    """
    out: Dict[Tuple[str, int, int, str], float] = {}
    sql = """
    SELECT scope, new_ward_id, new_region_id, street_id, median_group, month, median_price_m2
    FROM data_median
    WHERE scope IN ('ward','region','street')
      AND month >= %s AND month <= %s
      AND median_group IS NOT NULL
      AND median_price_m2 IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(sql, (from_month, to_month))
        rows = cur.fetchall()
    for r in rows:
        scope = r["scope"]
        mg = safe_int(r["median_group"])
        m = r["month"]
        med = float(r["median_price_m2"])
        if mg is None:
            continue
        if scope == "ward":
            area = safe_int(r["new_ward_id"])
        elif scope == "street":
            area = safe_int(r["street_id"])
        else:
            area = safe_int(r["new_region_id"])
        if area is None:
            continue
        out[(scope, area, mg, m)] = med
    return out


def stream_rows(conn, from_month: str, to_month: str, domain: str = ""):
    where_domain = ""
    params: List = [from_month, to_month]
    if domain:
        where_domain = " AND domain = %s "
        params.append(domain)

    sql = f"""
    SELECT
        std_month,
        cf_province_id,
        cf_ward_id,
        cf_street_id,
        domain,
        median_group,
        price_m2,
        land_price_status,
        price_land,
        std_area
    FROM data_clean_v1
    WHERE std_month IS NOT NULL
      AND std_month >= %s AND std_month <= %s
      AND cf_province_id IS NOT NULL
      AND (
            (median_group IS NOT NULL AND price_m2 IS NOT NULL AND price_m2 > 0)
         OR (land_price_status = 'DONE' AND price_land IS NOT NULL AND price_land > 0 AND std_area IS NOT NULL AND std_area > 0)
      )
      {where_domain}
    """
    cur = conn.cursor(SSDictCursor)
    cur.execute(sql, tuple(params))
    try:
        for row in cur:
            yield row
    finally:
        cur.close()


def trim_10_percent(sorted_prices: List[float]) -> List[float]:
    n = len(sorted_prices)
    if n <= 2:
        return sorted_prices
    cut = int(math.floor(n * 0.1))
    if cut <= 0:
        return sorted_prices
    if n - 2 * cut <= 0:
        return sorted_prices
    return sorted_prices[cut : n - cut]


def compute_metrics(prices: List[float], force_median: Optional[float]) -> Tuple[int, int, Optional[float], Optional[float], Optional[float], Optional[float], str]:
    prices = sorted(prices)
    raw_n = len(prices)
    if raw_n == 0:
        return 0, 0, None, None, None, None, "python"

    trimmed = trim_10_percent(prices)
    trim_n = len(trimmed)
    if trim_n == 0:
        return raw_n, 0, None, None, None, None, "python"

    if force_median is not None and force_median > 0:
        med = float(force_median)
        med_src = "data_median"
    else:
        med = float(median(trimmed))
        med_src = "python"

    sd = float(pstdev(trimmed)) if trim_n > 1 else 0.0
    cv = (sd / med) if med and med > 0 else None
    vitality = (trim_n / cv) if cv and cv > 0 else None

    return raw_n, trim_n, med, sd, cv, vitality, med_src


def assign_liquidity_levels(rows: List[AggRow]) -> None:
    """
    Ward/street: xếp hạng nội bộ trong từng tỉnh theo period+group.
    Region: xếp hạng toàn quốc theo period+group.
    """
    cohorts: Dict[Tuple, List[AggRow]] = defaultdict(list)

    for r in rows:
        if r.trimmed_rows < 10 or r.vitality_score is None:
            r.liquidity_level = "KHONG_DU_DU_LIEU"
            continue
        if r.scope in ("ward", "street") and r.province_id is not None:
            cohorts[(r.scope, r.period_type, r.period_value, r.province_id, r.median_group)].append(r)
        elif r.scope == "region":
            cohorts[(r.scope, r.period_type, r.period_value, r.median_group)].append(r)
        else:
            r.liquidity_level = None

    for _, arr in cohorts.items():
        arr.sort(key=lambda x: (x.vitality_score if x.vitality_score is not None else -1), reverse=True)
        n = len(arr)
        top_n = max(1, int(math.ceil(n * 0.25)))
        bot_n = max(1, int(math.ceil(n * 0.25)))

        for i, r in enumerate(arr):
            if i < top_n:
                r.liquidity_level = "THANH_KHOAN_CAO"
            elif i >= n - bot_n:
                r.liquidity_level = "THANH_KHOAN_THAP"
            else:
                r.liquidity_level = "THANH_KHOAN_TRUNG_BINH"


def upsert_rows(conn, rows: List[AggRow]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO thanhkhoan_index (
        period_type, period_value, scope,
        province_id, ward_id, street_id, median_group,
        raw_rows, trimmed_rows,
        median_price_m2, stddev_price_m2, cv, vitality_score,
        liquidity_level, median_source, computed_at
    ) VALUES (
        %s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,
        %s,%s,%s,%s,
        %s,%s,NOW()
    )
    ON DUPLICATE KEY UPDATE
        raw_rows=VALUES(raw_rows),
        trimmed_rows=VALUES(trimmed_rows),
        median_price_m2=VALUES(median_price_m2),
        stddev_price_m2=VALUES(stddev_price_m2),
        cv=VALUES(cv),
        vitality_score=VALUES(vitality_score),
        liquidity_level=VALUES(liquidity_level),
        median_source=VALUES(median_source),
        computed_at=VALUES(computed_at)
    """

    data = []
    for r in rows:
        data.append(
            (
                r.period_type,
                r.period_value,
                r.scope,
                r.province_id,
                r.ward_id,
                r.street_id,
                r.median_group,
                r.raw_rows,
                r.trimmed_rows,
                round(r.median_price_m2, 2) if r.median_price_m2 is not None else None,
                round(r.stddev_price_m2, 2) if r.stddev_price_m2 is not None else None,
                round(r.cv, 6) if r.cv is not None else None,
                round(r.vitality_score, 6) if r.vitality_score is not None else None,
                r.liquidity_level,
                r.median_source,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(sql, data)
    return len(data)


def build_index(conn, from_month: str, to_month: str, domain: str = "", ward_filter: Optional[int] = None, province_filter: Optional[int] = None) -> List[AggRow]:
    # Key => list prices
    # key: (period_type, period_value, scope, province_id, ward_id, street_id, median_group)
    grouped: Dict[Tuple[str, str, str, Optional[int], Optional[int], Optional[int], int], List[float]] = defaultdict(list)
    loaded = 0
    for r in stream_rows(conn, from_month, to_month, domain=domain):
        loaded += 1
        m = r["std_month"]
        province_id = safe_int(r["cf_province_id"])
        ward_id = safe_int(r["cf_ward_id"])
        street_id = safe_int(r["cf_street_id"])
        mg = safe_int(r["median_group"])
        price_m2 = float(r["price_m2"]) if r["price_m2"] is not None else None
        land_ok = (r.get("land_price_status") == "DONE" and r.get("price_land") is not None and r.get("std_area") is not None and float(r["std_area"]) > 0)
        land_price_m2 = (float(r["price_land"]) / float(r["std_area"])) if land_ok else None
        domain_name = r.get("domain")

        if province_id is None:
            continue
        if province_filter is not None and province_id != province_filter:
            continue
        if ward_filter is not None and ward_id != ward_filter:
            continue

        def add_metric(group_id: int, metric_value: Optional[float]) -> None:
            if metric_value is None or metric_value <= 0:
                return

            if ward_id is not None:
                grouped[("month", m, "ward", province_id, ward_id, None, group_id)].append(metric_value)
                grouped[("quarter", quarter_of(m), "ward", province_id, ward_id, None, group_id)].append(metric_value)
                grouped[("year", year_of(m), "ward", province_id, ward_id, None, group_id)].append(metric_value)

            grouped[("month", m, "region", province_id, None, None, group_id)].append(metric_value)
            grouped[("quarter", quarter_of(m), "region", province_id, None, None, group_id)].append(metric_value)
            grouped[("year", year_of(m), "region", province_id, None, None, group_id)].append(metric_value)

            if domain_name == "nhadat" and street_id is not None and street_id > 0:
                grouped[("month", m, "street", province_id, None, street_id, group_id)].append(metric_value)
                grouped[("quarter", quarter_of(m), "street", province_id, None, street_id, group_id)].append(metric_value)
                grouped[("year", year_of(m), "street", province_id, None, street_id, group_id)].append(metric_value)

        if mg is not None:
            add_metric(mg, price_m2)
        add_metric(5, land_price_m2)

    print(f"Loaded rows: {loaded:,}")

    monthly_median = load_monthly_median_map(conn, from_month, to_month)
    out: List[AggRow] = []

    for (pt, pv, scope, province_id, ward_id, street_id, mg), prices in grouped.items():
        force_median = None
        if pt == "month":
            if scope == "ward" and ward_id is not None:
                force_median = monthly_median.get(("ward", ward_id, mg, pv))
            elif scope == "street" and street_id is not None:
                force_median = monthly_median.get(("street", street_id, mg, pv))
            elif scope == "region" and province_id is not None:
                force_median = monthly_median.get(("region", province_id, mg, pv))

        raw_n, trim_n, med, sd, cv, vitality, med_src = compute_metrics(prices, force_median)

        out.append(
            AggRow(
                period_type=pt,
                period_value=pv,
                scope=scope,
                province_id=province_id,
                ward_id=ward_id,
                street_id=street_id,
                median_group=mg,
                raw_rows=raw_n,
                trimmed_rows=trim_n,
                median_price_m2=med,
                stddev_price_m2=sd,
                cv=cv,
                vitality_score=vitality,
                liquidity_level=None,
                median_source=med_src,
            )
        )

    assign_liquidity_levels(out)
    return out


def parse_args():
    p = argparse.ArgumentParser(description="Build thanhkhoan_index from data_clean_v1/data_median")
    p.add_argument("--from-month", default="2025-12", help="Start month YYYY-MM")
    p.add_argument("--to-month", default=datetime.now().strftime("%Y-%m"), help="End month YYYY-MM")
    p.add_argument("--domain", default="", help="Filter by domain in data_clean_v1")
    p.add_argument("--ward-id", type=int, default=0, help="Only compute for one ward_id")
    p.add_argument("--province-id", type=int, default=0, help="Only compute for one province_id")
    p.add_argument("--truncate", action="store_true", help="Truncate target table before insert")
    return p.parse_args()


def main():
    args = parse_args()

    conn = get_conn()
    try:
        ensure_table(conn)

        if args.truncate:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE thanhkhoan_index")
            print("Truncated thanhkhoan_index")

        rows = build_index(
            conn,
            from_month=args.from_month,
            to_month=args.to_month,
            domain=args.domain.strip(),
            ward_filter=(args.ward_id if args.ward_id > 0 else None),
            province_filter=(args.province_id if args.province_id > 0 else None),
        )
        print(f"Computed rows: {len(rows):,}")

        written = upsert_rows(conn, rows)
        print(f"Upserted rows: {written:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
