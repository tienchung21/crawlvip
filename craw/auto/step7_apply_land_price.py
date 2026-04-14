import argparse
import time
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional

import pymysql


BATCH_SIZE_DEFAULT = 5000

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}


DOMAIN_RULES: Dict[str, Dict[str, Any]] = {
    "batdongsan": {
        "domain_value": "batdongsan.com.vn",
        "land": {"Đất", "Đất nền dự án"},
        "house": {
            "Nhà riêng",
            "Nhà mặt phố",
            "Biệt thự liền kề",
            "Kho, nhà xưởng, đất",
            "Kho, nhà xưởng",
            "Shophouse",
            "Trang trại/Khu nghỉ dưỡng",
        },
    },
    "mogi": {
        "domain_value": "mogi",
        "land": {"Đất thổ cư", "Đất nền dự án", "Đất nông nghiệp", "Đất kho xưởng"},
        "house": {
            "Nhà mặt tiền phố",
            "Nhà hẻm ngõ",
            "Nhà biệt thự, liền kề",
            "Đường nội bộ",
            "Nhà xưởng, kho bãi",
        },
    },
    "nhatot": {
        "domain_value": "nhatot",
        "land": {"1040"},
        "house": {"1020"},
    },
    "nhadat": {
        "domain_value": "nhadat",
        "land": {"8", "10", "11", "57"},
        "house": {"1", "2", "3", "13", "14"},
    },
}


def get_conn():
    return pymysql.connect(**DB_CONFIG)


def normalize_category(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value).strip().rstrip(" .")


def to_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(value)
    except Exception:
        return None


def ensure_columns(cur, conn) -> None:
    alters = [
        "ALTER TABLE data_clean_v1 ADD COLUMN price_land BIGINT NULL",
        "ALTER TABLE data_clean_v1 ADD COLUMN land_price_status VARCHAR(20) NULL",
    ]
    for sql in alters:
        try:
            cur.execute(sql)
            conn.commit()
        except Exception:
            conn.rollback()


def compute_land(domain_key: str, row: Dict[str, Any]) -> Dict[str, Any]:
    rule = DOMAIN_RULES[domain_key]
    cat = normalize_category(row.get("std_category"))
    trans_type = (row.get("std_trans_type") or "").strip()
    price_vnd = to_int(row.get("price_vnd"))

    if trans_type != "s":
        return {"price_land": None, "land_price_status": "SKIP"}
    if not price_vnd or price_vnd <= 0:
        return {"price_land": None, "land_price_status": "SKIP"}
    if cat in rule["land"]:
        return {"price_land": price_vnd, "land_price_status": "DONE"}
    if cat in rule["house"]:
        return {"price_land": int(round(price_vnd * 0.85)), "land_price_status": "DONE"}
    return {"price_land": None, "land_price_status": "SKIP"}


def fetch_batch(cur, domain_value: str, batch_size: int) -> Iterable[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, std_category, std_trans_type, price_vnd
        FROM data_clean_v1
        WHERE domain = %s
          AND COALESCE(process_status, 0) >= 6
          AND land_price_status IS NULL
        ORDER BY id
        LIMIT %s
        """,
        (domain_value, batch_size),
    )
    return cur.fetchall()


def main():
    parser = argparse.ArgumentParser(description="Apply land price mapping after Step 6 without changing process_status.")
    parser.add_argument("--domain", required=True, choices=sorted(DOMAIN_RULES.keys()))
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT)
    args = parser.parse_args()

    domain_key = args.domain
    domain_value = DOMAIN_RULES[domain_key]["domain_value"]
    script_name = f"step7_apply_land_price.py:{domain_key}"

    conn = get_conn()
    cur = conn.cursor()
    ensure_columns(cur, conn)

    started = time.time()
    total_selected = 0
    total_done = 0
    total_skip = 0
    batch_no = 0

    while True:
        rows = list(fetch_batch(cur, domain_value, int(args.batch_size)))
        if not rows:
            break

        batch_no += 1
        total_selected += len(rows)
        updates = []
        batch_done = 0
        batch_skip = 0

        for row in rows:
            result = compute_land(domain_key, row)
            if result["land_price_status"] == "DONE":
                batch_done += 1
            else:
                batch_skip += 1
            updates.append(
                (
                    result["price_land"],
                    result["land_price_status"],
                    script_name,
                    row["id"],
                )
            )

        cur.executemany(
            """
            UPDATE data_clean_v1
            SET
                price_land = %s,
                land_price_status = %s,
                last_script = %s
            WHERE id = %s
              AND land_price_status IS NULL
            """,
            updates,
        )
        conn.commit()
        total_done += batch_done
        total_skip += batch_skip
        print(
            f"[{domain_key.upper()}][LAND][Batch {batch_no}] "
            f"selected={len(rows)} done={batch_done} skip={batch_skip}",
            flush=True,
        )
        if len(rows) < int(args.batch_size):
            break

    print(
        f"[{domain_key.upper()}][LAND] total_selected={total_selected} "
        f"done={total_done} skip={total_skip} elapsed={time.time() - started:.2f}s",
        flush=True,
    )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
