#!/usr/bin/env python3
"""
Refresh per-ward counts for data_full + data_no_full, write a pretty text file,
and sync a DB table used by the data_no_full auto uploader.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from database import Database


OUTPUT_PATH = Path("/home/chungnt/crawlvip/upload_data_no_full_pretty.txt")
COUNT_TABLE = "upload_data_no_full_area_counts"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def fetch_rows(db: Database):
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            WITH wards AS (
                SELECT
                    p.new_city_id AS province_id,
                    p.new_city_name AS province_name,
                    w.new_city_id AS ward_id,
                    w.new_city_name AS ward_name
                FROM transaction_city_merge w
                INNER JOIN transaction_city_merge p
                    ON p.new_city_id = w.new_city_parent_id
                   AND p.new_city_parent_id = 0
                WHERE w.new_city_parent_id <> 0
                  AND w.action_type = 0
                  AND p.action_type = 0
                GROUP BY p.new_city_id, p.new_city_name, w.new_city_id, w.new_city_name
            ),
            df AS (
                SELECT province_id, ward_id, COUNT(*) AS data_full_count
                FROM data_full
                GROUP BY province_id, ward_id
            ),
            dnf AS (
                SELECT province_id, ward_id, COUNT(*) AS data_no_full_count
                FROM data_no_full
                GROUP BY province_id, ward_id
            )
            SELECT
                wards.province_id,
                wards.province_name,
                wards.ward_id,
                wards.ward_name,
                COALESCE(df.data_full_count, 0) AS data_full_count,
                COALESCE(dnf.data_no_full_count, 0) AS data_no_full_count,
                COALESCE(df.data_full_count, 0) + COALESCE(dnf.data_no_full_count, 0) AS total_count
            FROM wards
            LEFT JOIN df
              ON df.province_id = wards.province_id
             AND df.ward_id = wards.ward_id
            LEFT JOIN dnf
              ON dnf.province_id = wards.province_id
             AND dnf.ward_id = wards.ward_id
            ORDER BY wards.province_id, wards.ward_name, wards.ward_id
            """
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def sync_count_table(db: Database, rows):
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {COUNT_TABLE} (
                province_id SMALLINT UNSIGNED NOT NULL,
                province_name VARCHAR(128) NOT NULL,
                ward_id INT UNSIGNED NOT NULL,
                ward_name VARCHAR(128) NOT NULL,
                data_full_count INT NOT NULL DEFAULT 0,
                data_no_full_count INT NOT NULL DEFAULT 0,
                total_count INT NOT NULL DEFAULT 0,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (province_id, ward_id),
                KEY idx_total_count (total_count)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cursor.execute(f"TRUNCATE TABLE {COUNT_TABLE}")
        if rows:
            cursor.executemany(
                f"""
                INSERT INTO {COUNT_TABLE}
                (province_id, province_name, ward_id, ward_name, data_full_count, data_no_full_count, total_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        row["province_id"],
                        row["province_name"],
                        row["ward_id"],
                        row["ward_name"],
                        row["data_full_count"],
                        row["data_no_full_count"],
                        row["total_count"],
                    )
                    for row in rows
                ],
            )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def write_pretty(rows):
    headers = [
        "province_id",
        "province_name",
        "ward_id",
        "ward_name",
        "data_full_count",
        "data_no_full_count",
        "total_count",
    ]
    string_rows = [headers]
    for row in rows:
        string_rows.append(
            [
                str(row["province_id"]),
                str(row["province_name"]),
                str(row["ward_id"]),
                str(row["ward_name"]),
                str(row["data_full_count"]),
                str(row["data_no_full_count"]),
                str(row["total_count"]),
            ]
        )

    widths = [0] * len(headers)
    for row in string_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        for idx, row in enumerate(string_rows):
            f.write(" | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) + "\n")
            if idx == 0:
                f.write("-+-".join("-" * widths[i] for i in range(len(widths))) + "\n")


def main():
    db = Database()
    rows = fetch_rows(db)
    sync_count_table(db, rows)
    write_pretty(rows)
    logger.info("Wrote %s rows to %s", len(rows), OUTPUT_PATH)
    logger.info("Synced table %s", COUNT_TABLE)


if __name__ == "__main__":
    main()
