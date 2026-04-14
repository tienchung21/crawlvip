#!/usr/bin/env python3
import argparse
import time

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


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill data_clean_v1.std_month from std_date in small batches")
    parser.add_argument("--batch", type=int, default=5000, help="Rows per batch")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between batches")
    parser.add_argument("--max-batches", type=int, default=0, help="Stop after N batches; 0 = run until done")
    args = parser.parse_args()

    if args.batch <= 0:
        raise SystemExit("--batch must be > 0")
    if args.sleep < 0:
        raise SystemExit("--sleep must be >= 0")
    if args.max_batches < 0:
        raise SystemExit("--max-batches must be >= 0")

    conn = pymysql.connect(**DB_CONFIG)
    total_updated = 0
    batch_no = 0

    try:
        while True:
            batch_no += 1
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE data_clean_v1 d
                    JOIN (
                        SELECT id
                        FROM data_clean_v1
                        WHERE std_month IS NULL
                          AND std_date IS NOT NULL
                        ORDER BY id
                        LIMIT %s
                    ) x ON x.id = d.id
                    SET d.std_month = DATE_FORMAT(d.std_date, '%%Y-%%m')
                    """,
                    (args.batch,),
                )
                updated = cur.rowcount
            conn.commit()

            total_updated += updated
            print(f"batch={batch_no} updated={updated} total_updated={total_updated}", flush=True)

            if updated == 0:
                break
            if args.max_batches and batch_no >= args.max_batches:
                break
            if args.sleep:
                time.sleep(args.sleep)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(std_month IS NULL AND std_date IS NOT NULL) AS need_fill,
                    SUM(std_month IS NULL AND std_date IS NULL) AS no_date
                FROM data_clean_v1
                """
            )
            summary = cur.fetchone()
        print(
            f"done total={summary['total']} need_fill={summary['need_fill']} no_date={summary['no_date']}",
            flush=True,
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
