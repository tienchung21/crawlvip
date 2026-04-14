#!/usr/bin/env python3
"""
Apply manually confirmed candidates from du_mogi_duan_candidates -> du_mogi_duan_merge.

Flow:
1) Generate candidates:
   python3 craw/du_mogi_duan_candidates.py --rebuild --only-unmapped
2) Review + confirm in SQL:
   UPDATE du_mogi_duan_candidates SET confirmed=1 WHERE id=...;
3) Apply:
   python3 craw/apply_confirmed_du_mogi_duan.py
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402


def ensure_merge_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS du_mogi_duan_merge (
                du_mogi_id INT NOT NULL,
                du_mogi_name VARCHAR(512) NOT NULL,
                duan_id INT NOT NULL,
                duan_ten VARCHAR(250) NULL,
                match_type VARCHAR(32) NOT NULL,
                score DOUBLE NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (du_mogi_id),
                INDEX idx_duan_id (duan_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply confirmed du_mogi_duan_candidates into du_mogi_duan_merge")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    db = Database()
    ensure_merge_table(db)

    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE 'du_mogi_duan_candidates'")
        if not cur.fetchone():
            print("Missing table: du_mogi_duan_candidates")
            return 2

        sql = """
            SELECT
                du_mogi_id,
                du_mogi_name,
                duan_id,
                duan_ten,
                match_type,
                similarity
            FROM du_mogi_duan_candidates
            WHERE confirmed=1 AND duan_id IS NOT NULL
            ORDER BY updated_at DESC
        """
        if args.limit and args.limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (args.limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()

        payload: List[Tuple[int, str, int, str, str, float]] = []
        for r in rows:
            sim = r.get("similarity")
            score = float(sim) / 100.0 if sim is not None else 1.0
            payload.append(
                (
                    int(r["du_mogi_id"]),
                    r["du_mogi_name"],
                    int(r["duan_id"]),
                    r.get("duan_ten"),
                    f"manual_{r.get('match_type') or 'confirmed'}",
                    score,
                )
            )

        if not payload:
            print("No confirmed rows to apply.")
            return 0

        if args.dry_run:
            print(f"Would apply: {len(payload)} rows")
            return 0

        cur.executemany(
            """
            INSERT INTO du_mogi_duan_merge
                (du_mogi_id, du_mogi_name, duan_id, duan_ten, match_type, score)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                duan_id=VALUES(duan_id),
                duan_ten=VALUES(duan_ten),
                match_type=VALUES(match_type),
                score=VALUES(score)
            """,
            payload,
        )
        conn.commit()
        print(f"Applied: {len(payload)} rows into du_mogi_duan_merge")
        return 0
    finally:
        try:
            cur.close()
        finally:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

