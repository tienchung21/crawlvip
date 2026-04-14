#!/usr/bin/env python3
"""
Apply manually confirmed candidates from duan_guland_duan_candidates
into duan_guland_duan_merge.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402
from merge_duan_guland_to_duan import ensure_merge_table  # noqa: E402


def load_confirmed_candidates(db: Database, limit: int = 0) -> List[dict]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        sql = """
            SELECT id, guland_project_name, duan_ten, similarity
            FROM duan_guland_duan_candidates
            WHERE confirmed = 1
            ORDER BY updated_at DESC, id DESC
        """
        if limit and limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        return list(cur.fetchall())
    finally:
        cur.close()
        conn.close()


def build_name_indexes(db: Database) -> Tuple[Dict[str, List[dict]], Dict[str, List[dict]]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, project_name, project_url
            FROM duan_guland
            WHERE project_name IS NOT NULL AND project_name <> ''
            """
        )
        guland_rows = cur.fetchall()
        guland_by_name: Dict[str, List[dict]] = {}
        for r in guland_rows:
            guland_by_name.setdefault(r["project_name"], []).append(r)

        cur.execute(
            """
            SELECT duan_id, duan_ten, duan_tinh_moi
            FROM duan
            WHERE duan_ten IS NOT NULL AND duan_ten <> ''
            """
        )
        duan_rows = cur.fetchall()
        duan_by_name: Dict[str, List[dict]] = {}
        for r in duan_rows:
            duan_by_name.setdefault(r["duan_ten"], []).append(r)

        return guland_by_name, duan_by_name
    finally:
        cur.close()
        conn.close()


def apply_rows(db: Database, rows: List[Tuple[int, str, str, int, str, int, str, float]], dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO duan_guland_duan_merge
                (guland_project_id, guland_project_name, guland_project_url, duan_id, duan_ten, duan_tinh_moi, match_type, score)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                guland_project_name=VALUES(guland_project_name),
                guland_project_url=VALUES(guland_project_url),
                duan_id=VALUES(duan_id),
                duan_ten=VALUES(duan_ten),
                duan_tinh_moi=VALUES(duan_tinh_moi),
                match_type=VALUES(match_type),
                score=VALUES(score)
            """,
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        cur.close()
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply confirmed duan_guland candidates into merge table")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    db = Database()
    ensure_merge_table(db)

    confirmed = load_confirmed_candidates(db, limit=args.limit)
    if not confirmed:
        print("No confirmed rows to apply.")
        return 0

    guland_by_name, duan_by_name = build_name_indexes(db)

    payload: List[Tuple[int, str, str, int, str, int, str, float]] = []
    skipped: List[Tuple[int, str, str, int, int]] = []
    for row in confirmed:
        guland_matches = guland_by_name.get(row["guland_project_name"], [])
        duan_matches = duan_by_name.get(row["duan_ten"], [])
        if len(guland_matches) != 1 or len(duan_matches) != 1:
            skipped.append((row["id"], row["guland_project_name"], row["duan_ten"], len(guland_matches), len(duan_matches)))
            continue
        g = guland_matches[0]
        d = duan_matches[0]
        sim = row.get("similarity")
        score = float(sim) / 100.0 if sim is not None else 1.0
        payload.append(
            (
                int(g["id"]),
                g["project_name"],
                g.get("project_url") or "",
                int(d["duan_id"]),
                d["duan_ten"],
                int(d["duan_tinh_moi"]) if d.get("duan_tinh_moi") else None,
                "manual_confirmed",
                score,
            )
        )

    applied = apply_rows(db, payload, dry_run=args.dry_run)
    print(f"Confirmed rows: {len(confirmed)}")
    print(f"Applied safely: {applied}")
    print(f"Skipped ambiguous: {len(skipped)}")
    if skipped:
        print("Sample skipped:")
        for item in skipped[:20]:
            cid, g_name, duan_ten, g_cnt, d_cnt = item
            print(f"- candidate_id={cid} | guland_cnt={g_cnt} | duan_cnt={d_cnt} | {g_name} -> {duan_ten}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
