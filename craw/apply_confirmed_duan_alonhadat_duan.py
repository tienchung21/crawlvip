#!/usr/bin/env python3
"""
Apply manually confirmed candidates from duan_alonhadat_duan_candidates
into duan_alonhadat_duan_merge.

Safe mode:
- only apply rows where alonhadat_project_name resolves to exactly 1 row in duan_alonhadat
- and duan_ten resolves to exactly 1 row in duan
- ambiguous rows are skipped and printed
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402
from merge_duan_alonhadat_to_duan import ensure_merge_table  # noqa: E402


def load_confirmed_candidates(db: Database, limit: int = 0) -> List[dict]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        sql = """
            SELECT id, alonhadat_project_name, duan_ten, similarity
            FROM duan_alonhadat_duan_candidates
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
            SELECT id, project_name, address, detail_url, slug
            FROM duan_alonhadat
            WHERE project_name IS NOT NULL AND project_name <> ''
            """
        )
        alo_rows = cur.fetchall()
        alo_by_name: Dict[str, List[dict]] = {}
        for r in alo_rows:
            alo_by_name.setdefault(r["project_name"], []).append(r)

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

        return alo_by_name, duan_by_name
    finally:
        cur.close()
        conn.close()


def apply_rows(db: Database, rows: List[Tuple[int, str, str, str, str, int, str, int, str, int, str, float]], dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO duan_alonhadat_duan_merge
                (alonhadat_project_id, alonhadat_project_name, alonhadat_address, alonhadat_detail_url, alonhadat_slug,
                 alonhadat_province_id, alonhadat_province_name, duan_id, duan_ten, duan_tinh_moi, match_type, score)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                alonhadat_project_name=VALUES(alonhadat_project_name),
                alonhadat_address=VALUES(alonhadat_address),
                alonhadat_detail_url=VALUES(alonhadat_detail_url),
                alonhadat_slug=VALUES(alonhadat_slug),
                alonhadat_province_id=VALUES(alonhadat_province_id),
                alonhadat_province_name=VALUES(alonhadat_province_name),
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
    ap = argparse.ArgumentParser(description="Apply confirmed duan_alonhadat candidates into merge table")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    db = Database()
    ensure_merge_table(db)

    confirmed = load_confirmed_candidates(db, limit=args.limit)
    if not confirmed:
        print("No confirmed rows to apply.")
        return 0

    alo_by_name, duan_by_name = build_name_indexes(db)

    payload: List[Tuple[int, str, str, str, str, int, str, int, str, int, str, float]] = []
    skipped: List[Tuple[int, str, str, int, int]] = []

    for row in confirmed:
        alo_matches = alo_by_name.get(row["alonhadat_project_name"], [])
        duan_matches = duan_by_name.get(row["duan_ten"], [])
        if len(alo_matches) != 1 or len(duan_matches) != 1:
            skipped.append((row["id"], row["alonhadat_project_name"], row["duan_ten"], len(alo_matches), len(duan_matches)))
            continue

        alo = alo_matches[0]
        duan = duan_matches[0]
        sim = row.get("similarity")
        score = float(sim) / 100.0 if sim is not None else 1.0
        payload.append(
            (
                int(alo["id"]),
                alo["project_name"],
                alo.get("address") or "",
                alo.get("detail_url") or "",
                alo.get("slug") or "",
                None,
                "",
                int(duan["duan_id"]),
                duan["duan_ten"],
                int(duan["duan_tinh_moi"]) if duan.get("duan_tinh_moi") else None,
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
            cid, alo_name, duan_ten, alo_cnt, duan_cnt = item
            print(f"- candidate_id={cid} | alo_cnt={alo_cnt} | duan_cnt={duan_cnt} | {alo_name} -> {duan_ten}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
