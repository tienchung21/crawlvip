#!/usr/bin/env python3
"""
Generate candidate matches for duan_guland -> duan for manual review.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Optional, Sequence, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402
from merge_duan_guland_to_duan import (  # noqa: E402
    DuanRow,
    build_token_index,
    ensure_merge_table,
    extract_trailing_number,
    fetch_duan,
    seq_ratio,
    strip_project_prefixes,
    tokenize,
    normalize_name,
)


def ensure_candidates_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS duan_guland_duan_candidates")
        cur.execute(
            """
            CREATE TABLE duan_guland_duan_candidates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                guland_project_name VARCHAR(500) NOT NULL,
                ten_rut_gon VARCHAR(500) NULL,
                duan_ten VARCHAR(250) NULL,
                similarity DECIMAL(5,2) DEFAULT NULL,
                confirmed TINYINT(1) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_confirmed (confirmed),
                INDEX idx_similarity (similarity)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def load_guland(db: Database, limit: int = 0, only_unmapped: bool = False) -> List[Tuple[int, str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if only_unmapped:
            sql = """
                SELECT g.id, g.project_name
                FROM duan_guland g
                LEFT JOIN duan_guland_duan_merge m ON m.guland_project_id = g.id
                WHERE g.project_name IS NOT NULL AND g.project_name <> ''
                  AND m.guland_project_id IS NULL
                ORDER BY g.id
            """
        else:
            sql = """
                SELECT id, project_name
                FROM duan_guland
                WHERE project_name IS NOT NULL AND project_name <> ''
                ORDER BY id
            """
        if limit and limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        return [(int(r["id"]), r["project_name"] or "") for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def best_candidates(
    src_norm: str,
    src_core: str,
    src_tokens: Tuple[str, ...],
    duans: Sequence[DuanRow],
    token_index: Dict[str, List[int]],
    topk: int,
    min_token_hits: int,
) -> List[Tuple[DuanRow, float]]:
    if not src_norm:
        return []
    counts: Dict[int, int] = {}
    for t in src_tokens:
        for i in token_index.get(t, []):
            counts[i] = counts.get(i, 0) + 1
    hit_th = 1 if len(src_tokens) <= 2 else min_token_hits
    cand_idx = [i for i, c in counts.items() if c >= hit_th]
    if not cand_idx:
        return []

    scored: List[Tuple[DuanRow, float]] = []
    for i in cand_idx:
        d = duans[i]
        best_r = 0.0
        for vn in d.variant_norms:
            best_r = max(best_r, seq_ratio(src_norm, vn))
        if src_core and d.variant_core_norms:
            for vn in d.variant_core_norms:
                best_r = max(best_r, seq_ratio(src_core, vn))
        if len(src_norm) >= 8 and any((src_norm in vn) or (vn in src_norm) for vn in d.variant_norms):
            best_r = min(1.0, best_r + 0.03)
        if src_core and any((src_core in vn) or (vn in src_core) for vn in d.variant_core_norms):
            best_r = min(1.0, best_r + 0.03)
        src_num = extract_trailing_number(src_core or src_norm)
        dst_nums = {extract_trailing_number(v) for v in d.variant_norms}
        dst_nums.update({extract_trailing_number(v) for v in d.variant_core_norms})
        dst_nums.discard(None)
        if src_num and dst_nums and src_num not in dst_nums:
            best_r = max(0.0, best_r - 0.18)
        scored.append((d, best_r))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:topk]


def insert_candidates(
    db: Database,
    rows: List[Tuple[str, Optional[str], Optional[str], Optional[float], int]],
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO duan_guland_duan_candidates
                (guland_project_name, ten_rut_gon, duan_ten, similarity, confirmed)
            VALUES (%s,%s,%s,%s,%s)
            """,
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        cur.close()
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate candidates for duan_guland -> duan")
    ap.add_argument("--rebuild", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only-unmapped", action="store_true")
    ap.add_argument("--topk", type=int, default=5)
    ap.add_argument("--min-potential", type=float, default=0.85)
    ap.add_argument("--max-potential", type=float, default=0.91)
    ap.add_argument("--min-token-hits", type=int, default=2)
    args = ap.parse_args()

    db = Database()
    ensure_merge_table(db)
    if args.rebuild:
        ensure_candidates_table(db)
    else:
        conn = db.get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'duan_guland_duan_candidates'")
            if not cur.fetchone():
                ensure_candidates_table(db)
        finally:
            cur.close()
            conn.close()

    duans = fetch_duan(db)
    token_index = build_token_index(duans)
    src_rows = load_guland(db, limit=args.limit, only_unmapped=args.only_unmapped)

    counts = {"potential": 0, "no_match": 0}
    batch: List[Tuple[str, Optional[str], Optional[str], Optional[float], int]] = []

    for _, name in src_rows:
        src_norm = normalize_name(name)
        src_core = strip_project_prefixes(name)
        src_tokens = tokenize(src_norm)
        cands = best_candidates(
            src_norm=src_norm,
            src_core=src_core,
            src_tokens=src_tokens,
            duans=duans,
            token_index=token_index,
            topk=args.topk,
            min_token_hits=args.min_token_hits,
        )
        kept = 0
        for d, sim in cands:
            if sim < args.min_potential or sim >= args.max_potential:
                continue
            kept += 1
            batch.append((name, src_core or None, d.duan_ten, round(sim * 100.0, 2), 0))
        if kept:
            counts["potential"] += 1
        else:
            counts["no_match"] += 1

        if len(batch) >= 2000:
            insert_candidates(db, batch, dry_run=args.dry_run)
            batch.clear()

    if batch:
        insert_candidates(db, batch, dry_run=args.dry_run)

    print("=== CANDIDATE GENERATION SUMMARY ===")
    print(f"duan_guland processed: {len(src_rows)}")
    print(f"- potential groups: {counts['potential']}")
    print(f"- no_match groups: {counts['no_match']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
