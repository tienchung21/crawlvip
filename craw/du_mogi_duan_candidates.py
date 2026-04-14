#!/usr/bin/env python3
"""
Generate candidate matches for du_mogi (Mogi projects) -> duan (Cafeland master table),
store into du_mogi_duan_candidates for manual review.

This is inspired by the duan<->chotot matching script you pasted:
- exact (case-insensitive)
- normalized (remove accents/special chars)
- prefix/suffix containment
- similarity (SequenceMatcher), auto-confirm if >= min_auto
- potential if between [min_potential, min_auto)

It does NOT modify table `duan`. It only writes candidates + (optionally) pushes
auto-confirmed rows into `du_mogi_duan_merge`.

Usage:
  source venv/bin/activate
  python3 craw/du_mogi_duan_candidates.py --rebuild
  python3 craw/du_mogi_duan_candidates.py --only-unmapped --topk 5
  python3 craw/du_mogi_duan_candidates.py --min-auto 0.97 --min-potential 0.85
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402


@dataclass(frozen=True)
class Duan:
    duan_id: int
    duan_ten: str
    duan_title: str
    duan_title_no: str
    variants_norm: Tuple[str, ...]
    variants_tokens: Tuple[Tuple[str, ...], ...]


_RE_NON_ALNUM = re.compile(r"[^a-z0-9\s]+")
_RE_SPACES = re.compile(r"\s+")


def remove_accents(s: str) -> str:
    if not s:
        return ""
    s = str(s).replace("đ", "d").replace("Đ", "d")
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def normalize(s: str) -> str:
    if not s:
        return ""
    s = remove_accents(s).lower()
    s = s.replace("’", "'").replace("`", "'")
    s = _RE_NON_ALNUM.sub(" ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    s = re.sub(r"\bthe\b", " ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    return s


def duan_title_aliases(title: str) -> Set[str]:
    out: Set[str] = set()
    if not title:
        return out
    t = title.strip()
    if not t:
        return out

    if ":" in t:
        left, right = t.split(":", 1)
        left = left.strip()
        right = right.strip()
        if len(left) >= 3:
            out.add(left)
        right2 = re.split(r"\b(tai|tại|o|ở)\b", right, flags=re.I)[0].strip()
        if len(right2) >= 3:
            out.add(right2)

    if " - " in t:
        left = t.split(" - ", 1)[0].strip()
        if len(left) >= 3:
            out.add(left)
    return out


def tokenize(norm: str) -> Tuple[str, ...]:
    if not norm:
        return tuple()
    stop = {
        "khu",
        "do",
        "thi",
        "moi",
        "du",
        "an",
        "dan",
        "cu",
        "kdc",
        "tt",
        "tp",
        "quan",
        "phuong",
        "xa",
        "huyen",
        "tinh",
        "city",
        "park",
        "tower",
        "plaza",
        "center",
        "central",
        "garden",
        "view",
        "home",
        "homes",
        "residence",
        "residences",
        "village",
        "complex",
    }
    toks = [t for t in norm.split(" ") if len(t) >= 2 and t not in stop]
    return tuple(sorted(set(toks)))


def ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def ensure_candidates_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS du_mogi_duan_candidates")
        cur.execute(
            """
            CREATE TABLE du_mogi_duan_candidates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                du_mogi_id INT NOT NULL,
                du_mogi_name VARCHAR(512) NOT NULL,
                duan_id INT NULL,
                duan_ten VARCHAR(250) NULL,
                duan_title VARCHAR(250) NULL,
                match_type ENUM(
                    'exact','normalized','prefix','suffix','similar_95','potential','no_match'
                ) NOT NULL,
                similarity DECIMAL(5,2) DEFAULT NULL,
                confirmed TINYINT(1) DEFAULT 0,
                notes TEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_pair (du_mogi_id, duan_id),
                INDEX idx_du_mogi_id (du_mogi_id),
                INDEX idx_duan_id (duan_id),
                INDEX idx_match_type (match_type),
                INDEX idx_confirmed (confirmed),
                INDEX idx_similarity (similarity)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        try:
            cur.close()
        finally:
            conn.close()


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


def load_du_mogi(db: Database, limit: int = 0, only_unmapped: bool = False) -> List[Tuple[int, str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if only_unmapped:
            sql = """
                SELECT dm.id, dm.project_name
                FROM du_mogi dm
                LEFT JOIN du_mogi_duan_merge m ON m.du_mogi_id = dm.id
                WHERE m.du_mogi_id IS NULL
                ORDER BY dm.id
            """
        else:
            sql = "SELECT id, project_name FROM du_mogi ORDER BY id"
        if limit and limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        out: List[Tuple[int, str]] = []
        for r in rows:
            out.append((int(r["id"]), r["project_name"] or ""))
        return out
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def load_duan(db: Database) -> List[Duan]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT duan_id, duan_ten, duan_title, duan_title_no
            FROM duan
            WHERE duan_ten IS NOT NULL AND duan_ten <> ''
            """
        )
        rows = cur.fetchall()
        out: List[Duan] = []
        for r in rows:
            duan_id = int(r["duan_id"])
            ten = r.get("duan_ten") or ""
            title = r.get("duan_title") or ""
            slug = r.get("duan_title_no") or ""

            variants: Set[str] = set()
            if ten:
                variants.add(normalize(ten))
            for a in duan_title_aliases(title):
                variants.add(normalize(a))
            if slug:
                variants.add(normalize(slug.replace("-", " ")))
            variants = {v for v in variants if v}
            vnorm = tuple(sorted(variants))
            vtoks = tuple(tokenize(v) for v in vnorm) if vnorm else tuple()
            out.append(
                Duan(
                    duan_id=duan_id,
                    duan_ten=ten,
                    duan_title=title,
                    duan_title_no=slug,
                    variants_norm=vnorm,
                    variants_tokens=vtoks,
                )
            )
        return out
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def build_norm_lookup(duans: Sequence[Duan]) -> Dict[str, Duan]:
    # norm -> Duan (pick max duan_id if collision)
    m: Dict[str, Duan] = {}
    for d in duans:
        for nn in d.variants_norm:
            prev = m.get(nn)
            if not prev or d.duan_id > prev.duan_id:
                m[nn] = d
    return m


def build_token_index(duans: Sequence[Duan]) -> Dict[str, List[int]]:
    idx_sets: Dict[str, Set[int]] = {}
    for i, d in enumerate(duans):
        all_tokens: Set[str] = set()
        for toks in d.variants_tokens:
            all_tokens.update(toks)
        for t in all_tokens:
            idx_sets.setdefault(t, set()).add(i)
    return {t: sorted(list(s)) for t, s in idx_sets.items()}


def best_candidates(
    mogi_norm: str,
    mogi_tokens: Tuple[str, ...],
    duans: Sequence[Duan],
    token_index: Dict[str, List[int]],
    topk: int,
    min_token_hits: int,
) -> List[Tuple[Duan, float, str]]:
    if not mogi_norm:
        return []

    counts: Dict[int, int] = {}
    for t in mogi_tokens:
        for i in token_index.get(t, []):
            counts[i] = counts.get(i, 0) + 1

    hit_th = min_token_hits
    if len(mogi_tokens) <= 2:
        hit_th = 1

    cand_idx = [i for i, c in counts.items() if c >= hit_th]
    if not cand_idx:
        return []

    scored: List[Tuple[Duan, float, str]] = []
    for i in cand_idx:
        d = duans[i]
        best_r = 0.0
        best_v = ""
        for vn in d.variants_norm:
            rr = ratio(mogi_norm, vn)
            if rr > best_r:
                best_r = rr
                best_v = vn
        # containment bonus
        if len(mogi_norm) >= 8 and any((mogi_norm in vn) or (vn in mogi_norm) for vn in d.variants_norm):
            best_r = min(1.0, best_r + 0.03)
        scored.append((d, best_r, best_v))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:topk]


def insert_candidates(
    db: Database,
    rows: List[Tuple[int, str, Optional[int], Optional[str], Optional[str], str, Optional[float], int, str]],
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
            INSERT INTO du_mogi_duan_candidates
                (du_mogi_id, du_mogi_name, duan_id, duan_ten, duan_title, match_type, similarity, confirmed, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                match_type=VALUES(match_type),
                similarity=VALUES(similarity),
                confirmed=VALUES(confirmed),
                notes=VALUES(notes),
                duan_ten=VALUES(duan_ten),
                duan_title=VALUES(duan_title)
            """,
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def upsert_merge(
    db: Database,
    rows: List[Tuple[int, str, int, str, str, float]],
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    ensure_merge_table(db)
    conn = db.get_connection()
    cur = conn.cursor()
    try:
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
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate candidates for du_mogi -> duan (manual review flow)")
    ap.add_argument("--rebuild", action="store_true", help="Drop+create du_mogi_duan_candidates")
    ap.add_argument("--dry-run", action="store_true", help="No DB writes")
    ap.add_argument("--limit", type=int, default=0, help="Limit du_mogi rows (0=all)")
    ap.add_argument("--only-unmapped", action="store_true", help="Only du_mogi not in du_mogi_duan_merge")
    ap.add_argument("--topk", type=int, default=5, help="Top K candidates per du_mogi")
    ap.add_argument("--min-auto", type=float, default=0.95, help="Auto-confirm if similarity >= this")
    ap.add_argument("--min-potential", type=float, default=0.80, help="Potential if similarity >= this")
    ap.add_argument("--min-token-hits", type=int, default=2, help="Token overlap threshold for shortlist")
    args = ap.parse_args()

    db = Database()
    if args.rebuild:
        ensure_candidates_table(db)
    else:
        # Ensure table exists (without dropping).
        conn = db.get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'du_mogi_duan_candidates'")
            if not cur.fetchone():
                ensure_candidates_table(db)
        finally:
            try:
                cur.close()
            finally:
                conn.close()

    t0 = time.time()
    duans = load_duan(db)
    norm_lookup = build_norm_lookup(duans)
    token_index = build_token_index(duans)
    du_mogis = load_du_mogi(db, limit=args.limit, only_unmapped=args.only_unmapped)

    counts = {
        "exact": 0,
        "normalized": 0,
        "prefix": 0,
        "suffix": 0,
        "similar_95": 0,
        "potential": 0,
        "no_match": 0,
        "auto_merge": 0,
    }

    cand_batch: List[
        Tuple[int, str, Optional[int], Optional[str], Optional[str], str, Optional[float], int, str]
    ] = []
    merge_batch: List[Tuple[int, str, int, str, str, float]] = []

    for mogi_id, mogi_name in du_mogis:
        name = (mogi_name or "").strip()
        if not name:
            continue

        name_lower = name.lower().strip()

        # METHOD 1: exact (case-insensitive) against duan_ten only
        # Note: for performance, we only use normalized lookup. Exact CI on all duans is expensive.
        # We'll treat "exact" as normalized exact with same original string below.

        # METHOD 2: normalized match (duan_ten or alias or slug)
        n = normalize(name)
        d = norm_lookup.get(n)
        if d:
            # Decide match_type: if exact CI match duan_ten -> exact else normalized
            mt = "normalized"
            if d.duan_ten and d.duan_ten.lower().strip() == name_lower:
                mt = "exact"
                counts["exact"] += 1
            else:
                counts["normalized"] += 1
            cand_batch.append(
                (
                    mogi_id,
                    name,
                    d.duan_id,
                    d.duan_ten,
                    d.duan_title,
                    mt,
                    100.0,
                    1,
                    "Normalized exact (ten/alias/slug)",
                )
            )
            merge_batch.append((mogi_id, name, d.duan_id, d.duan_ten, mt, 1.0))
            counts["auto_merge"] += 1
            continue

        # METHOD 3/4: prefix/suffix via variant norms (approx; uses candidate shortlist)
        mogi_tokens = tokenize(n)
        cands = best_candidates(
            mogi_norm=n,
            mogi_tokens=mogi_tokens,
            duans=duans,
            token_index=token_index,
            topk=args.topk,
            min_token_hits=args.min_token_hits,
        )

        if not cands:
            cand_batch.append((mogi_id, name, None, None, None, "no_match", 0.0, 0, "No candidates"))
            counts["no_match"] += 1
        else:
            best_d, best_r, best_v = cands[0]

            # prefix/suffix classification using best variant
            mt_best = None
            if best_v and best_v.startswith(n + " "):
                mt_best = "prefix"
            elif best_v and n.startswith(best_v + " "):
                mt_best = "suffix"

            if best_r >= args.min_auto:
                mt = "similar_95" if best_r >= 0.95 else (mt_best or "similar_95")
                counts["similar_95"] += 1
                cand_batch.append(
                    (
                        mogi_id,
                        name,
                        best_d.duan_id,
                        best_d.duan_ten,
                        best_d.duan_title,
                        mt,
                        round(best_r * 100.0, 2),
                        1,
                        f'Auto-confirm {best_r:.2f} via "{best_v}"',
                    )
                )
                merge_batch.append((mogi_id, name, best_d.duan_id, best_d.duan_ten, mt, float(best_r)))
                counts["auto_merge"] += 1
            elif best_r >= args.min_potential:
                counts["potential"] += 1
                # insert topk as candidates
                for d2, r2, v2 in cands:
                    cand_batch.append(
                        (
                            mogi_id,
                            name,
                            d2.duan_id,
                            d2.duan_ten,
                            d2.duan_title,
                            "potential",
                            round(r2 * 100.0, 2),
                            0,
                            f'Candidate via "{v2}"',
                        )
                    )
            else:
                counts["no_match"] += 1
                cand_batch.append(
                    (
                        mogi_id,
                        name,
                        best_d.duan_id,
                        best_d.duan_ten,
                        best_d.duan_title,
                        "no_match",
                        round(best_r * 100.0, 2),
                        0,
                        f'Best guess {best_r:.2f} via "{best_v}"',
                    )
                )

        if len(cand_batch) >= 2000:
            insert_candidates(db, cand_batch, dry_run=args.dry_run)
            cand_batch.clear()

        if len(merge_batch) >= 1000:
            upsert_merge(db, merge_batch, dry_run=args.dry_run)
            merge_batch.clear()

    if cand_batch:
        insert_candidates(db, cand_batch, dry_run=args.dry_run)
    if merge_batch:
        upsert_merge(db, merge_batch, dry_run=args.dry_run)

    dur = time.time() - t0
    print("=== CANDIDATE GENERATION SUMMARY ===")
    print(f"du_mogi processed: {len(du_mogis)}")
    for k in ["exact", "normalized", "prefix", "suffix", "similar_95", "potential", "no_match"]:
        print(f"- {k}: {counts[k]}")
    print(f"- auto_merge -> du_mogi_duan_merge: {counts['auto_merge']}")
    print(f"duration: {dur:.2f}s")
    print("\nUseful queries:")
    print("  Review potential:")
    print("    SELECT * FROM du_mogi_duan_candidates WHERE match_type='potential' AND confirmed=0 ORDER BY similarity DESC;")
    print("  Confirm one:")
    print("    UPDATE du_mogi_duan_candidates SET confirmed=1 WHERE id=...;")
    print("  Apply confirmed:")
    print("    python3 craw/apply_confirmed_du_mogi_duan.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

