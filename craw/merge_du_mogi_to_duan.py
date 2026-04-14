#!/usr/bin/env python3
"""
Merge (map) du an Mogi (du_mogi) voi du an goc Cafeland (duan) theo ten.

Muc tieu:
- Tao bang mapping de join nhanh: du_mogi_duan_merge
- Match theo 3 tang:
  1) exact: du_mogi.project_name == duan.duan_ten
  2) normalized exact: bo dau, lower, bo ky tu dac biet
  3) fuzzy: shortlist theo token overlap + SequenceMatcher

Bang mapping KHONG sua bang goc 'duan'. Chi insert mapping.

Usage:
  source venv/bin/activate
  python3 craw/merge_du_mogi_to_duan.py --rebuild
  python3 craw/merge_du_mogi_to_duan.py --dry-run --limit 200
  python3 craw/merge_du_mogi_to_duan.py --min-score 0.92
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
from typing import Dict, Iterable, List, Optional, Set, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402


@dataclass(frozen=True)
class DuanRow:
    duan_id: int
    duan_ten: str
    duan_title: str
    duan_title_no: str
    norm: str
    tokens: Tuple[str, ...]
    variant_norms: Tuple[str, ...]
    variant_tokens: Tuple[Tuple[str, ...], ...]


def remove_accents(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    s = s.replace("đ", "d").replace("Đ", "d")
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


_RE_NON_ALNUM = re.compile(r"[^a-z0-9\s]+")
_RE_SPACES = re.compile(r"\s+")


def normalize_name(s: str) -> str:
    # Normalize for matching, keep ASCII.
    s = remove_accents(s).lower()
    s = s.replace("’", "'").replace("`", "'")
    s = _RE_NON_ALNUM.sub(" ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    # Remove some very common noise tokens (optional).
    s = re.sub(r"\bthe\b", " ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    return s


def tokenize(norm: str) -> Tuple[str, ...]:
    if not norm:
        return tuple()
    stop = {
        # Viet noise
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
        # Eng noise
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
    # Dedup, stable-ish order (sort).
    return tuple(sorted(set(toks)))

def duan_title_aliases(title: str) -> Set[str]:
    """
    Rút alias từ duan_title kiểu:
    - "Tên dự án: mô tả..."
    - "Tên dự án - mô tả..."
    - "Tên dự án: Dự án ... tại ..."
    """
    out: Set[str] = set()
    if not title:
        return out
    t = title.strip()
    if not t:
        return out

    # Tách theo ":" (lấy cả vế trái và vế phải, vì nhiều title dạng "Alias: Tên đầy đủ ...")
    if ":" in t:
        left, right = t.split(":", 1)
        left = left.strip()
        right = right.strip()
        if len(left) >= 3:
            out.add(left)
        # lấy cụm đầu tiên ở vế phải trước "tại/ở"
        right2 = re.split(r"\b(tai|tại|o|ở)\b", right, flags=re.I)[0].strip()
        if len(right2) >= 3:
            out.add(right2)

    # Tách theo " - "
    if " - " in t:
        left = t.split(" - ", 1)[0].strip()
        if len(left) >= 3:
            out.add(left)
    return out


def build_variant_norms(duan_ten: str, duan_title: str, duan_title_no: str) -> Tuple[str, ...]:
    variants: Set[str] = set()
    if duan_ten:
        variants.add(normalize_name(duan_ten))

    # alias từ title
    for a in duan_title_aliases(duan_title or ""):
        na = normalize_name(a)
        if na:
            variants.add(na)

    # slug (duan_title_no) thường dạng "abc-def-ghi"
    if duan_title_no:
        slug = duan_title_no.replace("-", " ")
        ns = normalize_name(slug)
        if ns:
            variants.add(ns)

    # bỏ rỗng
    variants = {v for v in variants if v}
    return tuple(sorted(variants))


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


def fetch_du_mogi(db: Database, limit: int = 0) -> List[Tuple[int, str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        sql = "SELECT id, project_name FROM du_mogi ORDER BY id"
        if limit and limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        out: List[Tuple[int, str]] = []
        for r in rows:
            if isinstance(r, dict):
                out.append((int(r["id"]), r["project_name"]))
            else:
                out.append((int(r[0]), r[1]))
        return out
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def fetch_duan(db: Database) -> List[DuanRow]:
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
        out: List[DuanRow] = []
        for r in rows:
            if isinstance(r, dict):
                duan_id = int(r["duan_id"])
                duan_ten = r.get("duan_ten") or ""
                duan_title = r.get("duan_title") or ""
                duan_title_no = r.get("duan_title_no") or ""
            else:
                duan_id = int(r[0])
                duan_ten = r[1] or ""
                duan_title = r[2] or ""
                duan_title_no = r[3] or ""
            n = normalize_name(duan_ten)
            vnorms = build_variant_norms(duan_ten, duan_title, duan_title_no)
            vtoks = tuple(tokenize(vn) for vn in vnorms) if vnorms else (tokenize(n),)
            out.append(
                DuanRow(
                    duan_id=duan_id,
                    duan_ten=duan_ten,
                    duan_title=duan_title,
                    duan_title_no=duan_title_no,
                    norm=n,
                    tokens=tokenize(n),
                    variant_norms=vnorms,
                    variant_tokens=vtoks,
                )
            )
        return out
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def build_token_index(duans: Iterable[DuanRow]) -> Dict[str, List[int]]:
    # token -> set(index) to avoid duplicates (important for performance)
    idx_sets: Dict[str, Set[int]] = {}
    for i, d in enumerate(duans):
        # index tokens từ tất cả variant_tokens để shortlist rộng hơn
        all_tokens: Set[str] = set()
        for toks in d.variant_tokens or (d.tokens,):
            all_tokens.update(toks)
        for t in all_tokens:
            idx_sets.setdefault(t, set()).add(i)
    return {t: sorted(list(s)) for t, s in idx_sets.items()}


def seq_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def best_fuzzy_match(
    mogi_norm: str,
    mogi_tokens: Tuple[str, ...],
    duans: List[DuanRow],
    token_index: Dict[str, List[int]],
    min_token_hits: int = 2,
) -> Optional[Tuple[DuanRow, float]]:
    if not mogi_norm:
        return None

    # Candidate shortlist via token overlap.
    cand_counts: Dict[int, int] = {}
    for t in mogi_tokens:
        for idx in token_index.get(t, []):
            cand_counts[idx] = cand_counts.get(idx, 0) + 1

    # If very short name -> broaden a bit (allow 1 token hit).
    token_hit_threshold = min_token_hits
    if len(mogi_tokens) <= 2:
        token_hit_threshold = 1

    cands = [i for i, c in cand_counts.items() if c >= token_hit_threshold]
    if not cands:
        return None

    best_i = None
    best_score = 0.0
    for i in cands:
        d = duans[i]
        # Combine char similarity with token Jaccard.
        # Evaluate against the best variant_norm (ten/alias/slug).
        variants = d.variant_norms or (d.norm,)
        variant_tokens = d.variant_tokens or (d.tokens,)
        r_best = 0.0
        best_variant_tokens: Tuple[str, ...] = d.tokens
        for vn, vt in zip(variants, variant_tokens):
            rr = seq_ratio(mogi_norm, vn)
            if rr > r_best:
                r_best = rr
                best_variant_tokens = vt

        # containment bonus: nếu 1 chuỗi chứa chuỗi kia và đủ dài -> boost
        contain_bonus = 0.0
        if len(mogi_norm) >= 8 and len(best_variant_tokens) >= 2:
            if any((mogi_norm in vn) or (vn in mogi_norm) for vn in variants if vn):
                contain_bonus = 0.05

        if mogi_tokens and d.tokens:
            inter = len(set(mogi_tokens).intersection(best_variant_tokens))
            union = len(set(mogi_tokens).union(best_variant_tokens))
            j = inter / union if union else 0.0
        else:
            j = 0.0
        score = min(1.0, 0.7 * r_best + 0.3 * j + contain_bonus)
        if score > best_score:
            best_score = score
            best_i = i

    if best_i is None:
        return None
    return duans[best_i], best_score


def insert_mappings(
    db: Database,
    rows: List[Tuple[int, str, int, str, str, Optional[float]]],
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
            INSERT INTO du_mogi_duan_merge
                (du_mogi_id, du_mogi_name, duan_id, duan_ten, match_type, score)
            VALUES (%s, %s, %s, %s, %s, %s)
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


def truncate_merge_table(db: Database, dry_run: bool) -> None:
    if dry_run:
        return
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE du_mogi_duan_merge")
        conn.commit()
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge du_mogi -> duan (Cafeland) by name")
    parser.add_argument("--dry-run", action="store_true", help="Khong ghi DB, chi in thong ke")
    parser.add_argument("--rebuild", action="store_true", help="TRUNCATE bang mapping truoc khi merge")
    parser.add_argument("--limit", type=int, default=0, help="Gioi han so dong du_mogi de test (0=all)")
    parser.add_argument("--min-score", type=float, default=0.90, help="Nguong score cho fuzzy")
    parser.add_argument("--min-token-hits", type=int, default=2, help="So token overlap toi thieu de shortlist")
    parser.add_argument("--print-samples", type=int, default=10, help="In N dong sample unmatched")
    args = parser.parse_args()

    db = Database()
    ensure_merge_table(db)
    if args.rebuild:
        truncate_merge_table(db, dry_run=args.dry_run)

    t0 = time.time()
    duans = fetch_duan(db)
    du_mogis = fetch_du_mogi(db, limit=args.limit)

    # Index for exact / normalized exact.
    exact_by_ten: Dict[str, DuanRow] = {}
    # norm -> DuanRow (ưu tiên duan_id lớn hơn nếu trùng norm)
    norm_to_duan: Dict[str, DuanRow] = {}
    for d in duans:
        if d.duan_ten and d.duan_ten not in exact_by_ten:
            exact_by_ten[d.duan_ten] = d
        # map tất cả variant_norms (ten + alias + slug)
        for nn in d.variant_norms or (d.norm,):
            if not nn:
                continue
            prev = norm_to_duan.get(nn)
            if not prev or d.duan_id > prev.duan_id:
                norm_to_duan[nn] = d

    token_index = build_token_index(duans)

    mapped = 0
    mapped_exact = 0
    mapped_norm = 0
    mapped_fuzzy = 0

    batch: List[Tuple[int, str, int, str, str, Optional[float]]] = []
    unmatched: List[Tuple[int, str]] = []

    for mogi_id, mogi_name in du_mogis:
        mogi_name = mogi_name or ""
        if not mogi_name:
            continue

        # 1) exact duan_ten
        d = exact_by_ten.get(mogi_name)
        if d:
            batch.append((mogi_id, mogi_name, d.duan_id, d.duan_ten, "exact", 1.0))
            mapped += 1
            mapped_exact += 1
            continue

        # 2) normalized exact
        n = normalize_name(mogi_name)
        d = norm_to_duan.get(n)
        if d:
            batch.append((mogi_id, mogi_name, d.duan_id, d.duan_ten, "norm_exact", 1.0))
            mapped += 1
            mapped_norm += 1
            continue

        # 3) fuzzy
        toks = tokenize(n)
        best = best_fuzzy_match(
            mogi_norm=n,
            mogi_tokens=toks,
            duans=duans,
            token_index=token_index,
            min_token_hits=args.min_token_hits,
        )
        if best:
            bd, score = best
            if score >= args.min_score:
                batch.append((mogi_id, mogi_name, bd.duan_id, bd.duan_ten, "fuzzy", float(score)))
                mapped += 1
                mapped_fuzzy += 1
            else:
                unmatched.append((mogi_id, mogi_name))
        else:
            unmatched.append((mogi_id, mogi_name))

        # Flush batch to avoid huge memory.
        if len(batch) >= 1000:
            insert_mappings(db, batch, dry_run=args.dry_run)
            batch.clear()

    if batch:
        insert_mappings(db, batch, dry_run=args.dry_run)

    dur = time.time() - t0
    total = len(du_mogis)
    print("=== MERGE SUMMARY ===")
    print(f"du_mogi total: {total}")
    print(f"mapped: {mapped} ({mapped/total*100:.2f}%)")
    print(f"- exact: {mapped_exact}")
    print(f"- norm_exact: {mapped_norm}")
    print(f"- fuzzy: {mapped_fuzzy} (min_score={args.min_score})")
    print(f"unmatched: {len(unmatched)}")
    print(f"duration: {dur:.2f}s")

    if unmatched and args.print_samples > 0:
        print(f"\n=== SAMPLE UNMATCHED (first {args.print_samples}) ===")
        for mogi_id, name in unmatched[: args.print_samples]:
            print(f"- du_mogi.id={mogi_id} name={name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
