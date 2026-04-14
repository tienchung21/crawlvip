#!/usr/bin/env python3
"""
Merge (map) du an Chotot (chotot_projects) voi du an goc Cafeland (duan) theo ten.

Muc tieu:
- Tao bang mapping de join nhanh: chotot_projects_duan_merge
- Match theo 3 tang (uu tien cung tinh/thanh):
  1) exact: project_name == duan_ten
  2) normalized exact: bo dau, lower, bo ky tu dac biet
  3) fuzzy: shortlist theo token overlap + SequenceMatcher

Bang mapping KHONG sua bang goc 'duan'. Chi insert mapping.
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
    duan_tinh_moi: Optional[int]
    duan_tinh_old: Optional[int]
    province_norm: str
    norm: str
    tokens: Tuple[str, ...]
    variant_norms: Tuple[str, ...]
    variant_tokens: Tuple[Tuple[str, ...], ...]


def remove_accents(s: str) -> str:
    if not s:
        return ""
    s = str(s).replace("đ", "d").replace("Đ", "d")
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


_RE_NON_ALNUM = re.compile(r"[^a-z0-9\s]+")
_RE_SPACES = re.compile(r"\s+")


def normalize_name(s: str) -> str:
    s = remove_accents(s or "").lower()
    s = s.replace("’", "'").replace("`", "'")
    s = _RE_NON_ALNUM.sub(" ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    s = re.sub(r"\b(the|du an|khu do thi|kdt|kdc)\b", " ", s)
    return _RE_SPACES.sub(" ", s).strip()


def tokenize(norm: str) -> Tuple[str, ...]:
    if not norm:
        return tuple()
    stop = {
        "du", "an", "khu", "do", "thi", "moi", "dan", "cu", "quan", "phuong", "xa", "huyen",
        "tinh", "tp", "thanh", "pho", "can", "ho", "chung", "cu", "nha", "o",
        "city", "park", "tower", "plaza", "center", "central", "garden", "view", "residence",
    }
    toks = [t for t in norm.split(" ") if len(t) >= 2 and t not in stop]
    return tuple(sorted(set(toks)))


def duan_title_aliases(title: str) -> Set[str]:
    out: Set[str] = set()
    t = (title or "").strip()
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


def build_variant_norms(duan_ten: str, duan_title: str, duan_title_no: str) -> Tuple[str, ...]:
    variants: Set[str] = set()
    if duan_ten:
        variants.add(normalize_name(duan_ten))
    for a in duan_title_aliases(duan_title):
        na = normalize_name(a)
        if na:
            variants.add(na)
    if duan_title_no:
        slug_norm = normalize_name((duan_title_no or "").replace("-", " "))
        if slug_norm:
            variants.add(slug_norm)
    return tuple(sorted(v for v in variants if v))


def ensure_merge_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS chotot_projects_duan_merge (
                chotot_project_id BIGINT NOT NULL,
                chotot_project_name VARCHAR(255) NOT NULL,
                chotot_region_name VARCHAR(255) NULL,
                duan_id INT NOT NULL,
                duan_ten VARCHAR(250) NULL,
                duan_tinh_moi INT NULL,
                match_type VARCHAR(32) NOT NULL,
                score DOUBLE NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chotot_project_id),
                INDEX idx_duan_id (duan_id),
                INDEX idx_region (chotot_region_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def truncate_merge_table(db: Database, dry_run: bool) -> None:
    if dry_run:
        return
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE chotot_projects_duan_merge")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def fetch_province_names(db: Database) -> Dict[int, str]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT city_id, city_title FROM transaction_city WHERE city_loai = 0")
        rows = cur.fetchall()
        out: Dict[int, str] = {}
        for r in rows:
            if isinstance(r, dict):
                out[int(r["city_id"])] = r.get("city_title") or ""
            else:
                out[int(r[0])] = r[1] or ""
        return out
    finally:
        cur.close()
        conn.close()


def fetch_duan(db: Database, province_names: Dict[int, str]) -> List[DuanRow]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT duan_id, duan_ten, duan_title, duan_title_no, duan_tinh_moi, duan_tinh
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
                tinh_moi = int(r["duan_tinh_moi"]) if r.get("duan_tinh_moi") else None
                tinh_old = int(r["duan_tinh"]) if r.get("duan_tinh") else None
            else:
                duan_id = int(r[0])
                duan_ten = r[1] or ""
                duan_title = r[2] or ""
                duan_title_no = r[3] or ""
                tinh_moi = int(r[4]) if r[4] else None
                tinh_old = int(r[5]) if r[5] else None

            province_name = ""
            if tinh_moi and tinh_moi in province_names:
                province_name = province_names[tinh_moi]
            elif tinh_old and tinh_old in province_names:
                province_name = province_names[tinh_old]

            n = normalize_name(duan_ten)
            vnorms = build_variant_norms(duan_ten, duan_title, duan_title_no)
            vtoks = tuple(tokenize(vn) for vn in vnorms) if vnorms else (tokenize(n),)
            out.append(
                DuanRow(
                    duan_id=duan_id,
                    duan_ten=duan_ten,
                    duan_title=duan_title,
                    duan_title_no=duan_title_no,
                    duan_tinh_moi=tinh_moi,
                    duan_tinh_old=tinh_old,
                    province_norm=normalize_name(province_name),
                    norm=n,
                    tokens=tokenize(n),
                    variant_norms=vnorms,
                    variant_tokens=vtoks,
                )
            )
        return out
    finally:
        cur.close()
        conn.close()


def fetch_chotot_projects(db: Database, limit: int = 0) -> List[Tuple[int, str, str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        sql = """
            SELECT id, project_name, region_name
            FROM chotot_projects
            WHERE project_name IS NOT NULL AND project_name <> ''
            ORDER BY id
        """
        if limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        out: List[Tuple[int, str, str]] = []
        for r in rows:
            if isinstance(r, dict):
                out.append((int(r["id"]), r.get("project_name") or "", r.get("region_name") or ""))
            else:
                out.append((int(r[0]), r[1] or "", r[2] or ""))
        return out
    finally:
        cur.close()
        conn.close()


def build_token_index(duans: Iterable[DuanRow]) -> Dict[str, List[int]]:
    idx_sets: Dict[str, Set[int]] = {}
    for i, d in enumerate(duans):
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
    ct_norm: str,
    ct_tokens: Tuple[str, ...],
    ct_region_norm: str,
    duans: List[DuanRow],
    token_index: Dict[str, List[int]],
    min_token_hits: int = 2,
) -> Optional[Tuple[DuanRow, float]]:
    if not ct_norm:
        return None

    cand_counts: Dict[int, int] = {}
    for t in ct_tokens:
        for idx in token_index.get(t, []):
            d = duans[idx]
            if ct_region_norm and d.province_norm and ct_region_norm != d.province_norm:
                continue
            cand_counts[idx] = cand_counts.get(idx, 0) + 1

    threshold = 1 if len(ct_tokens) <= 2 else min_token_hits
    cands = [i for i, c in cand_counts.items() if c >= threshold]
    if not cands:
        return None

    best_i = None
    best_score = 0.0
    for i in cands:
        d = duans[i]
        variants = d.variant_norms or (d.norm,)
        variant_tokens = d.variant_tokens or (d.tokens,)
        r_best = 0.0
        best_vt: Tuple[str, ...] = d.tokens
        for vn, vt in zip(variants, variant_tokens):
            rr = seq_ratio(ct_norm, vn)
            if rr > r_best:
                r_best = rr
                best_vt = vt

        j = 0.0
        if ct_tokens and best_vt:
            inter = len(set(ct_tokens).intersection(best_vt))
            union = len(set(ct_tokens).union(best_vt))
            j = inter / union if union else 0.0

        province_bonus = 0.05 if (ct_region_norm and d.province_norm and ct_region_norm == d.province_norm) else 0.0
        contain_bonus = 0.05 if (len(ct_norm) >= 8 and any((ct_norm in vn) or (vn in ct_norm) for vn in variants if vn)) else 0.0
        score = min(1.0, 0.65 * r_best + 0.3 * j + province_bonus + contain_bonus)
        if score > best_score:
            best_score = score
            best_i = i

    if best_i is None:
        return None
    return duans[best_i], best_score


def insert_mappings(
    db: Database,
    rows: List[Tuple[int, str, str, int, str, Optional[int], str, Optional[float]]],
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
            INSERT INTO chotot_projects_duan_merge
                (chotot_project_id, chotot_project_name, chotot_region_name, duan_id, duan_ten, duan_tinh_moi, match_type, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                chotot_project_name=VALUES(chotot_project_name),
                chotot_region_name=VALUES(chotot_region_name),
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
    parser = argparse.ArgumentParser(description="Merge chotot_projects -> duan by project name")
    parser.add_argument("--dry-run", action="store_true", help="Khong ghi DB, chi in thong ke")
    parser.add_argument("--rebuild", action="store_true", help="TRUNCATE bang merge truoc khi chay")
    parser.add_argument("--limit", type=int, default=0, help="Limit so dong chotot de test (0=all)")
    parser.add_argument("--min-score", type=float, default=0.91, help="Nguong fuzzy score")
    parser.add_argument("--min-token-hits", type=int, default=2, help="So token overlap toi thieu")
    parser.add_argument("--print-samples", type=int, default=15, help="So unmatched in ra")
    args = parser.parse_args()

    db = Database()
    ensure_merge_table(db)
    if args.rebuild:
        truncate_merge_table(db, dry_run=args.dry_run)

    t0 = time.time()
    province_names = fetch_province_names(db)
    duans = fetch_duan(db, province_names)
    chotot_projects = fetch_chotot_projects(db, limit=args.limit)

    exact_by_ten_and_province: Dict[Tuple[str, str], DuanRow] = {}
    norm_to_duan_by_province: Dict[Tuple[str, str], DuanRow] = {}
    norm_to_duan_global: Dict[str, DuanRow] = {}

    for d in duans:
        p = d.province_norm or ""
        k_exact = (d.duan_ten or "", p)
        if d.duan_ten and k_exact not in exact_by_ten_and_province:
            exact_by_ten_and_province[k_exact] = d

        for nn in d.variant_norms or (d.norm,):
            if not nn:
                continue
            kp = (nn, p)
            prevp = norm_to_duan_by_province.get(kp)
            if not prevp or d.duan_id > prevp.duan_id:
                norm_to_duan_by_province[kp] = d
            prevg = norm_to_duan_global.get(nn)
            if not prevg or d.duan_id > prevg.duan_id:
                norm_to_duan_global[nn] = d

    token_index = build_token_index(duans)

    mapped = 0
    mapped_exact = 0
    mapped_norm = 0
    mapped_fuzzy = 0
    unmatched: List[Tuple[int, str, str]] = []
    batch: List[Tuple[int, str, str, int, str, Optional[int], str, Optional[float]]] = []

    for ct_id, ct_name, ct_region_name in chotot_projects:
        ct_name = ct_name or ""
        ct_region_norm = normalize_name(ct_region_name or "")
        if not ct_name:
            continue

        d = exact_by_ten_and_province.get((ct_name, ct_region_norm))
        if not d:
            d = exact_by_ten_and_province.get((ct_name, ""))
        if d:
            batch.append((ct_id, ct_name, ct_region_name, d.duan_id, d.duan_ten, d.duan_tinh_moi, "exact", 1.0))
            mapped += 1
            mapped_exact += 1
            if len(batch) >= 1000:
                insert_mappings(db, batch, args.dry_run)
                batch.clear()
            continue

        n = normalize_name(ct_name)
        d = norm_to_duan_by_province.get((n, ct_region_norm)) or norm_to_duan_global.get(n)
        if d:
            mt = "norm_exact_province" if ct_region_norm and d.province_norm == ct_region_norm else "norm_exact"
            batch.append((ct_id, ct_name, ct_region_name, d.duan_id, d.duan_ten, d.duan_tinh_moi, mt, 1.0))
            mapped += 1
            mapped_norm += 1
            if len(batch) >= 1000:
                insert_mappings(db, batch, args.dry_run)
                batch.clear()
            continue

        toks = tokenize(n)
        best = best_fuzzy_match(
            ct_norm=n,
            ct_tokens=toks,
            ct_region_norm=ct_region_norm,
            duans=duans,
            token_index=token_index,
            min_token_hits=args.min_token_hits,
        )
        if best:
            bd, score = best
            if score >= args.min_score:
                mt = "fuzzy_province" if (ct_region_norm and bd.province_norm == ct_region_norm) else "fuzzy"
                batch.append((ct_id, ct_name, ct_region_name, bd.duan_id, bd.duan_ten, bd.duan_tinh_moi, mt, float(score)))
                mapped += 1
                mapped_fuzzy += 1
            else:
                unmatched.append((ct_id, ct_name, ct_region_name))
        else:
            unmatched.append((ct_id, ct_name, ct_region_name))

        if len(batch) >= 1000:
            insert_mappings(db, batch, args.dry_run)
            batch.clear()

    if batch:
        insert_mappings(db, batch, args.dry_run)

    total = len(chotot_projects)
    dur = time.time() - t0
    print("=== MERGE SUMMARY ===")
    print(f"chotot_projects total: {total}")
    print(f"mapped: {mapped} ({(mapped / total * 100) if total else 0:.2f}%)")
    print(f"- exact: {mapped_exact}")
    print(f"- norm_exact: {mapped_norm}")
    print(f"- fuzzy: {mapped_fuzzy} (min_score={args.min_score})")
    print(f"unmatched: {len(unmatched)}")
    print(f"duration: {dur:.2f}s")

    if args.print_samples > 0 and unmatched:
        print(f"\n=== SAMPLE UNMATCHED (first {min(args.print_samples, len(unmatched))}) ===")
        for rid, name, rname in unmatched[: args.print_samples]:
            print(f"- id={rid} | region={rname} | name={name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

