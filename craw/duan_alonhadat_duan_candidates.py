#!/usr/bin/env python3
"""
Generate candidate matches for duan_alonhadat -> duan for manual review.

It does NOT modify table `duan`.
It writes candidates into `duan_alonhadat_duan_candidates`.
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
    duan_tinh_moi: Optional[int]
    province_norm: str
    variants_norm: Tuple[str, ...]
    variants_core_norm: Tuple[str, ...]
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
    s = re.sub(r"\b(the|du an|khu do thi|kdt|kdc)\b", " ", s)
    return _RE_SPACES.sub(" ", s).strip()


_PROJECT_PREFIXES = (
    "khu do thi moi",
    "khu do thi",
    "khu dan cu",
    "khu biet thu",
    "khu nghi duong",
    "khu nha pho",
    "cao oc van phong",
    "toa nha van phong",
    "can ho chung cu",
    "can ho dich vu",
    "can ho",
    "chung cu",
    "van phong",
    "khu phuc hop",
)


def strip_project_prefixes(s: str) -> str:
    raw = normalize(s)
    if not raw:
        return ""
    changed = True
    while changed:
        changed = False
        for prefix in _PROJECT_PREFIXES:
            if raw == prefix:
                return ""
            if raw.startswith(prefix + " "):
                raw = raw[len(prefix) + 1 :].strip()
                changed = True
                break
    return raw


_RE_TRAILING_NUMBER = re.compile(r"(\d+)(?:\s*[a-z])?$")


def extract_trailing_number(s: str) -> Optional[str]:
    raw = normalize(s)
    if not raw:
        return None
    m = _RE_TRAILING_NUMBER.search(raw)
    return m.group(1) if m else None


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


def ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


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


def build_variant_norms(duan_ten: str, duan_title: str, duan_title_no: str) -> Tuple[str, ...]:
    variants: Set[str] = set()
    if duan_ten:
        variants.add(normalize(duan_ten))
    for a in duan_title_aliases(duan_title):
        na = normalize(a)
        if na:
            variants.add(na)
    if duan_title_no:
        variants.add(normalize(duan_title_no.replace("-", " ")))
    return tuple(sorted(v for v in variants if v))


def ensure_candidates_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS duan_alonhadat_duan_candidates")
        cur.execute(
            """
            CREATE TABLE duan_alonhadat_duan_candidates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                alonhadat_project_name VARCHAR(500) NOT NULL,
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


def fetch_province_names(db: Database) -> Dict[int, str]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT city_id, city_title FROM transaction_city WHERE city_loai = 0")
        rows = cur.fetchall()
        out: Dict[int, str] = {}
        for r in rows:
            out[int(r["city_id"])] = r["city_title"] or ""
        return out
    finally:
        cur.close()
        conn.close()


def build_province_alias_map(province_names: Dict[int, str]) -> Dict[str, Tuple[int, str]]:
    alias_map: Dict[str, Tuple[int, str]] = {}
    for pid, name in province_names.items():
        n = normalize(name)
        aliases = {
            n,
            re.sub(r"^\b(thanh pho|tp)\b\s*", "", n).strip(),
            re.sub(r"^\b(tinh)\b\s*", "", n).strip(),
        }
        if n == "ho chi minh":
            aliases.update({"tp hcm", "tphcm", "tp ho chi minh", "sai gon"})
        if n == "ha noi":
            aliases.update({"tp ha noi", "thanh pho ha noi"})
        for alias in {a for a in aliases if a}:
            alias_map[alias] = (pid, name)
    return alias_map


def extract_province_from_address(address: str, alias_map: Dict[str, Tuple[int, str]]) -> Tuple[Optional[int], str]:
    raw = (address or "").strip()
    if not raw:
        return None, ""
    parts = [p.strip() for p in raw.split(",") if p and p.strip()]
    for part in reversed(parts):
        norm = normalize(part)
        if not norm or norm == "viet nam":
            continue
        hit = alias_map.get(norm)
        if hit:
            return hit[0], hit[1]
        norm2 = re.sub(r"^\b(thanh pho|tp|tinh)\b\s*", "", norm).strip()
        hit = alias_map.get(norm2)
        if hit:
            return hit[0], hit[1]
    return None, ""


def load_alonhadat(db: Database, limit: int = 0, only_unmapped: bool = False) -> List[Tuple[int, str, str, Optional[int], str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        if only_unmapped:
            sql = """
                SELECT a.id, a.project_name, a.address, m.alonhadat_province_id, m.alonhadat_province_name
                FROM duan_alonhadat a
                LEFT JOIN duan_alonhadat_duan_merge m ON m.alonhadat_project_id = a.id
                WHERE a.project_name IS NOT NULL AND a.project_name <> ''
                  AND m.alonhadat_project_id IS NULL
                ORDER BY a.id
            """
        else:
            sql = """
                SELECT id, project_name, address, NULL AS alonhadat_province_id, NULL AS alonhadat_province_name
                FROM duan_alonhadat
                WHERE project_name IS NOT NULL AND project_name <> ''
                ORDER BY id
            """
        if limit and limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        return [
            (
                int(r["id"]),
                r["project_name"] or "",
                r["address"] or "",
                int(r["alonhadat_province_id"]) if r.get("alonhadat_province_id") else None,
                r.get("alonhadat_province_name") or "",
            )
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()


def load_duan(db: Database, province_names: Dict[int, str]) -> List[Duan]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT duan_id, duan_ten, duan_title, duan_title_no, duan_tinh_moi
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
            tinh_moi = int(r["duan_tinh_moi"]) if r.get("duan_tinh_moi") else None
            province_name = province_names.get(tinh_moi or 0, "")
            vnorm = build_variant_norms(ten, title, slug)
            vcore = tuple(sorted({strip_project_prefixes(v) for v in vnorm if strip_project_prefixes(v)}))
            vtoks = tuple(tokenize(v) for v in vnorm) if vnorm else tuple()
            out.append(
                Duan(
                    duan_id=duan_id,
                    duan_ten=ten,
                    duan_title=title,
                    duan_title_no=slug,
                    duan_tinh_moi=tinh_moi,
                    province_norm=normalize(province_name),
                    variants_norm=vnorm,
                    variants_core_norm=vcore,
                    variants_tokens=vtoks,
                )
            )
        return out
    finally:
        cur.close()
        conn.close()


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
    src_norm: str,
    src_core: str,
    src_tokens: Tuple[str, ...],
    src_region_norm: str,
    duans: Sequence[Duan],
    token_index: Dict[str, List[int]],
    topk: int,
    min_token_hits: int,
) -> List[Tuple[Duan, float, str]]:
    if not src_norm:
        return []
    counts: Dict[int, int] = {}
    for t in src_tokens:
        for i in token_index.get(t, []):
            d = duans[i]
            if src_region_norm and d.province_norm and src_region_norm != d.province_norm:
                continue
            counts[i] = counts.get(i, 0) + 1
    hit_th = 1 if len(src_tokens) <= 2 else min_token_hits
    cand_idx = [i for i, c in counts.items() if c >= hit_th]
    if not cand_idx:
        return []
    scored: List[Tuple[Duan, float, str]] = []
    for i in cand_idx:
        d = duans[i]
        best_r = 0.0
        best_v = ""
        for vn in d.variants_norm:
            rr = ratio(src_norm, vn)
            if rr > best_r:
                best_r = rr
                best_v = vn
        if src_core and d.variants_core_norm:
            for vn in d.variants_core_norm:
                rr = ratio(src_core, vn)
                if rr > best_r:
                    best_r = rr
                    best_v = vn
        if len(src_norm) >= 8 and any((src_norm in vn) or (vn in src_norm) for vn in d.variants_norm):
            best_r = min(1.0, best_r + 0.03)
        if src_core and any((src_core in vn) or (vn in src_core) for vn in d.variants_core_norm):
            best_r = min(1.0, best_r + 0.03)
        if src_region_norm and d.province_norm and src_region_norm == d.province_norm:
            best_r = min(1.0, best_r + 0.03)
        src_num = extract_trailing_number(src_core or src_norm)
        dst_nums = {extract_trailing_number(v) for v in d.variants_norm}
        dst_nums.update({extract_trailing_number(v) for v in d.variants_core_norm})
        dst_nums.discard(None)
        if src_num and dst_nums and src_num not in dst_nums:
            best_r = max(0.0, best_r - 0.18)
        scored.append((d, best_r, best_v))
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
            INSERT INTO duan_alonhadat_duan_candidates
                (alonhadat_project_name, ten_rut_gon, duan_ten, similarity, confirmed)
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
    ap = argparse.ArgumentParser(description="Generate candidates for duan_alonhadat -> duan")
    ap.add_argument("--rebuild", action="store_true", help="Drop+create duan_alonhadat_duan_candidates")
    ap.add_argument("--dry-run", action="store_true", help="No DB writes")
    ap.add_argument("--limit", type=int, default=0, help="Limit rows (0=all)")
    ap.add_argument("--only-unmapped", action="store_true", help="Only projects not in duan_alonhadat_duan_merge")
    ap.add_argument("--topk", type=int, default=5, help="Top K candidates per project")
    ap.add_argument("--min-potential", type=float, default=0.85, help="Potential if similarity >= this")
    ap.add_argument("--max-potential", type=float, default=0.91, help="Only keep below this threshold")
    ap.add_argument("--min-token-hits", type=int, default=2, help="Token overlap threshold")
    args = ap.parse_args()

    db = Database()
    if args.rebuild:
        ensure_candidates_table(db)
    else:
        conn = db.get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES LIKE 'duan_alonhadat_duan_candidates'")
            if not cur.fetchone():
                ensure_candidates_table(db)
        finally:
            cur.close()
            conn.close()

    t0 = time.time()
    province_names = fetch_province_names(db)
    alias_map = build_province_alias_map(province_names)
    duans = load_duan(db, province_names)
    token_index = build_token_index(duans)
    src_rows = load_alonhadat(db, limit=args.limit, only_unmapped=args.only_unmapped)

    counts = {"potential": 0, "no_match": 0}
    batch: List[Tuple[str, Optional[str], Optional[str], Optional[float], int]] = []

    for src_id, src_name, src_address, src_province_id, src_province_name in src_rows:
        name = (src_name or "").strip()
        if not name:
            continue
        province_id = src_province_id
        province_name = src_province_name
        if not province_id and src_address:
            province_id, province_name = extract_province_from_address(src_address, alias_map)
        region_norm = normalize(province_name or "")

        src_norm = normalize(name)
        src_core = strip_project_prefixes(name)
        src_tokens = tokenize(src_norm)
        cands = best_candidates(
            src_norm=src_norm,
            src_core=src_core,
            src_tokens=src_tokens,
            src_region_norm=region_norm,
            duans=duans,
            token_index=token_index,
            topk=args.topk,
            min_token_hits=args.min_token_hits,
        )

        kept = 0
        for d, sim, via in cands:
            if sim < args.min_potential or sim >= args.max_potential:
                continue
            kept += 1
            batch.append(
                (
                    name,
                    src_core or None,
                    d.duan_ten,
                    round(sim * 100.0, 2),
                    0,
                )
            )
        if kept:
            counts["potential"] += 1
        else:
            counts["no_match"] += 1

        if len(batch) >= 2000:
            insert_candidates(db, batch, dry_run=args.dry_run)
            batch.clear()

    if batch:
        insert_candidates(db, batch, dry_run=args.dry_run)

    dur = time.time() - t0
    print("=== CANDIDATE GENERATION SUMMARY ===")
    print(f"duan_alonhadat processed: {len(src_rows)}")
    print(f"- potential groups: {counts['potential']}")
    print(f"- no_match groups: {counts['no_match']}")
    print(f"duration: {dur:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
