#!/usr/bin/env python3
"""
Merge (map) du an Alonhadat (duan_alonhadat) voi du an goc Cafeland (duan) theo ten.

Muc tieu:
- Tao bang mapping de join nhanh: duan_alonhadat_duan_merge
- Match theo 3 tang, uu tien cung tinh/thanh neu rut duoc tu address:
  1) exact
  2) normalized exact
  3) fuzzy

Bang mapping KHONG sua bang goc 'duan'. Chi insert/update bang moi.
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
    core_norm: str
    tokens: Tuple[str, ...]
    variant_norms: Tuple[str, ...]
    variant_core_norms: Tuple[str, ...]
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
    raw = normalize_name(s)
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
    raw = normalize_name(s)
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


def build_variant_core_norms(variant_norms: Tuple[str, ...]) -> Tuple[str, ...]:
    return tuple(sorted({strip_project_prefixes(v) for v in variant_norms if strip_project_prefixes(v)}))


def ensure_merge_table(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS duan_alonhadat_duan_merge (
                alonhadat_project_id INT NOT NULL,
                alonhadat_project_name VARCHAR(500) NOT NULL,
                alonhadat_address TEXT NULL,
                alonhadat_detail_url VARCHAR(1000) NULL,
                alonhadat_slug VARCHAR(1000) NULL,
                alonhadat_province_id INT NULL,
                alonhadat_province_name VARCHAR(255) NULL,
                duan_id INT NOT NULL,
                duan_ten VARCHAR(250) NULL,
                duan_tinh_moi INT NULL,
                match_type VARCHAR(32) NOT NULL,
                score DOUBLE NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (alonhadat_project_id),
                INDEX idx_duan_id (duan_id),
                INDEX idx_alonhadat_province_id (alonhadat_province_id)
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
        cur.execute("TRUNCATE TABLE duan_alonhadat_duan_merge")
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


def build_province_alias_map(province_names: Dict[int, str]) -> Dict[str, Tuple[int, str]]:
    alias_map: Dict[str, Tuple[int, str]] = {}
    for pid, name in province_names.items():
        n = normalize_name(name)
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
        norm = normalize_name(part)
        if not norm or norm == "viet nam":
            continue
        hit = alias_map.get(norm)
        if hit:
            return hit[0], hit[1]
        # try removing generic prefixes from address segment
        norm2 = re.sub(r"^\b(thanh pho|tp|tinh)\b\s*", "", norm).strip()
        hit = alias_map.get(norm2)
        if hit:
            return hit[0], hit[1]
    # final fallback: scan any alias contained in address
    addr_norm = normalize_name(raw)
    best: Optional[Tuple[int, str, int]] = None
    for alias, (pid, pname) in alias_map.items():
        if alias and alias in addr_norm:
            score = len(alias)
            if not best or score > best[2]:
                best = (pid, pname, score)
    if best:
        return best[0], best[1]
    return None, ""


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
            vcore_norms = build_variant_core_norms(vnorms)
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
                    core_norm=strip_project_prefixes(duan_ten),
                    tokens=tokenize(n),
                    variant_norms=vnorms,
                    variant_core_norms=vcore_norms,
                    variant_tokens=vtoks,
                )
            )
        return out
    finally:
        cur.close()
        conn.close()


def fetch_alonhadat_projects(db: Database, limit: int = 0) -> List[Tuple[int, str, str, str, str]]:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        sql = """
            SELECT id, project_name, address, detail_url, slug
            FROM duan_alonhadat
            WHERE project_name IS NOT NULL AND project_name <> ''
            ORDER BY id
        """
        if limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        out: List[Tuple[int, str, str, str, str]] = []
        for r in rows:
            if isinstance(r, dict):
                out.append((
                    int(r["id"]),
                    r.get("project_name") or "",
                    r.get("address") or "",
                    r.get("detail_url") or "",
                    r.get("slug") or "",
                ))
            else:
                out.append((int(r[0]), r[1] or "", r[2] or "", r[3] or "", r[4] or ""))
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
    src_norm: str,
    src_core: str,
    src_tokens: Tuple[str, ...],
    src_region_norm: str,
    duans: List[DuanRow],
    token_index: Dict[str, List[int]],
    min_token_hits: int = 2,
) -> Optional[Tuple[DuanRow, float]]:
    if not src_norm:
        return None
    cand_counts: Dict[int, int] = {}
    for t in src_tokens:
        for idx in token_index.get(t, []):
            d = duans[idx]
            if src_region_norm and d.province_norm and src_region_norm != d.province_norm:
                continue
            cand_counts[idx] = cand_counts.get(idx, 0) + 1
    threshold = 1 if len(src_tokens) <= 2 else min_token_hits
    cands = [i for i, c in cand_counts.items() if c >= threshold]
    if not cands:
        return None

    best_i = None
    best_score = 0.0
    for i in cands:
        d = duans[i]
        variants = d.variant_norms or (d.norm,)
        core_variants = d.variant_core_norms or ((d.core_norm,) if d.core_norm else tuple())
        variant_tokens = d.variant_tokens or (d.tokens,)
        r_best = 0.0
        best_vt: Tuple[str, ...] = d.tokens
        for vn, vt in zip(variants, variant_tokens):
            rr = seq_ratio(src_norm, vn)
            if rr > r_best:
                r_best = rr
                best_vt = vt
        if src_core and core_variants:
            for cv in core_variants:
                rr = seq_ratio(src_core, cv)
                if rr > r_best:
                    r_best = rr
                    best_vt = d.tokens

        j = 0.0
        if src_tokens and best_vt:
            inter = len(set(src_tokens).intersection(best_vt))
            union = len(set(src_tokens).union(best_vt))
            j = inter / union if union else 0.0

        province_bonus = 0.05 if (src_region_norm and d.province_norm and src_region_norm == d.province_norm) else 0.0
        contain_bonus = 0.05 if (len(src_norm) >= 8 and any((src_norm in vn) or (vn in src_norm) for vn in variants if vn)) else 0.0
        num_penalty = 0.0
        src_num = extract_trailing_number(src_core or src_norm)
        dst_nums = {extract_trailing_number(v) for v in variants}
        dst_nums.update({extract_trailing_number(v) for v in core_variants})
        dst_nums.discard(None)
        if src_num and dst_nums and src_num not in dst_nums:
            num_penalty = 0.18
        score = min(1.0, max(0.0, 0.65 * r_best + 0.3 * j + province_bonus + contain_bonus - num_penalty))
        if score > best_score:
            best_score = score
            best_i = i

    if best_i is None:
        return None
    return duans[best_i], best_score


def insert_mappings(
    db: Database,
    rows: List[Tuple[int, str, str, str, str, Optional[int], str, int, str, Optional[int], str, Optional[float]]],
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
            INSERT INTO duan_alonhadat_duan_merge
                (alonhadat_project_id, alonhadat_project_name, alonhadat_address, alonhadat_detail_url, alonhadat_slug,
                 alonhadat_province_id, alonhadat_province_name, duan_id, duan_ten, duan_tinh_moi, match_type, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    parser = argparse.ArgumentParser(description="Merge duan_alonhadat -> duan by project name")
    parser.add_argument("--dry-run", action="store_true", help="Khong ghi DB, chi in thong ke")
    parser.add_argument("--rebuild", action="store_true", help="TRUNCATE bang merge truoc khi chay")
    parser.add_argument("--limit", type=int, default=0, help="Limit so dong duan_alonhadat de test (0=all)")
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
    province_alias_map = build_province_alias_map(province_names)
    duans = fetch_duan(db, province_names)
    src_rows = fetch_alonhadat_projects(db, limit=args.limit)

    exact_by_ten_and_province: Dict[Tuple[str, str], DuanRow] = {}
    norm_to_duan_by_province: Dict[Tuple[str, str], DuanRow] = {}
    norm_to_duan_global: Dict[str, DuanRow] = {}
    core_to_duan_by_province: Dict[Tuple[str, str], DuanRow] = {}
    core_to_duan_global: Dict[str, DuanRow] = {}

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
        for cn in d.variant_core_norms or ((d.core_norm,) if d.core_norm else tuple()):
            if not cn:
                continue
            kp = (cn, p)
            prevp = core_to_duan_by_province.get(kp)
            if not prevp or d.duan_id > prevp.duan_id:
                core_to_duan_by_province[kp] = d
            prevg = core_to_duan_global.get(cn)
            if not prevg or d.duan_id > prevg.duan_id:
                core_to_duan_global[cn] = d

    token_index = build_token_index(duans)

    mapped = 0
    mapped_exact = 0
    mapped_norm = 0
    mapped_fuzzy = 0
    unmatched: List[Tuple[int, str, str]] = []
    batch: List[Tuple[int, str, str, str, str, Optional[int], str, int, str, Optional[int], str, Optional[float]]] = []

    for src_id, src_name, src_address, src_detail_url, src_slug in src_rows:
        src_name = src_name or ""
        if not src_name:
            continue
        province_id, province_name = extract_province_from_address(src_address, province_alias_map)
        src_region_norm = normalize_name(province_name)

        d = exact_by_ten_and_province.get((src_name, src_region_norm))
        if not d:
            d = exact_by_ten_and_province.get((src_name, ""))
        if d:
            batch.append((src_id, src_name, src_address, src_detail_url, src_slug, province_id, province_name, d.duan_id, d.duan_ten, d.duan_tinh_moi, "exact", 1.0))
            mapped += 1
            mapped_exact += 1
            if len(batch) >= 1000:
                insert_mappings(db, batch, args.dry_run)
                batch.clear()
            continue

        n = normalize_name(src_name)
        c = strip_project_prefixes(src_name)
        d = norm_to_duan_by_province.get((n, src_region_norm)) or norm_to_duan_global.get(n)
        if d:
            mt = "norm_exact_province" if src_region_norm and d.province_norm == src_region_norm else "norm_exact"
            batch.append((src_id, src_name, src_address, src_detail_url, src_slug, province_id, province_name, d.duan_id, d.duan_ten, d.duan_tinh_moi, mt, 1.0))
            mapped += 1
            mapped_norm += 1
            if len(batch) >= 1000:
                insert_mappings(db, batch, args.dry_run)
                batch.clear()
            continue

        if c:
            d = core_to_duan_by_province.get((c, src_region_norm)) or core_to_duan_global.get(c)
            if d:
                mt = "core_exact_province" if src_region_norm and d.province_norm == src_region_norm else "core_exact"
                batch.append((src_id, src_name, src_address, src_detail_url, src_slug, province_id, province_name, d.duan_id, d.duan_ten, d.duan_tinh_moi, mt, 1.0))
                mapped += 1
                mapped_norm += 1
                if len(batch) >= 1000:
                    insert_mappings(db, batch, args.dry_run)
                    batch.clear()
                continue

        toks = tokenize(n)
        best = best_fuzzy_match(
            src_norm=n,
            src_core=c,
            src_tokens=toks,
            src_region_norm=src_region_norm,
            duans=duans,
            token_index=token_index,
            min_token_hits=args.min_token_hits,
        )
        if best:
            bd, score = best
            if score >= args.min_score:
                mt = "fuzzy_province" if (src_region_norm and bd.province_norm == src_region_norm) else "fuzzy"
                batch.append((src_id, src_name, src_address, src_detail_url, src_slug, province_id, province_name, bd.duan_id, bd.duan_ten, bd.duan_tinh_moi, mt, float(score)))
                mapped += 1
                mapped_fuzzy += 1
            else:
                unmatched.append((src_id, src_name, province_name))
        else:
            unmatched.append((src_id, src_name, province_name))

        if len(batch) >= 1000:
            insert_mappings(db, batch, args.dry_run)
            batch.clear()

    if batch:
        insert_mappings(db, batch, args.dry_run)

    total = len(src_rows)
    dur = time.time() - t0
    print("=== MERGE SUMMARY ===")
    print(f"duan_alonhadat total: {total}")
    print(f"mapped: {mapped} ({(mapped / total * 100) if total else 0:.2f}%)")
    print(f"- exact: {mapped_exact}")
    print(f"- norm_exact: {mapped_norm}")
    print(f"- fuzzy: {mapped_fuzzy} (min_score={args.min_score})")
    print(f"unmatched: {len(unmatched)}")
    print(f"duration: {dur:.2f}s")

    if args.print_samples > 0 and unmatched:
        print(f"\n=== SAMPLE UNMATCHED (first {min(args.print_samples, len(unmatched))}) ===")
        for rid, name, pname in unmatched[: args.print_samples]:
            print(f"- id={rid} | province={pname} | name={name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
