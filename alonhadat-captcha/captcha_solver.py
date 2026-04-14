#!/usr/bin/env python3
"""
Alonhadat.com.vn Captcha Solver
- Phase 1: Build database (manual labeling)
- Phase 2: Auto solve

Captcha: 4 animals, 1 blue border (excluded), answer is 3 remaining animals concatenated.
"""

import hashlib
import io
import json
import math
import os
import shutil
import string
import sys

import numpy as np
import requests
from PIL import Image


def setup_console_encoding():
    """Force UTF-8 output on terminals that default to legacy code pages."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass


setup_console_encoding()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, "captcha_db.json")
TRAIN_DIR = os.path.join(SCRIPT_DIR, "training_data")
DEDUP_TRASH_DIR = os.path.join(TRAIN_DIR, "_dedup_removed")
ANIMALS = ["bo", "chim", "cho", "chuot", "ga", "heo", "ho", "meo", "ngua", "tho", "trau", "vit", "voi"]

PHASH_PREFIX = "phash:"
PHASH_DISTANCE_THRESHOLD = 10
PHASH_SCAN_DIRS = [os.path.join(SCRIPT_DIR, "cells"), os.path.join(SCRIPT_DIR, "training_data")]
SAVE_DEDUP_HAMMING_THRESHOLD = 4

_DCT_MATRIX_CACHE = {}
_LABEL_PHASH_CACHE = {}


def is_md5_key(key):
    return len(key) == 32 and all(c in string.hexdigits for c in key)


def is_phash_key(key):
    return key.startswith(PHASH_PREFIX) and len(key) == len(PHASH_PREFIX) + 16


def db_image_count(db):
    return sum(1 for key in db if is_md5_key(key))


def db_phash_count(db):
    return sum(1 for key in db if is_phash_key(key))


def load_db():
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)


def get_label_phash_cache(label):
    cache = _LABEL_PHASH_CACHE.get(label)
    if cache is not None:
        return cache

    label_dir = os.path.join(TRAIN_DIR, label)
    cache = []
    if os.path.exists(label_dir):
        for fname in os.listdir(label_dir):
            if not fname.lower().endswith(".png"):
                continue
            fpath = os.path.join(label_dir, fname)
            try:
                img = Image.open(fpath).convert("RGB")
                cache.append(phash_cell(img))
            except Exception:
                continue
    _LABEL_PHASH_CACHE[label] = cache
    return cache


def persist_labeled_cell(cell, label, md5_hash):
    """Persist labeled samples to training_data/<label>/ for future backfill/training."""
    label_dir = os.path.join(TRAIN_DIR, label)
    os.makedirs(label_dir, exist_ok=True)
    out_path = os.path.join(label_dir, f"{md5_hash}.png")
    if not os.path.exists(out_path):
        new_phash = phash_cell(cell)
        known_phashes = get_label_phash_cache(label)
        for known in known_phashes:
            if hamming_distance_hex(new_phash, known) <= SAVE_DEDUP_HAMMING_THRESHOLD:
                return False

        cell.save(out_path)
        known_phashes.append(new_phash)
        return True
    return False


def download_captcha(session=None):
    """Download captcha image bytes."""
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    session.get("https://alonhadat.com.vn/xac-thuc-nguoi-dung.html")
    r = session.get("https://alonhadat.com.vn/ImageCaptcha.ashx?v=3")
    return r.content, session


def cut_cells(img_bytes):
    """Split captcha into 4 animal cells."""
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    img_arr = np.array(img)

    row_mask = np.any(img_arr > 30, axis=(1, 2))
    col_mask = np.any(img_arr > 30, axis=(0, 2))
    rows = np.where(row_mask)[0]
    cols = np.where(col_mask)[0]

    if len(rows) == 0 or len(cols) == 0:
        print("Khong tim thay vung anh!")
        return [], img

    top, bottom = rows[0], rows[-1]
    left, right = cols[0], cols[-1]
    content = img.crop((left, top, right + 1, bottom + 1))
    cw, ch = content.size

    cell_w = cw // 4
    cells = []
    for i in range(4):
        x1 = i * cell_w
        x2 = (i + 1) * cell_w if i < 3 else cw
        cells.append(content.crop((x1, 0, x2, ch)).convert("RGB"))
    return cells, img


def detect_border_color(cell):
    """Return 'blue' if blue border is detected, else 'other'."""
    arr = np.array(cell)
    border_width = 5
    border_pixels = []

    border_pixels.extend(arr[:border_width, :].reshape(-1, 3).tolist())
    border_pixels.extend(arr[-border_width:, :].reshape(-1, 3).tolist())
    border_pixels.extend(arr[:, :border_width].reshape(-1, 3).tolist())
    border_pixels.extend(arr[:, -border_width:].reshape(-1, 3).tolist())

    border_pixels = np.array(border_pixels)
    avg_r = np.mean(border_pixels[:, 0])
    avg_g = np.mean(border_pixels[:, 1])
    avg_b = np.mean(border_pixels[:, 2])

    if avg_b > 150 and avg_r < 100 and avg_g < 100:
        return "blue"
    return "other"


def strip_colored_border(cell):
    """
    Remove colored frame (blue/yellow) using color-distance from border color.
    Falls back to fixed margin if detection is unstable.
    """
    arr = np.array(cell.convert("RGB"), dtype=np.int16)
    h, w = arr.shape[:2]
    if h < 20 or w < 20:
        return cell

    bw = max(2, min(5, min(h, w) // 12))
    border_pixels = np.concatenate(
        [
            arr[:bw, :, :].reshape(-1, 3),
            arr[-bw:, :, :].reshape(-1, 3),
            arr[:, :bw, :].reshape(-1, 3),
            arr[:, -bw:, :].reshape(-1, 3),
        ],
        axis=0,
    )
    border_color = np.median(border_pixels, axis=0)
    dist = np.linalg.norm(arr - border_color, axis=2)
    content_mask = dist > 45.0

    ratio_thresh = 0.08
    max_scan_y = max(1, h // 3)
    max_scan_x = max(1, w // 3)

    top = 0
    for y in range(max_scan_y):
        if np.mean(content_mask[y, :]) >= ratio_thresh:
            top = y
            break

    bottom = h - 1
    for y in range(h - 1, h - max_scan_y - 1, -1):
        if np.mean(content_mask[y, :]) >= ratio_thresh:
            bottom = y
            break

    left = 0
    for x in range(max_scan_x):
        if np.mean(content_mask[:, x]) >= ratio_thresh:
            left = x
            break

    right = w - 1
    for x in range(w - 1, w - max_scan_x - 1, -1):
        if np.mean(content_mask[:, x]) >= ratio_thresh:
            right = x
            break

    if right - left + 1 < max(16, w // 2) or bottom - top + 1 < max(16, h // 2):
        margin = min(10, max(2, min(w, h) // 8))
        return cell.crop((margin, margin, w - margin, h - margin))

    return cell.crop((left, top, right + 1, bottom + 1))


def preprocess_cell(cell, size=80):
    """Strip frame aggressively, normalize size, and reduce color sensitivity."""
    inner = strip_colored_border(cell)
    # Extra safety trim so blue/yellow frame never leaks into fingerprinting.
    w, h = inner.size
    trim = max(1, int(min(w, h) * 0.08))
    if w - 2 * trim >= 16 and h - 2 * trim >= 16:
        inner = inner.crop((trim, trim, w - trim, h - trim))
    inner = inner.convert("L")
    return inner.resize((size, size), Image.LANCZOS)


def hash_cell(cell):
    """Stable MD5 hash for exact matching."""
    inner = preprocess_cell(cell, size=80)
    buf = io.BytesIO()
    inner.save(buf, format="PNG")
    return hashlib.md5(buf.getvalue()).hexdigest()


def get_dct_matrix(n):
    if n in _DCT_MATRIX_CACHE:
        return _DCT_MATRIX_CACHE[n]

    mat = np.zeros((n, n), dtype=np.float32)
    factor = math.pi / (2.0 * n)
    scale0 = math.sqrt(1.0 / n)
    scale = math.sqrt(2.0 / n)

    for u in range(n):
        alpha = scale0 if u == 0 else scale
        for x in range(n):
            mat[u, x] = alpha * math.cos((2 * x + 1) * u * factor)

    _DCT_MATRIX_CACHE[n] = mat
    return mat


def phash_cell(cell, hash_size=8, highfreq_factor=4):
    """Perceptual hash (pHash) represented as 16-char hex (64 bits)."""
    size = hash_size * highfreq_factor
    gray = np.array(preprocess_cell(cell, size=size).convert("L"), dtype=np.float32)
    dct_mat = get_dct_matrix(size)
    dct = dct_mat @ gray @ dct_mat.T

    low_freq = dct[:hash_size, :hash_size].flatten()
    median = np.median(low_freq[1:]) if len(low_freq) > 1 else low_freq[0]
    bits = (low_freq > median).astype(np.uint8)
    bits[0] = 0  # ignore DC term

    value = 0
    for bit in bits:
        value = (value << 1) | int(bit)
    return f"{value:016x}"


def hamming_distance_hex(a, b):
    return (int(a, 16) ^ int(b, 16)).bit_count()


def build_phash_index(db):
    index = []
    for key, label in db.items():
        if is_phash_key(key):
            index.append((key[len(PHASH_PREFIX):], label))
    return index


def find_cell_label(cell, db, phash_index, max_distance=PHASH_DISTANCE_THRESHOLD):
    md5_hash = hash_cell(cell)
    if md5_hash in db:
        return db[md5_hash], "exact", 0, md5_hash, None

    cell_phash = phash_cell(cell)
    best_label = None
    best_dist = 10**9
    second_label = None
    second_dist = 10**9

    for known_phash, label in phash_index:
        dist = hamming_distance_hex(cell_phash, known_phash)
        if dist < best_dist:
            second_label = best_label
            second_dist = best_dist
            best_dist = dist
            best_label = label
            if best_dist == 0:
                break
        elif dist < second_dist:
            second_label = label
            second_dist = dist

    # Accept confident near-match:
    # - very close match (<= 5), or
    # - close enough and clearly better than next candidate.
    confident = best_dist <= 5 or (best_dist <= max_distance and (second_dist - best_dist) >= 3)

    # bo/trau is a common confusion pair: require stricter evidence.
    if best_label in {"bo", "trau"} and best_dist > 6:
        confident = False
    if {best_label, second_label} == {"bo", "trau"} and (second_dist - best_dist) < 4:
        confident = False

    if best_label is not None and confident:
        return best_label, "phash", best_dist, md5_hash, cell_phash

    return None, "unknown", best_dist, md5_hash, cell_phash


def upsert_phash_label(db, cell, label, force=False):
    phash_value = phash_cell(cell)
    key = f"{PHASH_PREFIX}{phash_value}"
    if key not in db:
        db[key] = label
        return True, False, False
    if db[key] != label:
        if force:
            db[key] = label
            return False, False, True
        return False, True, False
    return False, False, False


def backfill_phash_from_local_images(db):
    """
    Add pHash entries from local images that already map to known MD5 labels.
    This upgrades old MD5-only DBs without losing compatibility.
    """
    added = 0
    conflicts = 0

    md5_prefix_to_labels = {}
    for key, label in db.items():
        if is_md5_key(key):
            md5_prefix_to_labels.setdefault(key[:8], set()).add(label)

    for base_dir in PHASH_SCAN_DIRS:
        if not os.path.exists(base_dir):
            continue
        for root, _, files in os.walk(base_dir):
            for fname in files:
                if not fname.lower().endswith(".png"):
                    continue
                fpath = os.path.join(root, fname)
                try:
                    cell = Image.open(fpath).convert("RGB")
                except Exception:
                    continue

                label = None
                short_hash = os.path.splitext(fname)[0].split("_")[-1].lower()
                if len(short_hash) == 8 and all(c in string.hexdigits for c in short_hash):
                    candidates = md5_prefix_to_labels.get(short_hash, set())
                    if len(candidates) == 1:
                        label = next(iter(candidates))

                if label is None:
                    md5_hash = hash_cell(cell)
                    label = db.get(md5_hash)
                if not label:
                    continue

                changed, conflict, _ = upsert_phash_label(db, cell, label)
                if changed:
                    added += 1
                if conflict:
                    conflicts += 1

    return added, conflicts


def show_captcha_info(cells, db, phash_index):
    """Print recognition status for 4 cells."""
    print("\n" + "=" * 50)
    for i, cell in enumerate(cells):
        border = detect_border_color(cell)
        border_label = "🔵 XANH" if border == "blue" else "🟡 KHAC"
        name, method, dist, md5_hash, phash_value = find_cell_label(cell, db, phash_index)

        if method == "exact":
            print(f"  O {i+1}: [{border_label}] {name} (exact, md5={md5_hash[:8]})")
        elif method == "phash":
            print(f"  O {i+1}: [{border_label}] {name} (phash, d={dist})")
        else:
            print(f"  O {i+1}: [{border_label}] ??? - md5: {md5_hash[:8]}..., phash: {phash_value[:8]}...")
    print("=" * 50)


def auto_solve(cells, db, phash_index):
    """Auto solve when all 4 cells are recognized (exact or pHash match)."""
    answer_parts = []

    for cell in cells:
        border = detect_border_color(cell)
        name, method, _, _, _ = find_cell_label(cell, db, phash_index)
        if method == "unknown":
            return None
        if border != "blue":
            answer_parts.append(name)

    if len(answer_parts) == 3:
        return "".join(answer_parts)
    return None


def learn_recognized_cells(cells, db, phash_index):
    """
    Promote phash matches into md5 entries and persist images to training_data.
    Returns: (md5_added, files_saved)
    """
    md5_added = 0
    files_saved = 0
    for cell in cells:
        label, method, dist, md5_hash, _ = find_cell_label(cell, db, phash_index)
        if method == "unknown" or not label:
            continue
        if method == "phash":
            # Prevent poisoning from uncertain phash predictions.
            if dist > 2:
                continue
            if label in {"bo", "trau"} and dist > 1:
                continue
        if md5_hash not in db:
            db[md5_hash] = label
            md5_added += 1
        if persist_labeled_cell(cell, label, md5_hash):
            files_saved += 1
    return md5_added, files_saved


def build_mode(count=20):
    """Build DB by manual labeling."""
    db = load_db()
    added, conflicts = backfill_phash_from_local_images(db)
    if added > 0:
        save_db(db)
    phash_index = build_phash_index(db)

    print("\n🔧 CHE DO BUILD DATABASE")
    print(f"   Exact entries: {db_image_count(db)}")
    print(f"   pHash entries: {db_phash_count(db)}")
    if added > 0 or conflicts > 0:
        print(f"   Backfill pHash: +{added}, conflict={conflicts}")
    print(f"   Con vat hop le: {', '.join(ANIMALS)}")
    print("   Nhap 'q' de thoat\n")

    solved = 0
    while solved < count:
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

        try:
            img_bytes, _ = download_captcha(session)
        except Exception as e:
            print(f"❌ Loi tai captcha: {e}")
            continue

        cells, full_img = cut_cells(img_bytes)
        if len(cells) != 4:
            print(f"❌ Khong cat duoc 4 o (got {len(cells)})")
            continue

        full_img.show()
        show_captcha_info(cells, db, phash_index)

        # Persist recognized cells immediately, even if some cells are still unknown.
        # This avoids re-learning the same known cells on the next captcha.
        md5_added_now, files_saved_now = learn_recognized_cells(cells, db, phash_index)
        if md5_added_now or files_saved_now:
            save_db(db)
            phash_index = build_phash_index(db)
            print(f"   + Luu tam tu nhan dien: md5 +{md5_added_now}, files +{files_saved_now}")

        unknown_count = sum(
            1 for cell in cells if find_cell_label(cell, db, phash_index)[1] == "unknown"
        )

        if unknown_count == 0:
            answer = auto_solve(cells, db, phash_index)
            if answer:
                md5_added, files_saved = learn_recognized_cells(cells, db, phash_index)
                if md5_added or files_saved:
                    save_db(db)
                    phash_index = build_phash_index(db)
                    print(f"   + Hoc them tu auto: md5 +{md5_added}, files +{files_saved}")
                print(f"✅ Auto: {answer}")
                solved += 1
                continue

        while True:
            names_input = input("\n  Nhap ten 4 con (cach nhau boi dau cach): ").strip().lower()
            if names_input == "q":
                save_db(db)
                print(f"\n💾 Da luu DB | exact={db_image_count(db)} | phash={db_phash_count(db)}")
                return

            names = names_input.split()
            if len(names) != 4:
                print("  ❌ Can nhap dung 4 ten! (VD: ga ngua bo ho)")
                continue

            invalid = [n for n in names if n not in ANIMALS]
            if invalid:
                print(f"  ❌ Ten khong hop le: {', '.join(invalid)}")
                print(f"     Chon: {', '.join(ANIMALS)}")
                continue

            for i, cell in enumerate(cells):
                md5_hash = hash_cell(cell)
                label = names[i]
                if md5_hash not in db:
                    db[md5_hash] = label
                    print(f"  ✅ O {i+1}: {label} (md5 moi)")
                else:
                    if db[md5_hash] != label:
                        old_label = db[md5_hash]
                        db[md5_hash] = label
                        print(f"  🔁 O {i+1}: sua md5 {old_label} -> {label}")
                    else:
                        print(f"  ✓  O {i+1}: {db[md5_hash]} (md5 da biet)")

                phash_added, phash_conflict, phash_overwrite = upsert_phash_label(
                    db, cell, label, force=True
                )
                if phash_added:
                    print(f"     + pHash them moi")
                elif phash_overwrite:
                    print(f"     🔁 pHash sua nhan theo input")
                elif phash_conflict:
                    print(f"     ! pHash xung dot")

                if persist_labeled_cell(cell, label, md5_hash):
                    print("     + Luu file training_data")

            save_db(db)
            phash_index = build_phash_index(db)
            break

        answer = auto_solve(cells, db, phash_index)
        if answer:
            print(f"  🎯 Dap an: {answer}")

        solved += 1
        print(
            f"\n--- Da xu ly {solved}/{count} | exact={db_image_count(db)} | phash={db_phash_count(db)} ---"
        )

    print(f"\n💾 Hoan tat | exact={db_image_count(db)} | phash={db_phash_count(db)}")


def solve(img_bytes):
    """API for crawler: return answer or None."""
    db = load_db()
    phash_index = build_phash_index(db)
    cells, _ = cut_cells(img_bytes)
    if len(cells) != 4:
        return None
    return auto_solve(cells, db, phash_index)


def test_mode():
    """Test auto solve with a fresh captcha."""
    db = load_db()
    phash_index = build_phash_index(db)
    print(f"\n🧪 TEST AUTO SOLVE | exact={db_image_count(db)} | phash={db_phash_count(db)}")

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    img_bytes, _ = download_captcha(session)
    cells, full_img = cut_cells(img_bytes)

    full_img.show()
    show_captcha_info(cells, db, phash_index)

    answer = auto_solve(cells, db, phash_index)
    if answer:
        print(f"\n✅ Dap an: {answer}")
    else:
        print("\n❌ Chua du data de auto solve")


def dedup_training_data():
    """
    Deduplicate near-identical images in training_data by pHash per label.
    Duplicates are moved to training_data/_dedup_removed/<label>/ for safety.
    """
    os.makedirs(DEDUP_TRASH_DIR, exist_ok=True)
    moved_total = 0
    kept_total = 0

    for label in ANIMALS:
        label_dir = os.path.join(TRAIN_DIR, label)
        if not os.path.exists(label_dir):
            continue

        files = [f for f in os.listdir(label_dir) if f.lower().endswith(".png")]
        files.sort()
        if not files:
            continue

        kept = []  # (phash, filename)
        moved = 0

        for fname in files:
            fpath = os.path.join(label_dir, fname)
            try:
                img = Image.open(fpath).convert("RGB")
                p = phash_cell(img)
            except Exception:
                continue

            is_dup = False
            for kp, _ in kept:
                if hamming_distance_hex(p, kp) <= SAVE_DEDUP_HAMMING_THRESHOLD:
                    is_dup = True
                    break

            if is_dup:
                trash_label_dir = os.path.join(DEDUP_TRASH_DIR, label)
                os.makedirs(trash_label_dir, exist_ok=True)
                target = os.path.join(trash_label_dir, fname)
                if os.path.exists(target):
                    base, ext = os.path.splitext(fname)
                    n = 1
                    while True:
                        candidate = os.path.join(trash_label_dir, f"{base}__dup{n}{ext}")
                        if not os.path.exists(candidate):
                            target = candidate
                            break
                        n += 1
                shutil.move(fpath, target)
                moved += 1
            else:
                kept.append((p, fname))

        moved_total += moved
        kept_total += len(kept)
        if moved > 0:
            print(f"{label}: moved {moved} duplicates, kept {len(kept)}")
        else:
            print(f"{label}: no duplicates, kept {len(kept)}")

    print(f"\nDedup done. Kept: {kept_total}, moved: {moved_total}")
    print(f"Backup duplicates at: {DEDUP_TRASH_DIR}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_mode()
        elif sys.argv[1] == "build":
            n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
            build_mode(n)
        elif sys.argv[1] == "dedup":
            dedup_training_data()
        elif sys.argv[1] == "status":
            db = load_db()
            print(f"Exact entries: {db_image_count(db)}")
            print(f"pHash entries: {db_phash_count(db)}")
            animals_found = set(db.values())
            animals_missing = [a for a in ANIMALS if a not in animals_found]
            print(f"Da co: {', '.join(sorted(animals_found)) or '(trong)'}")
            print(f"Thieu: {', '.join(animals_missing) or '(du het!)'}")
        else:
            print("Dung: python captcha_solver.py [build|test|status|dedup]")
    else:
        print("Alonhadat Captcha Solver")
        print("========================")
        print("  python captcha_solver.py build [N]  - Giai tay N captcha de build DB")
        print("  python captcha_solver.py test        - Test auto solve")
        print("  python captcha_solver.py status      - Xem tien do database")
        print("  python captcha_solver.py dedup       - Don duplicate trong training_data")
