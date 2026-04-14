#!/usr/bin/env python3
"""Tải batch captcha, cắt cells ra file để nhận diện"""
import requests, hashlib, json, os, sys
from PIL import Image
import io, numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, "captcha_db.json")
CELLS_DIR = os.path.join(SCRIPT_DIR, "cells")

def load_db():
    try:
        with open(DB_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def cut_cells(img_bytes):
    img = Image.open(io.BytesIO(img_bytes))
    img_arr = np.array(img)
    row_mask = np.any(img_arr > 30, axis=(1, 2))
    col_mask = np.any(img_arr > 30, axis=(0, 2))
    rows = np.where(row_mask)[0]
    cols = np.where(col_mask)[0]
    if len(rows) == 0 or len(cols) == 0:
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
        cells.append(content.crop((x1, 0, x2, ch)))
    return cells, img

def hash_cell(cell):
    arr = np.array(cell)
    h, w = arr.shape[:2]
    margin = 8
    inner = cell.crop((margin, margin, w - margin, h - margin))
    inner = inner.resize((80, 80), Image.LANCZOS)
    buf = io.BytesIO()
    inner.save(buf, format='PNG')
    return hashlib.md5(buf.getvalue()).hexdigest()

def detect_border_color(cell):
    arr = np.array(cell)
    border_pixels = []
    bw = 5
    border_pixels.extend(arr[:bw, :].reshape(-1, 3).tolist())
    border_pixels.extend(arr[-bw:, :].reshape(-1, 3).tolist())
    border_pixels.extend(arr[:, :bw].reshape(-1, 3).tolist())
    border_pixels.extend(arr[:, -bw:].reshape(-1, 3).tolist())
    border_pixels = np.array(border_pixels)
    avg_r, avg_g, avg_b = np.mean(border_pixels[:, 0]), np.mean(border_pixels[:, 1]), np.mean(border_pixels[:, 2])
    return 'blue' if avg_b > 150 and avg_r < 100 and avg_g < 100 else 'other'

def batch_download(n=10):
    db = load_db()
    os.makedirs(CELLS_DIR, exist_ok=True)
    
    new_cells = []  # (hash, filepath, border_color)
    skipped = 0
    
    for i in range(n):
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        try:
            session.get("https://alonhadat.com.vn/xac-thuc-nguoi-dung.html")
            r = session.get("https://alonhadat.com.vn/ImageCaptcha.ashx?v=3")
        except Exception as e:
            print(f"  Lỗi tải captcha {i+1}: {e}")
            continue
        
        cells, full_img = cut_cells(r.content)
        if len(cells) != 4:
            continue
        
        # Lưu ảnh gốc
        full_img.save(os.path.join(CELLS_DIR, f"captcha_{i+1:03d}_full.png"))
        
        for j, cell in enumerate(cells):
            h = hash_cell(cell)
            border = detect_border_color(cell)
            
            if h in db:
                skipped += 1
                continue
            
            # Lưu cell ra file
            fname = f"captcha_{i+1:03d}_cell{j+1}_{border}_{h[:8]}.png"
            fpath = os.path.join(CELLS_DIR, fname)
            cell.save(fpath)
            new_cells.append((h, fname, border))
    
    print(f"\n📊 Kết quả tải {n} captcha:")
    print(f"   Cells mới (chưa biết): {len(new_cells)}")
    print(f"   Cells đã biết (skip): {skipped}")
    print(f"   Database hiện tại: {len(db)} hash")
    
    if new_cells:
        print(f"\n📁 Ảnh cells mới lưu tại: {CELLS_DIR}/")
        for h, fname, border in new_cells:
            b = "🔵" if border == "blue" else "🟡"
            print(f"   {b} {fname}")
    
    # Lưu danh sách cells chưa nhận diện
    pending_file = os.path.join(SCRIPT_DIR, "pending_cells.json")
    with open(pending_file, 'w') as f:
        json.dump(new_cells, f, indent=2)
    print(f"\n💾 Danh sách cells chưa nhận diện: pending_cells.json")

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    batch_download(n)
