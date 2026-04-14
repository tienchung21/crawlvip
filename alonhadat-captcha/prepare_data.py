#!/usr/bin/env python3
"""
Bước 1: Tổ chức training data từ cells đã có (dùng captcha_db.json label)
Bước 2: Tải thêm captcha, cắt cells lưu vào thư mục chưa label
"""
import json, os, shutil
from PIL import Image
import hashlib, io, numpy as np
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, "captcha_db.json")
CELLS_DIR = os.path.join(SCRIPT_DIR, "cells")
TRAIN_DIR = os.path.join(SCRIPT_DIR, "training_data")
UNLABELED_DIR = os.path.join(SCRIPT_DIR, "unlabeled_cells")

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

def organize_existing():
    """Tổ chức cells đã label thành training folders"""
    db = load_db()
    
    # Tạo thư mục cho từng con vật
    animals = ["bo", "chim", "cho", "chuot", "ga", "heo", "ho", "meo", "ngua", "tho", "trau", "vit", "voi"]
    for animal in animals:
        os.makedirs(os.path.join(TRAIN_DIR, animal), exist_ok=True)
    
    # Duyệt cells đã có
    if not os.path.exists(CELLS_DIR):
        print("Không có thư mục cells/")
        return
    
    copied = 0
    for fname in os.listdir(CELLS_DIR):
        if '_cell' not in fname or not fname.endswith('.png'):
            continue
        
        # Lấy hash từ filename
        parts = fname.replace('.png', '').split('_')
        short_hash = parts[-1]
        
        # Tìm animal name từ db
        animal = None
        for h, name in db.items():
            if h.startswith(short_hash):
                animal = name
                break
        
        if animal and animal in animals:
            src = os.path.join(CELLS_DIR, fname)
            dst = os.path.join(TRAIN_DIR, animal, fname)
            shutil.copy2(src, dst)
            copied += 1
    
    print(f"✅ Đã copy {copied} cells vào training_data/")
    for animal in animals:
        cnt = len(os.listdir(os.path.join(TRAIN_DIR, animal)))
        print(f"   {animal}: {cnt} ảnh")

def download_more(n=50):
    """Tải thêm captcha, lưu full image + cắt cells"""
    os.makedirs(UNLABELED_DIR, exist_ok=True)
    
    count = 0
    total_cells = 0
    for i in range(n):
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        try:
            session.get("https://alonhadat.com.vn/xac-thuc-nguoi-dung.html")
            r = session.get("https://alonhadat.com.vn/ImageCaptcha.ashx?v=3")
        except:
            continue
        
        cells, full_img = cut_cells(r.content)
        if len(cells) != 4:
            continue
        
        # Lưu full image
        full_img.save(os.path.join(UNLABELED_DIR, f"batch2_{i+1:03d}_full.png"))
        
        # Lưu từng cell
        for j, cell in enumerate(cells):
            h = hash_cell(cell)[:8]
            cell.save(os.path.join(UNLABELED_DIR, f"batch2_{i+1:03d}_cell{j+1}_{h}.png"))
            total_cells += 1
        
        count += 1
    
    print(f"✅ Đã tải {count} captcha, {total_cells} cells")
    print(f"   Lưu tại: {UNLABELED_DIR}/")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'download':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        download_more(n)
    else:
        organize_existing()
