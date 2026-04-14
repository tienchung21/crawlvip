#!/usr/bin/env python3
"""
Cắt ảnh captcha full thành 4 cell riêng lẻ và lưu vào training_data/ theo label đã nhận diện.
"""

import os
from PIL import Image

# Labels đã nhận diện visual cho batch2 (50 captcha x 4 cells = 200 cells)
# Format: (captcha_number, [cell1, cell2, cell3, cell4])
LABELS = {
    1: ['trau', 'tho', 'bo', 'ngua'],
    2: ['trau', 'voi', 'cho', 'ngua'],
    3: ['ho', 'tho', 'meo', 'bo'],
    4: ['chim', 'voi', 'trau', 'ga'],
    5: ['ga', 'meo', 'heo', 'ngua'],
    6: ['vit', 'tho', 'ngua', 'chuot'],
    7: ['vit', 'voi', 'bo', 'tho'],
    8: ['trau', 'ga', 'chuot', 'cho'],
    9: ['chuot', 'bo', 'ga', 'ho'],
    10: ['ho', 'tho', 'meo', 'bo'],
    11: ['chuot', 'bo', 'ga', 'ho'],
    12: ['ho', 'bo', 'trau', 'vit'],
    13: ['bo', 'vit', 'ho', 'tho'],
    14: ['ga', 'tho', 'chim', 'chuot'],
    15: ['cho', 'voi', 'chuot', 'ngua'],
    16: ['vit', 'cho', 'ga', 'bo'],
    17: ['chuot', 'tho', 'ngua', 'ga'],
    18: ['ho', 'tho', 'meo', 'bo'],
    19: ['meo', 'tho', 'ga', 'heo'],
    20: ['cho', 'meo', 'trau', 'chim'],
    21: ['meo', 'tho', 'chuot', 'voi'],
    22: ['heo', 'trau', 'ngua', 'ga'],
    23: ['chuot', 'cho', 'meo', 'bo'],
    24: ['ngua', 'heo', 'ga', 'ho'],
    25: ['bo', 'heo', 'bo', 'vit'],
    26: ['cho', 'ga', 'heo', 'trau'],
    27: ['tho', 'voi', 'chim', 'chuot'],
    28: ['tho', 'ga', 'meo', 'chim'],
    29: ['tho', 'voi', 'chuot', 'chuot'],
    30: ['ga', 'bo', 'tho', 'heo'],
    31: ['voi', 'bo', 'chuot', 'trau'],
    32: ['ngua', 'voi', 'ga', 'chuot'],
    33: ['chim', 'voi', 'bo', 'ngua'],
    34: ['chuot', 'trau', 'chuot', 'chuot'],
    35: ['ga', 'bo', 'ngua', 'ho'],
    36: ['ga', 'meo', 'ho', 'ngua'],
    37: ['voi', 'tho', 'vit', 'chuot'],
    38: ['heo', 'tho', 'bo', 'ga'],
    39: ['tho', 'chuot', 'voi', 'bo'],
    40: ['vit', 'tho', 'chim', 'chuot'],
    41: ['ga', 'tho', 'cho', 'vit'],
    42: ['meo', 'heo', 'vit', 'tho'],
    43: ['cho', 'heo', 'bo', 'voi'],
    44: ['cho', 'voi', 'heo', 'bo'],
    45: ['tho', 'heo', 'chim', 'ho'],
    46: ['ho', 'ga', 'meo', 'voi'],
    47: ['trau', 'ngua', 'ga', 'chim'],
    48: ['ngua', 'trau', 'tho', 'chuot'],
    49: ['chim', 'bo', 'chuot', 'ga'],
    50: ['bo', 'bo', 'chim', 'ga'],
}

def cut_cells(img):
    """Cắt ảnh captcha thành 4 cells"""
    w, h = img.size
    cell_w = w // 4
    cells = []
    for i in range(4):
        x1 = i * cell_w
        x2 = (i + 1) * cell_w
        # Crop bỏ viền border (3px mỗi bên)
        border = 3
        cell = img.crop((x1 + border, border, x2 - border, h - border))
        cells.append(cell)
    return cells

def main():
    src_dir = '/home/chungnt/alonhadat-captcha/unlabeled_cells'
    dst_dir = '/home/chungnt/alonhadat-captcha/training_data'
    
    total = 0
    counts = {}
    
    for num, labels in LABELS.items():
        fname = f'batch2_{num:03d}_full.png'
        fpath = os.path.join(src_dir, fname)
        
        if not os.path.exists(fpath):
            print(f"⚠️  Không tìm thấy: {fname}")
            continue
        
        img = Image.open(fpath)
        cells = cut_cells(img)
        
        for i, (cell, label) in enumerate(zip(cells, labels)):
            label_dir = os.path.join(dst_dir, label)
            os.makedirs(label_dir, exist_ok=True)
            
            # Đếm file hiện có để tránh trùng tên
            existing = len([f for f in os.listdir(label_dir) if f.endswith('.png')])
            out_name = f'batch2_{num:03d}_cell{i+1}.png'
            out_path = os.path.join(label_dir, out_name)
            
            cell.save(out_path)
            total += 1
            counts[label] = counts.get(label, 0) + 1
    
    print(f"\n✅ Đã lưu {total} cells vào training_data/")
    print(f"\n📊 Thống kê theo loài:")
    for label in sorted(counts.keys()):
        print(f"   {label}: {counts[label]} ảnh mới")

if __name__ == '__main__':
    main()
