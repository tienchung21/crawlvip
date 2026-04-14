#!/usr/bin/env python3
"""Cập nhật database từ kết quả nhận diện"""
import json, os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(SCRIPT_DIR, "captcha_db.json")

def load_db():
    try:
        with open(DB_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

# Mapping: captcha_number -> [con1, con2, con3, con4] theo thứ tự ô 1-4
# Nhận diện từ ảnh captcha_XXX_full.png
identifications = {
    1: ["ho", "ga", "trau", "vit"],      # hổ(xanh), gà, trâu, vịt
    2: ["ho", "tho", "trau", "heo"],     # hổ, thỏ, trâu, heo
    3: ["ho", "ga", "tho", "meo"],       # hổ(xanh), gà, thỏ, mèo
    4: ["meo", "ga", "heo", "chim"],     # mèo, gà(xanh), heo, chim
    5: ["meo", "tho", "voi", "heo"],     # mèo, thỏ, voi(xanh), heo
    6: ["cho", "meo", "ngua", "chim"],   # chó, mèo, ngựa(xanh), chim
    7: ["chim", "chuot", "ho", "meo"],   # chim, chuột, hổ(xanh), mèo
    8: ["chim", "heo", "meo", "chuot"],  # chim(xanh), heo, mèo, chuột
    9: ["meo", "bo", "chuot", "chim"],   # mèo, bò(xanh), chuột, chim
    10: ["vit", "bo", "ga", "heo"],      # vịt, bò(xanh), gà, heo
    11: ["ga", "bo", "tho", "ngua"],     # gà, bò, thỏ(xanh), ngựa
    12: ["meo", "chuot", "ga", "heo"],   # mèo, chuột, gà(xanh), heo
    13: ["trau", "tho", "chim", "voi"],  # trâu, thỏ, chim(xanh), voi
    14: ["ngua", "tho", "meo", "bo"],    # ngựa, thỏ(xanh), mèo, bò
    15: ["chim", "tho", "voi", "chuot"], # chim, thỏ, voi, chuột(xanh)
}

# Load pending cells
pending_file = os.path.join(SCRIPT_DIR, "pending_cells.json")
with open(pending_file, 'r') as f:
    pending = json.load(f)

db = load_db()
added = 0

for cell_hash, fname, border in pending:
    # Parse captcha number and cell number from filename
    # Format: captcha_001_cell1_blue_61d3f69a.png
    parts = fname.split('_')
    captcha_num = int(parts[1])
    cell_num = int(parts[2].replace('cell', ''))
    
    if captcha_num in identifications:
        animal = identifications[captcha_num][cell_num - 1]
        if cell_hash not in db:
            db[cell_hash] = animal
            added += 1
            print(f"  ✅ {fname} → {animal}")
        else:
            print(f"  ✓  {fname} → {db[cell_hash]} (đã có)")

# Save
with open(DB_FILE, 'w') as f:
    json.dump(db, f, indent=2, ensure_ascii=False)

print(f"\n📊 Đã thêm: {added} hash mới")
print(f"   Database tổng: {len(db)} hash")

# Stats
animals_found = set(db.values())
ANIMALS = ["bo", "chim", "cho", "chuot", "ga", "heo", "ho", "meo", "ngua", "tho", "trau", "vit", "voi"]
missing = [a for a in ANIMALS if a not in animals_found]
print(f"   Con vật đã có: {', '.join(sorted(animals_found))}")
print(f"   Còn thiếu: {', '.join(missing) or 'ĐỦ HẾT! 🎉'}")
