import pymysql
import unicodedata
import re

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

def remove_accents(s):
    if not s: return ""
    s = str(s)
    s = s.replace('đ', 'd').replace('Đ', 'd') 
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

def normalize_comp(name):
    if not name: return ""
    if '(' in name: name = name.split('(')[0]
    name = re.sub(r'\s+', ' ', name)
    name = name.lower().strip()
    name = remove_accents(name)
    prefixes = ['tinh ', 'thanh pho ', 'tp ', 'tp. ', 'quan ', 'huyen ', 'thi xa ', 'tx ', 'phuong ', 'xa ', 'thi tran ', 'tt ']
    for p in prefixes:
        if name.startswith(p):
            name = name[len(p):].strip()
    return name.strip()

print("=== GENERATING WARNING REPORT ===")

# Query Nhatot Data + Mapped ID (LIMIT 500 to find enough warnings)
query = """
SELECT 
    w.name as w_name, w.cafeland_id,
    d.name as d_name,
    p.name as p_name
FROM location_detail w
LEFT JOIN location_detail d ON w.area_id = d.area_id AND d.level = 2
LEFT JOIN location_detail p ON w.region_id = p.region_id AND p.level = 1
WHERE w.level = 3 AND w.cafeland_id IS NOT NULL
ORDER BY RAND()
LIMIT 500
"""

cursor.execute(query)
samples = cursor.fetchall()

warnings = []

for nt_w, cf_id, nt_d, nt_p in samples:
    cursor.execute("""
        SELECT w.city_title, d.city_title, p.city_title
        FROM transaction_city w
        LEFT JOIN transaction_city d ON w.city_parent_id = d.city_id
        LEFT JOIN transaction_city p ON d.city_parent_id = p.city_id
        WHERE w.city_id = %s
    """, (cf_id,))
    cf_row = cursor.fetchone()
    
    if not cf_row: continue
        
    cf_w, cf_d, cf_p = cf_row
    
    # Compare
    p1, p2 = normalize_comp(nt_p), normalize_comp(cf_p)
    d1, d2 = normalize_comp(nt_d), normalize_comp(cf_d)
    w1, w2 = normalize_comp(nt_w), normalize_comp(cf_w)
    
    reasons = []
    
    # Prov Check
    if 'ho chi minh' in p1 and 'ho chi minh' in p2: pass
    elif p1 != p2 and p1 not in p2 and p2 not in p1:
        reasons.append(f"Tỉnh lệch: '{nt_p}' vs '{cf_p}'")

    # Dist Check
    if d1 != d2 and d1 not in d2 and d2 not in d1:
        # Ignore Thu Duc merge
        if ('thu duc' in d1 and 'quan 2' in d2) or ('thu duc' in d1 and 'quan 9' in d2) or ('thu duc' in d1 and 'thu duc' in d2): pass
        else:
             reasons.append(f"Huyện lệch: '{nt_d}' vs '{cf_d}'")

    # Ward Check
    if w1 != w2 and w1 not in w2 and w2 not in w1:
         reasons.append(f"Xã lệch: '{nt_w}' vs '{cf_w}'")
    
    if reasons:
        warnings.append({
            'source': f"{nt_w} ({nt_d}, {nt_p})",
            'target': f"{cf_w} ({cf_d}, {cf_p})",
            'reasons': "; ".join(reasons)
        })
        if len(warnings) >= 20: break # Just show top 20 warnings

# Report
with open("warning_report.txt", "w", encoding="utf-8") as f:
    f.write(f"=== DANH SACH SAI LECH (WARNINGS) ===\n")
    if not warnings:
        f.write("Khong tim thay warning nao trong 500 mau ngau nhien!\n")
    else:
        for idx, w in enumerate(warnings):
            f.write(f"{idx+1}. {w['reasons']}\n")
            f.write(f"   - Nhatot:   {w['source']}\n")
            f.write(f"   - Cafeland: {w['target']}\n")
            f.write("-" * 50 + "\n")

print(f"Found {len(warnings)} warnings.")
cursor.close()
conn.close()
