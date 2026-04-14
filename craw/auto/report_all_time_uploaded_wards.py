import pymysql
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT / 'report_data'
REPORT_DIR.mkdir(parents=True, exist_ok=True)

in_file = ROOT / 'uploaded_by_ward_full_no_full_pretty.txt'
out_file = REPORT_DIR / 'uploaded_by_ward_full_no_full_updated.txt'

ordered_wards = []
stats = {}

with in_file.open('r', encoding='utf-8') as f:
    for line in f:
        # Bỏ qua các dòng tiêu đề hoặc kẻ ngang
        if line.startswith('---') or '-+-' in line or 'ward_id' in line:
            continue
            
        parts = [x.strip() for x in line.split('|')]
        if len(parts) >= 4 and parts[0].isdigit():
            try:
                w_id = int(parts[2])
                if w_id not in stats:
                    ordered_wards.append(w_id)
                    stats[w_id] = {
                        'p_id': parts[0],
                        'p_name': parts[1],
                        'w_id': parts[2],
                        'w_name': parts[3],
                        'df': 0,
                        'dnf': 0
                    }
            except ValueError:
                continue

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ward_id, COUNT(*) as c
            FROM data_full
            WHERE images_status = 'LISTING_UPLOADED'
              AND ward_id IS NOT NULL
            GROUP BY ward_id
        """)
        for r in cur.fetchall():
            wid = r['ward_id']
            if wid in stats:
                stats[wid]['df'] += r['c']
                
        cur.execute("""
            SELECT ward_id, COUNT(*) as c
            FROM data_no_full
            WHERE images_status = 'LISTING_UPLOADED'
              AND ward_id IS NOT NULL
            GROUP BY ward_id
        """)
        for r in cur.fetchall():
            wid = r['ward_id']
            if wid in stats:
                stats[wid]['dnf'] += r['c']
finally:
    conn.close()

with out_file.open('w', encoding='utf-8') as f:
    f.write(f"{'province_id':<11} | {'province_name':<15} | {'ward_id':<7} | {'ward_name':<23} | {'data_full_uploaded':<18} | {'data_no_full_uploaded':<21} | {'total_uploaded'}\n")
    f.write("-" * 11 + "-+-" + "-" * 15 + "-+-" + "-" * 7 + "-+-" + "-" * 23 + "-+-" + "-" * 18 + "-+-" + "-" * 21 + "-+-" + "-" * 15 + "\n")
    for wid in ordered_wards:
        s = stats[wid]
        total = s['df'] + s['dnf']
        f.write(f"{s['p_id']:<11} | {s['p_name']:<15} | {s['w_id']:<7} | {s['w_name']:<23} | {s['df']:<18} | {s['dnf']:<21} | {total}\n")

print(f"\nĐã lưu kết quả theo Xã All-Time tại: {out_file}")
print("Trích xuất 5 xã đầu tiên:")
print(f"{'province_name':<15} | {'ward_name':<23} | DF | DNF | TOTAL")
for wid in ordered_wards[:5]:
    s = stats[wid]
    total = s['df'] + s['dnf']
    print(f"{s['p_name']:<15} | {s['w_name']:<23} | {s['df']} | {s['dnf']} | {total}")
