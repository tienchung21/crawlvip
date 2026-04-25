import re
import pymysql

ids = set()
with open('report_data/ward_order.txt') as f:
    for line in f:
        m = re.match(r'^(\d+)', line.strip())
        if m:
            ids.add(int(m.group(1)))

try:
    conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db')
    with conn.cursor() as cur:
        cur.execute("SELECT new_city_id, new_city_name FROM transaction_city_merge WHERE new_city_parent_id > 0")
        db_wards = {r[0]: r[1] for r in cur.fetchall()}
        
    missing = [(wid, name) for wid, name in db_wards.items() if wid not in ids]
    print(f"Total in order.txt: {len(ids)}")
    print(f"Total in DB: {len(db_wards)}")
    print(f"Missing in order.txt ({len(missing)}):")
    for wid, name in missing:
        print(f"  {wid} - {name}")
finally:
    conn.close()
