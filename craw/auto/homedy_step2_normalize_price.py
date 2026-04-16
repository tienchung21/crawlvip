import pymysql
import argparse
import time
import re

def parse_price(price_str):
    if not price_str: return None
    price_str = price_str.lower().strip()
    if 'thương lượng' in price_str or 'thỏa thuận' in price_str:
        return None
        
    price_str = price_str.replace(',', '.')
    
    if re.match(r'^[0-9]+(\.[0-9]+)?$', price_str):
        val = float(price_str)
        if val > 9223372036854775807: return None
        return val
        
    num_match = re.search(r'([0-9\.]+)', price_str)
    if not num_match: return None
    
    try:
        val = float(num_match.group(1))
    except ValueError:
        return None
    
    if 'tỷ' in price_str: val *= 1e9
    elif 'triệu' in price_str: val *= 1e6
    elif 'nghìn' in price_str or 'ngàn' in price_str: val *= 1e3
    
    if val > 9223372036854775807:
        return None
        
    return val

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print("=== homedy_step2_normalize_price.py ===")
    
    sql = "SELECT id, src_price FROM data_clean_v1 WHERE domain = 'homedy.com' AND process_status = 1 ORDER BY id LIMIT %s"
    start = time.time()
    cursor.execute(sql, (args.limit,))
    rows = cursor.fetchall()
    
    if not rows:
        print("No rows to process.")
        return

    update_query = "UPDATE data_clean_v1 SET price_vnd = %s, process_status = %s, last_script = 'homedy_step2_normalize_price.py' WHERE id = %s"
    update_data = []

    for r in rows:
        val = parse_price(r['src_price'])
        if val is not None and val > 0:
            update_data.append((val, 2, r['id'])) # Thành công, sang bước 2
        else:
            update_data.append((None, -2, r['id'])) # Báo lỗi, status -2
            
    if update_data:
        cursor.executemany(update_query, update_data)
    conn.commit()
    
    print(f"-> Normalized price and updated status (2 or -2) for {len(update_data)} homedy records in {time.time()-start:.2f}s.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
