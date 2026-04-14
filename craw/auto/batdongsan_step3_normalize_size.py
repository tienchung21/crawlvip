
import pymysql
import time
import re

BATCH_SIZE = 5000
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def parse_area(size_str):
    """
    Parse area string to float.
    Example: "84,56 m²" -> 84.56
    """
    if not size_str:
        return None

    s = str(size_str).lower()
    s = s.replace("m²", "").replace("m2", "").replace("㎡", "")
    s = s.strip()

    # Keep only first numeric token with possible thousand/decimal separators.
    m = re.search(r"[-+]?\d[\d\.,]*", s)
    if not m:
        return None
    token = m.group(0)
    token = token.replace(" ", "")

    has_dot = "." in token
    has_comma = "," in token

    # Case 1: both separators exist -> infer decimal separator by last occurrence.
    if has_dot and has_comma:
        if token.rfind(",") > token.rfind("."):
            # 1.234,56 -> 1234.56
            normalized = token.replace(".", "").replace(",", ".")
        else:
            # 1,234.56 -> 1234.56
            normalized = token.replace(",", "")
    # Case 2: only dot
    elif has_dot:
        # 1.200 or 12.345.678 -> thousand separators
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", token):
            normalized = token.replace(".", "")
        else:
            normalized = token
    # Case 3: only comma
    elif has_comma:
        # 1,200 or 12,345,678 -> thousand separators
        if re.fullmatch(r"\d{1,3}(,\d{3})+", token):
            normalized = token.replace(",", "")
        else:
            # 84,56 -> 84.56
            normalized = token.replace(",", ".")
    else:
        normalized = token

    try:
        val = float(normalized)
    except Exception:
        return None

    if val <= 0:
        return None
    return val

def main():
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    script_name = "batdongsan_step3_normalize_size.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    total_updated = 0
    
    while True:
        # Get Batch (Status = 2 from Step 2)
        # We only process Batdongsan domain
        sql_get = f"""
            SELECT id, src_size, price_vnd 
            FROM data_clean_v1 
            WHERE domain = 'batdongsan.com.vn'
            AND process_status = 2 
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_get)
        rows = cursor.fetchall()
        
        if not rows:
            break
            
        batch_count = 0
        for row in rows:
            record_id = row.get('id')
            raw_size = row.get('src_size')
            price_vnd = row.get('price_vnd')
            
            # 1. Parse Area
            std_area = parse_area(raw_size)
            
            # 2. Calculate Price/m2
            price_m2 = None
            if std_area and std_area > 0 and price_vnd and price_vnd > 0:
                try:
                    price_m2 = int(price_vnd / std_area)
                except:
                    price_m2 = None
            
            # Update
            # We update std_area, price_m2, process_status=3 and last_script
            sql_update = """
                UPDATE data_clean_v1 
                SET std_area = %s, price_m2 = %s, process_status = 3, last_script = %s
                WHERE id = %s
            """
            cursor.execute(sql_update, (std_area, price_m2, script_name, record_id))
            batch_count += 1
                
        conn.commit()
        total_updated += batch_count
        print(f"  Batch: +{batch_count} rows (Total: {total_updated})")
        
        if len(rows) < BATCH_SIZE:
            break

    print(f"-> Parsed area for {total_updated} rows.")
    # Final Update Removed (Done in batch)
    print("=== Finished ===")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
