
import pymysql
import time
import re

BATCH_SIZE = 5000
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def parse_price_to_vnd(price_str):
    """
    Chuyển đổi chuỗi giá về đơn vị VNĐ (Integer).
    Logic Batdongsan:
      - "5,5 tỷ" -> 5,5 * 1e9 -> 5,500,000,000
      - "800 triệu" -> 800 * 1e6 -> 800,000,000
      - "Thỏa thuận" -> 0
      - "25 triệu/tháng" -> 25 * 1e6 -> 25,000,000
    """
    if not price_str:
        return None
    
    p = str(price_str).lower().strip()
    
    # 1. Handle "Thỏa thuận"
    if "thỏa thuận" in p:
        return 0
        
    # 2. Identify Multiplier
    multiplier = 1
    if 'tỷ' in p:
        multiplier = 1_000_000_000
    elif 'triệu' in p:
        multiplier = 1_000_000
    elif 'ngàn' in p or 'nghìn' in p:
        multiplier = 1_000
    elif 'đ' in p or 'vnd' in p:
        multiplier = 1
        
    # 3. Extract Number
    # Regex to find numbers like "5.5", "5,5", "100"
    # Matches: Optional +/- sign, digits, optional separator (.|,), digits
    nums = re.findall(r"[-+]?\d*[\.,]\d+|\d+", p)
    if nums:
        # Take the first number found
        num_str = nums[0].replace(',', '.') # Standardize decimal to dot
        try:
            val = float(num_str)
            return int(val * multiplier)
        except:
            return None
            
    return None

def main():
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor # Use DictCursor for easier row access
    )
    cursor = conn.cursor()

    script_name = "batdongsan_step2_normalize_price.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    total_updated = 0
    
    while True:
        # Get Batch (Status = 1 from Step 1)
        # We only process Batdongsan domain
        sql_get = f"""
            SELECT id, src_price 
            FROM data_clean_v1 
            WHERE domain = 'batdongsan.com.vn'
            AND process_status = 1 
            AND src_price IS NOT NULL 
            AND price_vnd IS NULL
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_get)
        rows = cursor.fetchall()
        
        if not rows:
            break
            
        batch_count = 0
        for row in rows:
            raw_price = row.get('src_price')
            record_id = row.get('id')
            
            price_vnd = parse_price_to_vnd(raw_price)
            
            # If price_vnd is identified (including 0), update it
            if price_vnd is not None:
                sql_update = "UPDATE data_clean_v1 SET price_vnd = %s WHERE id = %s"
                cursor.execute(sql_update, (price_vnd, record_id))
                batch_count += 1
                
        conn.commit()
        total_updated += batch_count
        print(f"  Batch: +{batch_count} rows (Total: {total_updated})")
        
        if len(rows) < BATCH_SIZE:
            break

    print(f"-> Parsed price for {total_updated} rows.")

    # Finalize: process_status 1 -> 2
    # Only advance rows with valid positive price_vnd.
    # Rows parsed as 0/NULL stay at Step 1 for manual review or separate handling.
    print("Finalizing step status (only price_vnd > 0)...")
    sql_final = f"""
        UPDATE data_clean_v1 
        SET process_status = 2, 
            last_script = '{script_name}'
        WHERE domain = 'batdongsan.com.vn'
          AND process_status = 1
          AND price_vnd IS NOT NULL
          AND price_vnd > 0
    """
    cursor.execute(sql_final)
    conn.commit()
    print(f"-> Updated process_status = 2 for {cursor.rowcount} rows.")

    cursor.execute(
        """
        SELECT COUNT(*) AS c
        FROM data_clean_v1
        WHERE domain='batdongsan.com.vn'
          AND process_status=1
          AND (price_vnd IS NULL OR price_vnd <= 0)
        """
    )
    remain = cursor.fetchone() or {}
    print(f"-> Kept at process_status = 1 (price_vnd NULL/<=0): {remain.get('c', 0)} rows.")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
