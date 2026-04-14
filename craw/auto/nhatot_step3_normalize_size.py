import pymysql
import time
import re

BATCH_SIZE = 5000

def parse_size_to_m2(size_str):
    """
    Chuyển đổi chuỗi diện tích về số thực (m2).
    Ví dụ: 
      - "50 m2" -> 50.0
      - "100m2" -> 100.0
      - "80.5 m²" -> 80.5
      - "120,5m2" -> 120.5
    """
    if not size_str:
        return None
    
    s = str(size_str).lower().strip()
    
    # Lấy số đầu tiên tìm thấy
    nums = re.findall(r"[-+]?\d*[\.,]\d+|\d+", s)
    if nums:
        num_str = nums[0].replace(',', '.')
        try:
            val = float(num_str)
            if val > 0:
                return val
        except:
            return None
            
    return None

def main():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='craw_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    script_name = "nhatot_step3_normalize_size.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    # PHẦN 1: Parse diện tích
    print("--- Phase 1: Parsing size ---")
    total_size_updated = 0
    
    while True:
        sql_get = f"""
            SELECT id, src_size 
            FROM data_clean_v1 
            WHERE domain = 'nhatot'
            AND process_status = 2
            AND src_size IS NOT NULL 
            AND std_area IS NULL
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_get)
        rows = cursor.fetchall()
        
        if not rows:
            break
            
        batch_count = 0
        for row in rows:
            raw_size = row.get('src_size')
            record_id = row.get('id')
            
            std_area = parse_size_to_m2(raw_size)
            
            if std_area is not None:
                sql_update = "UPDATE data_clean_v1 SET std_area = %s WHERE id = %s"
                cursor.execute(sql_update, (std_area, record_id))
                batch_count += 1
                
        conn.commit()
        total_size_updated += batch_count
        print(f"  Batch: +{batch_count} rows (Total: {total_size_updated})")
        
        if len(rows) < BATCH_SIZE:
            break

    print(f"-> Parsed size for {total_size_updated} rows.")

    # PHẦN 2: Tính giá/m2 (price_m2 = price_vnd / std_area)
    print("--- Phase 2: Calculating price per m2 ---")
    sql_calc_m2 = """
        UPDATE data_clean_v1
        SET price_m2 = price_vnd / std_area
        WHERE domain = 'nhatot'
          AND process_status = 2
          AND price_vnd IS NOT NULL AND price_vnd > 0
          AND std_area IS NOT NULL AND std_area > 0
          AND price_m2 IS NULL
    """
    cursor.execute(sql_calc_m2)
    conn.commit()
    print(f"-> Calculated price_m2 for {cursor.rowcount} rows.")

    # PHẦN 3: Finalize - Nâng process_status lên 3
    print("--- Phase 3: Finalizing ---")
    sql_final = f"""
        UPDATE data_clean_v1 
        SET process_status = 3, last_script = '{script_name}'
        WHERE domain = 'nhatot'
        AND process_status = 2
    """
    cursor.execute(sql_final)
    conn.commit()
    print(f"-> Updated process_status = 3 for {cursor.rowcount} rows.")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
