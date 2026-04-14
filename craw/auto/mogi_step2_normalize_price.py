import pymysql
import time
import re

BATCH_SIZE = 5000

def parse_price_to_vnd(price_str):
    """
    Chuyển đổi chuỗi giá về đơn vị VNĐ (Integer).
    Ví dụ:
      - "5 tỷ" -> 5,000,000,000
      - "800 triệu" -> 800,000,000
      - "1.2 tỷ" -> 1,200,000,000
      - "2,5 tỷ" -> 2,500,000,000
      - "15 triệu/tháng" -> 15,000,000
    """
    if not price_str:
        return None

    p = str(price_str).lower().strip()

    multiplier = 1
    if 'tỷ' in p:
        multiplier = 1_000_000_000
    elif 'triệu' in p:
        multiplier = 1_000_000
    elif 'ngàn' in p or 'nghìn' in p:
        multiplier = 1_000
    elif 'đ' in p or 'vnd' in p:
        multiplier = 1
    else:
        return None

    nums = re.findall(r"[-+]?\d*[\.,]\d+|\d+", p)
    if nums:
        num_str = nums[0].replace(',', '.')
        try:
            val = float(num_str)
            return int(val * multiplier)
        except Exception:
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

    script_name = "mogi_step2_normalize_price.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    total_updated = 0

    while True:
        sql_get = f"""
            SELECT id, src_price
            FROM data_clean_v1
            WHERE domain = 'mogi'
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

    print("Finalizing step status...")
    sql_final = f"""
        UPDATE data_clean_v1
        SET process_status = 2, last_script = '{script_name}'
        WHERE domain = 'mogi'
          AND process_status = 1
          AND price_vnd IS NOT NULL
          AND price_vnd > 0
    """
    cursor.execute(sql_final)
    conn.commit()
    print(f"-> Updated process_status = 2 for {cursor.rowcount} rows.")

    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM data_clean_v1
        WHERE domain='mogi' AND process_status=1 AND (price_vnd IS NULL OR price_vnd<=0)
        """
    )
    stuck = cursor.fetchone() or {}
    print(f"Skipped (status stays 1 due to missing/invalid price_vnd): {stuck.get('cnt', 0)}")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
