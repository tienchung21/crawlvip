import pymysql
import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print("=== homedy_step3_normalize_size.py ===")

    start = time.time()
    script_name = 'homedy_step3_normalize_size.py'

    # Điều chỉnh: Dùng IF/ELSE trong Update. Nếu diện tích rỗng -> process_status = -3
    cursor.execute(
        """
        UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
        SET
            std_area = CASE
                WHEN REPLACE(TRIM(src_size), ',', '.') REGEXP '^[0-9]+(\\\.[0-9]+)?$'
                     AND CAST(REPLACE(TRIM(src_size), ',', '.') AS DECIMAL(18,2)) > 0
                THEN CAST(REPLACE(TRIM(src_size), ',', '.') AS DECIMAL(18,2))
                ELSE NULL
            END,
            unit = CASE
                WHEN REPLACE(TRIM(src_size), ',', '.') REGEXP '^[0-9]+(\\\.[0-9]+)?$'
                     AND CAST(REPLACE(TRIM(src_size), ',', '.') AS DECIMAL(18,4)) > 0
                THEN 'm2'
                ELSE unit
            END,
            price_m2 = CASE
                WHEN price_vnd IS NOT NULL
                     AND REPLACE(TRIM(src_size), ',', '.') REGEXP '^[0-9]+(\\\.[0-9]+)?$'
                     AND CAST(REPLACE(TRIM(src_size), ',', '.') AS DECIMAL(18,4)) > 0
                THEN price_vnd / CAST(REPLACE(TRIM(src_size), ',', '.') AS DECIMAL(18,4))
                ELSE NULL
            END,
            process_status = CASE
                WHEN REPLACE(TRIM(src_size), ',', '.') REGEXP '^[0-9]+(\\\.[0-9]+)?$'
                     AND CAST(REPLACE(TRIM(src_size), ',', '.') AS DECIMAL(18,4)) > 0
                THEN 3
                ELSE -3
            END,
            last_script = %s
        WHERE domain = 'homedy.com'
          AND process_status = 2
        ORDER BY id
        LIMIT %s
        """,
        (script_name, args.limit)
    )
    updated = cursor.rowcount
    conn.commit()
    
    print(f"-> Normalized size, price_m2 and set status (3 or -3) for {updated} homedy records in {time.time()-start:.2f}s.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
