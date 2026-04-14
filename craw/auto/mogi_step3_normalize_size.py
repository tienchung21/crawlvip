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
    nums = re.findall(r"[-+]?\d*[\.,]\d+|\d+", s)
    if nums:
        num_str = nums[0].replace(',', '.')
        try:
            val = float(num_str)
            if val > 0:
                return val
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

    script_name = "mogi_step3_normalize_size.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    # PHASE 1: Parse diện tích
    print("--- Phase 1: Parsing size ---")
    total_size_updated = 0

    # Fast path: do the same "first number token" extraction in SQL (MariaDB supports REGEXP_SUBSTR).
    # This preserves existing logic:
    # - take first numeric token (supports '.' or ',')
    # - replace ',' -> '.'
    # - cast to float and keep only > 0
    try:
        while True:
            sql_update = f"""
                UPDATE data_clean_v1
                SET std_area = CAST(
                    REPLACE(
                        REGEXP_SUBSTR(LOWER(src_size), '[-+]?[0-9]*[\\\\.,][0-9]+|[0-9]+'),
                        ',', '.'
                    ) AS DECIMAL(18,4)
                )
                WHERE domain = 'mogi'
                  AND process_status = 2
                  AND src_size IS NOT NULL AND src_size <> ''
                  AND std_area IS NULL
                  AND REGEXP_SUBSTR(LOWER(src_size), '[-+]?[0-9]*[\\\\.,][0-9]+|[0-9]+') IS NOT NULL
                  AND CAST(
                        REPLACE(
                            REGEXP_SUBSTR(LOWER(src_size), '[-+]?[0-9]*[\\\\.,][0-9]+|[0-9]+'),
                            ',', '.'
                        ) AS DECIMAL(18,4)
                    ) > 0
                LIMIT {BATCH_SIZE}
            """
            cursor.execute(sql_update)
            rows = cursor.rowcount
            conn.commit()
            if rows == 0:
                break
            total_size_updated += rows
            print(f"  Batch: +{rows} rows (Total: {total_size_updated})")
            if rows < BATCH_SIZE:
                break
    except Exception as e:
        # Fallback: keep Python logic but reduce DB round-trips (executemany).
        print(f"[WARN] SQL fast-path failed ({e}). Falling back to Python parsing...")
        while True:
            sql_get = f"""
                SELECT id, src_size
                FROM data_clean_v1
                WHERE domain = 'mogi'
                  AND process_status = 2
                  AND src_size IS NOT NULL
                  AND std_area IS NULL
                LIMIT {BATCH_SIZE}
            """
            cursor.execute(sql_get)
            rows = cursor.fetchall()

            if not rows:
                break

            updates = []
            for row in rows:
                raw_size = row.get('src_size')
                record_id = row.get('id')
                std_area = parse_size_to_m2(raw_size)
                if std_area is not None:
                    updates.append((std_area, record_id))

            if updates:
                cursor.executemany("UPDATE data_clean_v1 SET std_area=%s WHERE id=%s", updates)
                conn.commit()
                total_size_updated += len(updates)
                print(f"  Batch: +{len(updates)} rows (Total: {total_size_updated})")
            else:
                # Nothing parsed in this batch; avoid infinite loop.
                break

    print(f"-> Parsed size for {total_size_updated} rows.")

    # PHASE 2: Tính giá/m2
    print("--- Phase 2: Calculating price per m2 ---")
    sql_calc_m2 = """
        UPDATE data_clean_v1
        SET price_m2 = price_vnd / std_area
        WHERE domain = 'mogi'
          AND process_status = 2
          AND price_vnd IS NOT NULL AND price_vnd > 0
          AND std_area IS NOT NULL AND std_area > 0
          AND price_m2 IS NULL
    """
    cursor.execute(sql_calc_m2)
    conn.commit()
    print(f"-> Calculated price_m2 for {cursor.rowcount} rows.")

    # PHASE 3: Finalize
    print("--- Phase 3: Finalizing ---")
    sql_final = f"""
        UPDATE data_clean_v1
        SET process_status = 3, last_script = '{script_name}'
        WHERE domain = 'mogi'
          AND process_status = 2
          AND price_vnd IS NOT NULL AND price_vnd > 0
          AND std_area IS NOT NULL AND std_area > 0
          AND price_m2 IS NOT NULL AND price_m2 > 0
    """
    cursor.execute(sql_final)
    conn.commit()
    print(f"-> Updated process_status = 3 for {cursor.rowcount} rows.")

    cursor.execute(
        """
        SELECT
          SUM(price_vnd IS NULL OR price_vnd<=0) AS missing_price,
          SUM(std_area IS NULL OR std_area<=0) AS missing_area,
          SUM(price_m2 IS NULL OR price_m2<=0) AS missing_price_m2,
          COUNT(*) AS total_status2
        FROM data_clean_v1
        WHERE domain='mogi' AND process_status=2
        """
    )
    r = cursor.fetchone() or {}
    print(
        "Skipped (status stays 2 due to missing std_area/price_m2): "
        f"total={r.get('total_status2', 0)}, "
        f"missing_price={r.get('missing_price', 0)}, "
        f"missing_area={r.get('missing_area', 0)}, "
        f"missing_price_m2={r.get('missing_price_m2', 0)}"
    )

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
