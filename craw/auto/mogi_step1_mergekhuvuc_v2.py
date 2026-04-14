import pymysql
import time

BATCH_SIZE = 10000

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

    script_name = "mogi_step1_mergekhuvuc_v2.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    # 1. Update Province (CITY)
    print("Updating cf_province_id (CITY)...")
    total_p = 0
    while True:
        sql_province = f"""
        UPDATE data_clean_v1 d
        JOIN location_mogi l ON d.src_province_id = l.mogi_id
        SET d.cf_province_id = l.cafeland_id
        WHERE l.type = 'CITY' AND l.cafeland_id IS NOT NULL
          AND d.domain = 'mogi'
          AND d.process_status = 0
          AND d.cf_province_id IS NULL
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_province)
        rows = cursor.rowcount
        conn.commit()
        total_p += rows
        print(f"  Batch: +{rows} rows (Total: {total_p})")
        if rows < BATCH_SIZE:
            break
    print(f"-> Updated {total_p} provinces.")

    # 2. Update District (DISTRICT)
    print("Updating cf_district_id (DISTRICT)...")
    total_d = 0
    while True:
        sql_district = f"""
        UPDATE data_clean_v1 d
        JOIN location_mogi l ON d.src_district_id = l.mogi_id
        SET d.cf_district_id = l.cafeland_id
        WHERE l.type = 'DISTRICT' AND l.cafeland_id IS NOT NULL
          AND d.domain = 'mogi'
          AND d.process_status = 0
          AND d.cf_district_id IS NULL
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_district)
        rows = cursor.rowcount
        conn.commit()
        total_d += rows
        print(f"  Batch: +{rows} rows (Total: {total_d})")
        if rows < BATCH_SIZE:
            break
    print(f"-> Updated {total_d} districts.")

    # 3. Update Ward (WARD)
    print("Updating cf_ward_id (WARD)...")
    total_w = 0
    while True:
        sql_ward = f"""
        UPDATE data_clean_v1 d
        JOIN location_mogi l ON d.src_ward_id = l.mogi_id
        SET d.cf_ward_id = COALESCE(l.cafeland_new_id, l.cafeland_id)
        WHERE l.type = 'WARD' AND l.cafeland_id IS NOT NULL
          AND d.domain = 'mogi'
          AND d.process_status = 0
          AND d.cf_ward_id IS NULL
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_ward)
        rows = cursor.rowcount
        conn.commit()
        total_w += rows
        print(f"  Batch: +{rows} rows (Total: {total_w})")
        if rows < BATCH_SIZE:
            break
    print(f"-> Updated {total_w} wards.")

    # 4. Finalize Step
    print("Finalizing step status and tracking...")
    sql_final = f"""
    UPDATE data_clean_v1
    SET process_status = 1,
        last_script = '{script_name}'
    WHERE domain = 'mogi'
      AND process_status = 0
      AND cf_province_id IS NOT NULL
      AND cf_district_id IS NOT NULL
      AND cf_ward_id IS NOT NULL
    """
    cursor.execute(sql_final)
    total_finalized = cursor.rowcount
    conn.commit()

    # Report how many rows are still stuck at status=0 due to missing mapping.
    cursor.execute(
        """
        SELECT
          SUM(cf_province_id IS NULL) AS missing_province,
          SUM(cf_district_id IS NULL) AS missing_district,
          SUM(cf_ward_id IS NULL) AS missing_ward,
          COUNT(*) AS total_status0
        FROM data_clean_v1
        WHERE domain='mogi' AND process_status=0
        """
    )
    r = cursor.fetchone() or {}
    print(
        "Skipped (status stays 0 due to missing khu vuc): "
        f"total={r.get('total_status0', 0)}, "
        f"missing_province={r.get('missing_province', 0)}, "
        f"missing_district={r.get('missing_district', 0)}, "
        f"missing_ward={r.get('missing_ward', 0)}"
    )

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")
    print(f"Total rows finalized: {total_finalized}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
