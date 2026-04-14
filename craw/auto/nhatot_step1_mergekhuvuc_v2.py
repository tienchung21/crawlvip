import pymysql
import time

BATCH_SIZE = 5000

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

    script_name = "nhatot_step1_mergekhuvuc_v2.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    # 1. Update Province (Level 1) - Batch processing
    print("Updating cf_province_id...")
    total_p = 0
    while True:
        sql_province = f"""
        UPDATE data_clean_v1 d
        JOIN location_detail l ON d.src_province_id = l.region_id
        SET d.cf_province_id = l.cafeland_id
        WHERE l.level = 1 AND l.cafeland_id IS NOT NULL 
          AND d.domain = 'nhatot'
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

    # 2. Update District (Level 2) - Batch processing
    print("Updating cf_district_id...")
    total_d = 0
    while True:
        sql_district = f"""
        UPDATE data_clean_v1 d
        JOIN location_detail l ON d.src_district_id = l.area_id AND d.src_province_id = l.region_id
        SET d.cf_district_id = l.cafeland_id
        WHERE l.level = 2 AND l.cafeland_id IS NOT NULL 
          AND d.domain = 'nhatot'
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

    # 3. Update Ward (Level 3) - Batch processing
    print("Updating cf_ward_id...")
    total_w = 0
    while True:
        sql_ward = f"""
        UPDATE data_clean_v1 d
        JOIN location_detail l ON d.src_ward_id = l.ward_id AND d.src_district_id = l.area_id AND d.src_province_id = l.region_id
        LEFT JOIN transaction_city_merge m ON l.cafeland_id = m.old_city_id
        SET d.cf_ward_id = COALESCE(m.new_city_id, l.cafeland_id)
        WHERE l.level = 3 AND l.cafeland_id IS NOT NULL 
          AND d.domain = 'nhatot'
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
    WHERE domain = 'nhatot'
      AND process_status = 0
    """
    cursor.execute(sql_final)
    total_finalized = cursor.rowcount
    conn.commit()
    
    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")
    print(f"Total rows finalized: {total_finalized}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
