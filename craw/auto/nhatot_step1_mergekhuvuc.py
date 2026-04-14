import pymysql
import time

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

    script_name = "nhatot_step1_mergekhuvuc.py"
    print(f"=== Running {script_name} ===")

    # Start timing
    start_time = time.time()

    # 1. Update Province (Level 1)
    print("Updating cf_province_id...")
    sql_province = """
    UPDATE data_clean_v1 d
    JOIN location_detail l ON d.src_province_id = l.region_id
    SET d.cf_province_id = l.cafeland_id
    WHERE l.level = 1 AND l.cafeland_id IS NOT NULL AND d.process_status = 0
    """
    cursor.execute(sql_province)
    p_rows = cursor.rowcount
    conn.commit()
    print(f"-> Updated {p_rows} provinces.")

    # 2. Update District (Level 2)
    print("Updating cf_district_id...")
    sql_district = """
    UPDATE data_clean_v1 d
    JOIN location_detail l ON d.src_district_id = l.area_id AND d.src_province_id = l.region_id
    SET d.cf_district_id = l.cafeland_id
    WHERE l.level = 2 AND l.cafeland_id IS NOT NULL AND d.process_status = 0
    """
    cursor.execute(sql_district)
    d_rows = cursor.rowcount
    conn.commit()
    print(f"-> Updated {d_rows} districts.")

    # 3. Update Ward (Level 3)
    print("Updating cf_ward_id...")
    sql_ward = """
    UPDATE data_clean_v1 d
    JOIN location_detail l ON d.src_ward_id = l.ward_id AND d.src_district_id = l.area_id AND d.src_province_id = l.region_id
    SET d.cf_ward_id = l.cafeland_id
    WHERE l.level = 3 AND l.cafeland_id IS NOT NULL AND d.process_status = 0
    """
    cursor.execute(sql_ward)
    w_rows = cursor.rowcount
    conn.commit()
    print(f"-> Updated {w_rows} wards.")

    # 4. Finalize Step
    print("Finalizing step status and tracking...")
    sql_final = f"""
    UPDATE data_clean_v1 
    SET process_status = 1, 
        last_script = '{script_name}'
    WHERE process_status = 0
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
