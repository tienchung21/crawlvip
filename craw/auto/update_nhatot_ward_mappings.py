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

    print("=== Starting Update Nhatot IDs to NEW IDs (from transaction_city_merge) ===")
    print("Target: data_clean_v1 (domain='nhatot')")
    start_time = time.time()

    # 1. Update WARD (Quan trọng nhất)
    # Mapping Old ID -> New ID based on transaction_city_merge
    print("\n1. Updating WARD IDs to NEW IDs...")
    total_w = 0
    while True:
        # Update records where current ID matches old_city_id in merge table
        sql_ward = f"""
        UPDATE data_clean_v1 d
        JOIN transaction_city_merge m ON d.cf_ward_id = m.old_city_id
        SET d.cf_ward_id = m.new_city_id
        WHERE d.domain = 'nhatot'
          AND m.new_city_id IS NOT NULL
          AND d.cf_ward_id != m.new_city_id
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_ward)
        rows = cursor.rowcount
        conn.commit()
        total_w += rows
        print(f"  Updated batch: {rows} records (Total Wards: {total_w})")
        if rows < BATCH_SIZE:
            break
    
    # 2. Update DISTRICT (Note: Merge table might only contain Cities/Wards?)
    # Check if transaction_city_merge covers districts. Usually it covers cities (wards/districts mixed).
    # If old_district_id mappings exist, use them. But merge table structure is city-centric.
    # Assuming District ID might also be 'old_city_id' if structure allows.
    # Safe to TRY update if ID matches.
    print("\n2. Updating DISTRICT IDs to NEW IDs...")
    total_d = 0
    while True:
        sql_district = f"""
        UPDATE data_clean_v1 d
        JOIN transaction_city_merge m ON d.cf_district_id = m.old_city_id
        SET d.cf_district_id = m.new_city_id
        WHERE d.domain = 'nhatot'
          AND m.new_city_id IS NOT NULL
          AND d.cf_district_id != m.new_city_id
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_district)
        rows = cursor.rowcount
        conn.commit()
        total_d += rows
        print(f"  Updated batch: {rows} records (Total Districts: {total_d})")
        if rows < BATCH_SIZE:
            break

    # 3. Update PROVINCE
    print("\n3. Updating PROVINCE IDs to NEW IDs...")
    total_p = 0
    while True:
        sql_province = f"""
        UPDATE data_clean_v1 d
        JOIN transaction_city_merge m ON d.cf_province_id = m.old_city_id
        SET d.cf_province_id = m.new_city_id
        WHERE d.domain = 'nhatot'
          AND m.new_city_id IS NOT NULL
          AND d.cf_province_id != m.new_city_id
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_province)
        rows = cursor.rowcount
        conn.commit()
        total_p += rows
        print(f"  Updated batch: {rows} records (Total Provinces: {total_p})")
        if rows < BATCH_SIZE:
            break

    end_time = time.time()
    print(f"\n=== Completed in {end_time - start_time:.2f}s ===")
    print(f"Total Updated: Wards={total_w}, Districts={total_d}, Provinces={total_p}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
