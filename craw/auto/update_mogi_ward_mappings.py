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

    print("=== Starting Update Mogi IDs to NEW IDs (cafeland_new_id) ===")
    print("Target: data_clean_v1 (domain='mogi')")
    start_time = time.time()

    # 1. Update WARD (Quan trọng nhất)
    # Mapping src_ward_id (Mogi ID) -> location_mogi (cafeland_new_id)
    print("\n1. Updating WARD IDs to NEW IDs...")
    total_w = 0
    while True:
        # Update records where current ID matches location_mogi.cafeland_id (Old ID) 
        # OR just update based on src mapping to be safe.
        # We target rows where new mapping exists AND differs from current value.
        sql_ward = f"""
        UPDATE data_clean_v1 d
        JOIN location_mogi l ON d.src_ward_id = l.mogi_id
        SET d.cf_ward_id = l.cafeland_new_id
        WHERE d.domain = 'mogi'
          AND l.type = 'WARD'
          AND l.cafeland_new_id IS NOT NULL
          AND d.cf_ward_id != l.cafeland_new_id
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_ward)
        rows = cursor.rowcount
        conn.commit()
        total_w += rows
        print(f"  Updated batch: {rows} records (Total Wards: {total_w})")
        if rows < BATCH_SIZE:
            break
    
    # 2. Update DISTRICT
    print("\n2. Updating DISTRICT IDs to NEW IDs...")
    total_d = 0
    while True:
        sql_district = f"""
        UPDATE data_clean_v1 d
        JOIN location_mogi l ON d.src_district_id = l.mogi_id
        SET d.cf_district_id = l.cafeland_new_id
        WHERE d.domain = 'mogi'
          AND l.type = 'DISTRICT'
          AND l.cafeland_new_id IS NOT NULL
          AND d.cf_district_id != l.cafeland_new_id
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
        JOIN location_mogi l ON d.src_province_id = l.mogi_id
        SET d.cf_province_id = l.cafeland_new_id
        WHERE d.domain = 'mogi'
          AND l.type = 'CITY'
          AND l.cafeland_new_id IS NOT NULL
          AND d.cf_province_id != l.cafeland_new_id
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
