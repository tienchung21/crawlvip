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

    script_name = "nhatot_step5_group_median.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    # Step 5: Gom nhóm Median (data_clean_v1)
    # Quy tắc theo yêu cầu:
    # - std_trans_type = 's': std_category 1020 -> group 1, 1010 -> group 2, 1040 -> group 3, 1030 bỏ qua
    # - std_trans_type = 'u': tất cả -> group 4

    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN median_group TINYINT NULL")
        conn.commit()
        print("Added column median_group to data_clean_v1")
    except Exception:
        pass

    try:
        cursor.execute("CREATE INDEX idx_dcv1_type_cat ON data_clean_v1(std_trans_type, std_category)")
        conn.commit()
        print("Created index idx_dcv1_type_cat")
    except Exception:
        pass

    updates = [
        (1, "std_trans_type = 's' AND std_category = '1020' AND median_group IS NULL"),
        (2, "std_trans_type = 's' AND std_category = '1010' AND median_group IS NULL"),
        (3, "std_trans_type = 's' AND std_category = '1040' AND median_group IS NULL"),
    ]
    for group, condition in updates:
        cursor.execute(f"UPDATE data_clean_v1 SET median_group = {group} WHERE domain='nhatot' AND {condition}")
        conn.commit()
        print(f"Updated {cursor.rowcount} rows: {condition} -> median_group={group}")

    cursor.execute(
        "UPDATE data_clean_v1 SET median_group = 4 "
        "WHERE domain='nhatot' AND std_trans_type = 'u' AND median_group IS NULL"
    )
    conn.commit()
    print(f"Updated {cursor.rowcount} rows: type='u' -> median_group=4")

    cursor.execute(
        "SELECT median_group, COUNT(*) AS total "
        "FROM data_clean_v1 "
        "WHERE domain='nhatot' AND std_trans_type IN ('s', 'u') "
        "GROUP BY median_group ORDER BY median_group"
    )
    rows = cursor.fetchall()
    print("=== SUMMARY (type s/u) ===")
    for row in rows:
        print(f"Group {row['median_group']}: {row['total']} rows")

    cursor.execute(
        f"""
        UPDATE data_clean_v1
        SET process_status = 5,
            last_script = '{script_name}'
        WHERE domain='nhatot'
          AND process_status = 4
        """
    )
    conn.commit()
    print(f"-> Updated process_status = 5 for {cursor.rowcount} rows.")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
