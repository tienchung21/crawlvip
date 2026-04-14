import pymysql
import time

BATCH_SIZE = 5000

GROUP_1 = [
    "Nhà hẻm ngõ",
    "Nhà mặt tiền phố",
    "Nhà biệt thự, liền kề",
    "Đường nội bộ",
]

GROUP_2 = [
    "Căn hộ chung cư",
    "Căn hộ dịch vụ",
    "Căn hộ tập thể, cư xá",
    "Căn hộ Penthouse",
    "Căn hộ Officetel",
]

GROUP_3 = [
    "Đất thổ cư",
    "Đất nền dự án",
    "Đất nông nghiệp",
    "Đất kho xưởng",
]

def main():
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="craw_db",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    cursor = conn.cursor()

    script_name = "mogi_step5_group_median.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN median_group TINYINT NULL")
        conn.commit()
        print("Added column median_group to data_clean_v1")
    except Exception:
        pass

    # Fast bulk update using CASE; still uses LIMIT batching.
    placeholders_g1 = ",".join(["%s"] * len(GROUP_1))
    placeholders_g2 = ",".join(["%s"] * len(GROUP_2))
    placeholders_g3 = ",".join(["%s"] * len(GROUP_3))
    params = [*GROUP_1, *GROUP_2, *GROUP_3]

    print("Updating median_group (bulk CASE)...")
    total_updated = 0
    while True:
        sql = f"""
            UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
            SET median_group = CASE
                WHEN std_trans_type = 'u' THEN 4
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g1}) THEN 1
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g2}) THEN 2
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g3}) THEN 3
                ELSE median_group
            END
            WHERE domain = 'mogi'
              AND process_status = 4
              AND median_group IS NULL
              AND (
                std_trans_type = 'u'
                OR (std_trans_type = 's' AND (
                    std_category IN ({placeholders_g1})
                    OR std_category IN ({placeholders_g2})
                    OR std_category IN ({placeholders_g3})
                ))
              )
            ORDER BY id
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql, params + params)  # CASE + WHERE
        rows = cursor.rowcount
        conn.commit()
        if rows == 0:
            break
        total_updated += rows
        print(f"  Batch: +{rows} rows (Total: {total_updated})")
        if rows < BATCH_SIZE:
            break

    print("Finalizing step status...")
    cursor.execute(
        f"""
        UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
        SET process_status = 5, last_script = '{script_name}'
        WHERE domain = 'mogi'
          AND process_status = 4
          AND median_group IS NOT NULL
        """
    )
    conn.commit()
    print(f"-> Updated process_status = 5 for {cursor.rowcount} rows.")

    cursor.execute(
        "SELECT median_group, COUNT(*) AS total "
        "FROM data_clean_v1 WHERE domain = 'mogi' AND process_status = 5 "
        "GROUP BY median_group ORDER BY median_group"
    )
    rows = cursor.fetchall()
    print("=== SUMMARY (process_status=5) ===")
    for row in rows:
        print(f"Group {row['median_group']}: {row['total']} rows")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
