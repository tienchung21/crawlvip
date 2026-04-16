import pymysql
import argparse
import time

# Homedy Category IDs
GROUP_1 = [
    '62', '63', '66', '56', '172', '170', '171', '190', '81', '174', '85'
]

GROUP_2 = [
    '57', '73', '70', '71', '164', '165', '166', '167', '68', '168', '169', '76'
]

GROUP_3 = [
    '58', '78', '77', '79', '83', '80', '87'
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print("=== homedy_step5_group_median.py ===")
    start_time = time.time()
    
    script_name = 'homedy_step5_group_median.py'

    placeholders_g1 = ",".join(["%s"] * len(GROUP_1))
    placeholders_g2 = ",".join(["%s"] * len(GROUP_2))
    placeholders_g3 = ",".join(["%s"] * len(GROUP_3))
    
    # Add extra params for the second set of category checks
    params = [*GROUP_1, *GROUP_2, *GROUP_3, *GROUP_1, *GROUP_2, *GROUP_3]
    # Duplicate params again for process_status CASE statement
    params = params * 2

    # Important: always bump process_status in this step.
    # Some categories are intentionally not mapped to a median_group;
    # if we only move rows with median_group IS NOT NULL, linear runner can freeze.
    sql = f"""
        UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
        SET
            median_group = CASE
                WHEN std_trans_type IN ('u', 'r') THEN 4
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g1}) THEN 1
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g2}) THEN 2
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g3}) THEN 3
                WHEN std_category IN ({placeholders_g1}) THEN 1
                WHEN std_category IN ({placeholders_g2}) THEN 2
                WHEN std_category IN ({placeholders_g3}) THEN 3
                ELSE NULL
            END,
            process_status = CASE
                WHEN (
                    CASE
                        WHEN std_trans_type IN ('u', 'r') THEN 4
                        WHEN std_trans_type = 's' AND std_category IN ({placeholders_g1}) THEN 1
                        WHEN std_trans_type = 's' AND std_category IN ({placeholders_g2}) THEN 2
                        WHEN std_trans_type = 's' AND std_category IN ({placeholders_g3}) THEN 3
                        WHEN std_category IN ({placeholders_g1}) THEN 1
                        WHEN std_category IN ({placeholders_g2}) THEN 2
                        WHEN std_category IN ({placeholders_g3}) THEN 3
                        ELSE NULL
                    END
                ) IS NOT NULL THEN 5 ELSE -5
            END,
            last_script = %s
        WHERE domain = 'homedy.com'
          AND process_status = 4
        ORDER BY id
        LIMIT %s
    """

    cursor.execute(sql, params + [script_name, args.limit])
    conn.commit()
    print(f"-> Updated process_status = 5 for {cursor.rowcount} rows.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
