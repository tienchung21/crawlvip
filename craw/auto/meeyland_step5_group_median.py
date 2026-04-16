import pymysql
import argparse
import time

GROUP_1 = [
    'nha_tap_the', 'khu_nghi_duong_resort', 'nha_rieng', 'nha_tro', 
    'biet_thu_lien_ke', 'khach_san_nha_nghi', 'van_phong', 
    'shophouse_nha_pho_thuong_mai', 'nha_mat_pho'
]

GROUP_2 = [
    'can_ho_van_phong_officetel', 'chung_cu_mini', 
    'can_ho_khach_san_condotel', 'can_ho_chung_cu', 'can_ho_dich_vu_homestay'
]

GROUP_3 = [
    'trang_trai', 'dat', 'kho_nha_xuong', 'dat_nen_du_an'
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print("=== meeyland_step5_group_median.py ===")
    start_time = time.time()
    
    script_name = 'meeyland_step5_group_median.py'

    placeholders_g1 = ",".join(["%s"] * len(GROUP_1))
    placeholders_g2 = ",".join(["%s"] * len(GROUP_2))
    placeholders_g3 = ",".join(["%s"] * len(GROUP_3))
    
    # RENT matches group 4 for meeyland
    # SALE matches 1, 2, 3
    params = [*GROUP_1, *GROUP_2, *GROUP_3]

    # Important: always bump process_status in this step.
    # Some categories are intentionally not mapped to a median_group;
    # if we only move rows with median_group IS NOT NULL, linear runner can freeze.
    sql = f"""
        UPDATE data_clean_v1 FORCE INDEX (idx_domain_status)
        SET
            median_group = CASE
                WHEN std_trans_type = 'u' THEN 4
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g1}) THEN 1
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g2}) THEN 2
                WHEN std_trans_type = 's' AND std_category IN ({placeholders_g3}) THEN 3
                ELSE NULL
            END,
            process_status = 5,
            last_script = %s
        WHERE domain = 'meeyland.com'
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
