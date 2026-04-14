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
    
    print("=== Adding Optimized Indexes for Region & Project Scopes ===")
    
    # 1. Index cho Region Scope (Bỏ qua cf_ward_id)
    # Giúp query GROUP BY cf_province_id, median_group chạy nhanh hơn
    print("1. Adding idx_dm_region_group (cf_province_id, median_group, std_date, price_m2)...")
    try:
        sql = "CREATE INDEX idx_dm_region_group ON data_clean_v1 (cf_province_id, median_group, std_date, price_m2)"
        cursor.execute(sql)
        print("   -> Done.")
    except Exception as e:
        print(f"   -> Error (might exist): {e}")

    # 2. Index cho Project Scope
    # Giúp query GROUP BY project_id, median_group chạy nhanh hơn
    print("2. Adding idx_dm_project_group (project_id, median_group, std_date, price_m2)...")
    try:
        sql = "CREATE INDEX idx_dm_project_group ON data_clean_v1 (project_id, median_group, std_date, price_m2)"
        cursor.execute(sql)
        print("   -> Done.")
    except Exception as e:
        print(f"   -> Error (might exist): {e}")
        
    conn.close()
    print("=== Finished Adding Indexes ===")

if __name__ == "__main__":
    main()
