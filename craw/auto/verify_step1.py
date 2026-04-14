import pymysql

def main():
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='',
            database='craw_db',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
        
        # Check Province Level
        print("Checking Province Level mapping (Step 1)...")
        sql = """
            SELECT 
                d.id, 
                d.src_province_id, 
                d.cf_province_id AS actual_in_dataclean,
                l.cafeland_id AS expected_from_location_detail
            FROM data_clean_v1 d
            JOIN location_detail l ON d.src_province_id = l.region_id
            WHERE l.level = 1
              AND d.process_status >= 1
            LIMIT 10
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        if not rows:
            print("No rows found with process_status >= 1. Has Step 1 ran?")
        else:
            print(f"{'ID':<10} | {'Src Prov':<15} | {'Actual CF':<15} | {'Expected CF':<15} | {'Match?'}")
            print("-" * 70)
            for row in rows:
                match = (row['actual_in_dataclean'] == row['expected_from_location_detail'])
                print(f"{row['id']:<10} | {row['src_province_id']:<15} | {row['actual_in_dataclean']:<15} | {row['expected_from_location_detail']:<15} | {match}")

        # Check District Level
        print("\nChecking District Level mapping...")
        sql_dist = """
             SELECT 
                d.id, 
                d.src_district_id, 
                d.cf_district_id AS actual,
                l.cafeland_id AS expected
             FROM data_clean_v1 d
             JOIN location_detail l ON d.src_district_id = l.area_id 
                AND d.src_province_id = l.region_id
             WHERE l.level = 2
               AND d.process_status >= 1
             LIMIT 5
        """
        cursor.execute(sql_dist)
        rows_dist = cursor.fetchall()
        for row in rows_dist:
             print(f"Dist ID: {row['id']} | Act: {row['actual']} | Exp: {row['expected']}")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
