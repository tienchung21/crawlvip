import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    print("=== UPDATING MEDIAN_GROUP IN DATA_CLEAN ===")
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()

    try:
        # 0. Create Index for Performance
        try:
            cursor.execute("CREATE INDEX idx_type_cat ON data_clean(type, category)")
            conn.commit()
            print("Created index idx_type_cat")
        except:
            pass

        # Reset all first
        cursor.execute("UPDATE data_clean SET median_group = NULL")
        conn.commit()
        print("Reset all median_group to NULL")

        # SELL side (type = 's') (MASS UPDATES)
        updates = [
            (1, "type = 's' AND category = 1020"),
            (2, "type = 's' AND category = 1010"),
            (3, "type = 's' AND category = 1040"),
        ]
        
        for group, condition in updates:
            cursor.execute(f"UPDATE data_clean SET median_group = {group} WHERE {condition}")
            conn.commit()
            print(f"Updated {cursor.rowcount} rows: {condition} -> median_group={group}")

        # All other SELL (except 1030) -> group 4
        # Note: 1030 is skipped (left NULL)
        cursor.execute("UPDATE data_clean SET median_group = 4 WHERE type = 's' AND category NOT IN (1010, 1020, 1030, 1040) AND median_group IS NULL")
        conn.commit()
        print(f"Updated {cursor.rowcount} rows: type='s', other categories -> median_group=4")

        # RENT side (type starts with '4' or type = 'r') -> group 4
        cursor.execute("UPDATE data_clean SET median_group = 4 WHERE type LIKE '4%' OR type = 'r'")
        conn.commit()
        print(f"Updated {cursor.rowcount} rows: RENT (type like '4%' or 'r') -> median_group=4")

        # FIX: Type 'u' -> group 4
        cursor.execute("UPDATE data_clean SET median_group = 4 WHERE type = 'u'")
        conn.commit()
        print(f"Updated {cursor.rowcount} rows: type='u' -> median_group=4")

        # Summary
        cursor.execute("SELECT median_group, COUNT(*) FROM data_clean GROUP BY median_group ORDER BY median_group")
        print("\n=== SUMMARY BY MEDIAN_GROUP ===")
        for r in cursor.fetchall():
            print(f"Group {r[0]}: {r[1]} rows")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run()
