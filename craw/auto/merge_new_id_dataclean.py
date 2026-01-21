import pymysql

# Config
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== MERGING NEW IDS FROM WARD -> CAFELAND (VIA LOCATION_DETAIL) ===")

    # 1. Add Columns if not exist
    cols = [
        ('cafeland_new_id', 'INT DEFAULT NULL'),
        ('cafeland_new_name', 'VARCHAR(255) DEFAULT NULL'),
        ('cafeland_new_parent_id', 'INT DEFAULT NULL'),
        ('cafeland_id', 'BIGINT DEFAULT NULL')
    ]
    
    for col_name, col_def in cols:
        try:
            cursor.execute(f"ALTER TABLE data_clean ADD COLUMN {col_name} {col_def}")
            conn.commit()
            print(f"Created column: {col_name}")
        except Exception as e:
            if "Duplicate column" in str(e):
                pass # Ignore if exists
            else:
                print(f"Error adding {col_name}: {e}")

    # 2. Add Indexes for Performance (CRITICAL FIX)
    print("Checking indexes...")
    indexes = [
        ('location_detail', 'idx_ward_level', '(ward_id, level)'),
        ('data_clean', 'idx_ward', '(ward)'),
        ('data_clean', 'idx_cafeland_id', '(cafeland_id)')
    ]
    for table, idx_name, idx_cols in indexes:
        try:
            cursor.execute(f"CREATE INDEX {idx_name} ON {table} {idx_cols}")
            conn.commit()
            print(f"Created index {idx_name} on {table}")
        except Exception as e:
            if "Duplicate key" in str(e) or "already exists" in str(e):
                pass
            else:
                print(f"Note: Index {idx_name} on {table} might already exist or error: {e}")

    # 3. Update cafeland_id from ward via location_detail (MASS UPDATE)
    print("\nStep 1: Updating cafeland_id from ward (Mass SQL)...")
    sql_cafeland = """
    UPDATE data_clean d
    JOIN location_detail l ON d.ward = l.ward_id AND l.level = 3
    SET d.cafeland_id = l.cafeland_id
    WHERE d.cafeland_id IS NULL AND l.cafeland_id IS NOT NULL
    """
    cursor.execute(sql_cafeland)
    conn.commit()
    print(f"Updated {cursor.rowcount} rows with cafeland_id")

    # 4. Update new IDs from merge table (MASS UPDATE)
    print("\nStep 2: Updating cafeland_new_id from merge table (Mass SQL)...")
    sql_merge = """
    UPDATE data_clean d
    JOIN transaction_city_merge m ON d.cafeland_id = m.old_city_id
    JOIN transaction_city tc ON m.new_city_id = tc.city_id
    SET 
        d.cafeland_new_id = m.new_city_id,
        d.cafeland_new_name = m.new_city_name,
        d.cafeland_new_parent_id = tc.city_parent_id
    WHERE d.cafeland_id IS NOT NULL
      AND (d.cafeland_new_id IS NULL OR d.cafeland_new_id != m.new_city_id)
    """
    cursor.execute(sql_merge)
    conn.commit()
    print(f"Updated {cursor.rowcount} rows with new ID, name, parent_id")

    # Summary
    cursor.execute("SELECT COUNT(*) FROM data_clean WHERE cafeland_id IS NOT NULL")
    print(f"\nSummary: cafeland_id filled: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM data_clean WHERE cafeland_new_id IS NOT NULL")
    print(f"Summary: cafeland_new_id filled: {cursor.fetchone()[0]}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run()
