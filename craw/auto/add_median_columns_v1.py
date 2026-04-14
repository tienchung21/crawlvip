"""
Preparation script cho hệ thống Data Median.
Thêm cột median_flag vào data_clean_v1 + indexes cần thiết.
"""
import pymysql
import argparse

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'


def add_column_if_not_exists(cursor, table, column, definition):
    cursor.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s LIMIT 1",
        (table, column),
    )
    if not cursor.fetchone():
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        print(f"  Added column {column} to {table}")
    else:
        print(f"  Column {column} already exists in {table}")


def add_index_if_not_exists(cursor, table, index_name, columns):
    cursor.execute(
        "SELECT 1 FROM information_schema.statistics "
        "WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s LIMIT 1",
        (table, index_name),
    )
    if not cursor.fetchone():
        col_str = columns if isinstance(columns, str) else ", ".join(columns)
        cursor.execute(f"ALTER TABLE {table} ADD INDEX {index_name} ({col_str})")
        print(f"  Added index {index_name} on {table}({col_str})")
    else:
        print(f"  Index {index_name} already exists on {table}")


def main():
    parser = argparse.ArgumentParser(description="Add median_flag + indexes to data_clean_v1")
    parser.add_argument("--apply", action="store_true", help="Actually apply changes")
    args = parser.parse_args()

    if not args.apply:
        print("Dry-run mode. Use --apply to execute.")
        print("Changes planned:")
        print("  1. Add column: median_flag TINYINT(1) NULL DEFAULT NULL")
        print("  2. Add indexes: idx_dm_median_flag, idx_dm_std_date, idx_dm_cf_ward, idx_dm_cf_province, idx_dm_price_m2, idx_dm_project_id")
        return

    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()

    print("=== PREPARATION: data_clean_v1 for Data Median ===")

    # 1. Add median_flag column
    print("\n1. Adding columns...")
    add_column_if_not_exists(cursor, 'data_clean_v1', 'median_flag', 'TINYINT(1) NULL DEFAULT NULL')
    conn.commit()

    # 2. Add indexes
    print("\n2. Adding indexes...")
    indexes = [
        ('data_clean_v1', 'idx_dm_median_flag', 'median_flag'),
        ('data_clean_v1', 'idx_dm_std_date', 'std_date'),
        ('data_clean_v1', 'idx_dm_cf_ward', 'cf_ward_id'),
        ('data_clean_v1', 'idx_dm_cf_province', 'cf_province_id'),
        ('data_clean_v1', 'idx_dm_price_m2', 'price_m2'),
        ('data_clean_v1', 'idx_dm_project_id', 'project_id'),
        ('data_clean_v1', 'idx_dm_stats_ward', 'cf_ward_id, median_group, std_date, price_m2'),
        ('data_clean_v1', 'idx_dm_stats_project', 'project_id, median_group, std_date, price_m2'),
    ]
    for table, idx_name, cols in indexes:
        add_index_if_not_exists(cursor, table, idx_name, cols)
    conn.commit()

    # 3. Summary
    print("\n3. Summary:")
    cursor.execute("SELECT COUNT(*) FROM data_clean_v1 WHERE median_flag IS NOT NULL")
    print(f"  Rows with median_flag set: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM data_clean_v1 WHERE std_date IS NOT NULL")
    print(f"  Rows with std_date: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM data_clean_v1 WHERE price_m2 IS NOT NULL AND price_m2 > 0")
    print(f"  Rows with price_m2 > 0: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM data_clean_v1 WHERE project_id IS NOT NULL AND project_id > 0")
    print(f"  Rows with project_id: {cursor.fetchone()[0]}")

    cursor.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
