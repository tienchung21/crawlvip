import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

print("=== UPDATING DATA_CLEAN WITH CAFELAND_ID ===")

# 1. Add Column if not exists
try:
    cursor.execute("ALTER TABLE data_clean ADD COLUMN cafeland_id BIGINT DEFAULT NULL")
    conn.commit()
    print("Added column cafeland_id.")
except pymysql.err.OperationalError as e:
    if "Duplicate column name" in str(e):
        print("Column cafeland_id already exists.")
    else:
        raise e

# 2. Update Data
# Join conditions:
# data_clean.ward = location_detail.ward_id
# data_clean.area_v2 = location_detail.area_id
# data_clean.region_v2 = location_detail.region_id
# location_detail.level = 3 (Ward Level mapping)

sql = """
UPDATE data_clean d
JOIN location_detail l ON 
    d.ward = l.ward_id AND 
    d.area_v2 = l.area_id AND 
    d.region_v2 = l.region_id
SET d.cafeland_id = l.cafeland_id
WHERE l.level = 3 AND l.cafeland_id IS NOT NULL
"""

print("Executing UPDATE query (this may take a while)...")
cursor.execute(sql)
rows_affected = cursor.rowcount
conn.commit()

print(f"Updated {rows_affected} rows in data_clean.")

cursor.close()
conn.close()
