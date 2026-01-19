import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

print("=== ALL TABLES in craw_db ===")
cursor.execute("SHOW TABLES")
tables = [row[0] for row in cursor.fetchall()]
for t in tables:
    print(f"  {t}")

print("\n=== Check 'ward' in transaction_city ===")
cursor.execute("SELECT * FROM transaction_city LIMIT 1")
desc = cursor.description
cols = [x[0] for x in desc]
print(f"Columns: {cols}")

# Check if city_loai (type) distinguishes ward?
print("\n=== Check city_loai types ===")
cursor.execute("SELECT city_loai, COUNT(*) FROM transaction_city GROUP BY city_loai")
for row in cursor.fetchall():
    print(f"  Loai {row[0]}: {row[1]}")

print("\n=== Check transaction_city_merge columns ===")
cursor.execute("SELECT * FROM transaction_city_merge LIMIT 1")
desc = [x[0] for x in cursor.description]
print(f"Merge Cols: {desc}")

print("\n=== Check location_detail columns ===")
if 'location_detail' in tables:
    cursor.execute("SELECT * FROM location_detail LIMIT 1")
    desc = [x[0] for x in cursor.description]
    print(f"Detail Cols: {desc}")

cursor.close()
conn.close()
