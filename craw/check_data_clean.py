import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

print("=== CHECKING DATA_CLEAN SCHEMA ===")
cursor.execute("DESCRIBE data_clean")
rows = cursor.fetchall()
for r in rows:
    print(r)

# Check sample data
print("\n=== SAMPLE DATA ===")
cursor.execute("SELECT * FROM data_clean LIMIT 3")
cols = [d[0] for d in cursor.description]
print(cols)
sample_rows = cursor.fetchall()
for r in sample_rows:
    print(r)

cursor.close()
conn.close()
