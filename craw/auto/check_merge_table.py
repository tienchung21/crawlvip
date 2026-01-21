import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

print("=== CHECKING TRANSACTION_CITY_MERGE SCHEMA ===")
cursor.execute("DESCRIBE transaction_city_merge")
rows = cursor.fetchall()
for r in rows:
    print(r)

print("\n=== SAMPLE DATA ===")
cursor.execute("SELECT * FROM transaction_city_merge LIMIT 5")
cols = [d[0] for d in cursor.description]
print(cols)
for r in cursor.fetchall():
    print(r)

cursor.close()
conn.close()
