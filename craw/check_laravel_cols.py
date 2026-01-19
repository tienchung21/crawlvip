import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

print("=== CHECKING CF_WARD_ID_NEW vs CAFELAND_ID ===")

# Check if filled
cursor.execute("SELECT COUNT(*) FROM data_clean WHERE cf_ward_id_new IS NOT NULL")
filled_old = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM data_clean WHERE cafeland_id IS NOT NULL")
filled_new = cursor.fetchone()[0]

print(f"Rows with cf_ward_id_new: {filled_old}")
print(f"Rows with cafeland_id: {filled_new}")

# Check if they match
if filled_old > 0 and filled_new > 0:
    cursor.execute("SELECT COUNT(*) FROM data_clean WHERE cf_ward_id_new != cafeland_id")
    diff = cursor.fetchone()[0]
    print(f"Rows where they differ: {diff}")

cursor.close()
conn.close()
