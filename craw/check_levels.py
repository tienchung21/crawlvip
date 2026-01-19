import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

print("=== CHECK CAFELAND ID FOR LEVEL 1 & 2 ===")

# Level 1 (Province)
cursor.execute("SELECT COUNT(*), COUNT(cafeland_id) FROM location_detail WHERE level = 1")
l1_total, l1_filled = cursor.fetchone()
print(f"Level 1 (Province): Total {l1_total} | Has ID: {l1_filled}")

# Level 2 (District)
cursor.execute("SELECT COUNT(*), COUNT(cafeland_id) FROM location_detail WHERE level = 2")
l2_total, l2_filled = cursor.fetchone()
print(f"Level 2 (District): Total {l2_total} | Has ID: {l2_filled}")

cursor.close()
conn.close()
