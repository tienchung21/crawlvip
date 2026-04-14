import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()

print("Check city_merge table for districts/wards:")
cursor.execute("SELECT COUNT(*) FROM transaction_city_merge WHERE old_city_parent_id > 0;")
count_d_w = cursor.fetchone()[0]
print(f"Number of mappings with parent_id > 0 (districts/wards): {count_d_w}")

cursor.execute("SELECT old_city_id, new_city_id, old_city_parent_id, old_city_name FROM transaction_city_merge WHERE old_city_parent_id > 0 LIMIT 5;")
for row in cursor.fetchall():
    print(row)

