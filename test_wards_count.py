import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()

print("--- COUNT BY level_type ---")
cursor.execute("SELECT level_type, COUNT(*) FROM location_meeland GROUP BY level_type")
for row in cursor.fetchall():
    print(row)

print("\n--- COUNT Wards explicitly ---")
cursor.execute("SELECT COUNT(*) FROM location_meeland WHERE level_type = 'ward'")
print("Wards with level_type = 'ward':", cursor.fetchone()[0])
