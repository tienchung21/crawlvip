import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()

# Check Meeyland districts
cursor.execute("SELECT meey_id, name, city_meey_id FROM location_meeland WHERE level_type = 'district' OR (city_meey_id != meey_id AND district_meey_id = meey_id) LIMIT 5")
print("--- Meeyland Districts ---")
for row in cursor.fetchall(): print(row)

# Check transaction_city_new districts for Hanoi (old_id = 1)
cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city_new WHERE city_parent_id = 1 LIMIT 5")
print("--- Sys Districts for Hanoi (id=1) ---")
for row in cursor.fetchall(): print(row)

# Check if transaction_city_merge maps districts
cursor.execute("SELECT * FROM transaction_city_merge WHERE old_city_parent_id = 1 OR new_city_parent_id = 1 LIMIT 5")
print("--- Merge table for districts ---")
for row in cursor.fetchall(): print(row)

