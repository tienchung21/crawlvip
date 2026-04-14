import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM location_meeland WHERE level_type = 'city' OR level_type = '' OR level_type IS NULL OR city_meey_id = meey_id")
print(f"Total Meeyland cities with normal query: {cursor.fetchone()[0]}")
cursor.execute("SELECT COUNT(DISTINCT city_meey_id) FROM location_meeland WHERE city_meey_id IS NOT NULL AND city_meey_id != ''")
print(f"Total distinct city_meey_id: {cursor.fetchone()[0]}")
