import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()
cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city_new WHERE city_parent_id IN (50, 49, 46, 41, 40, 60, 32, 31, 30, 28, 27, 26)")
for row in cursor.fetchall():
    print(row)
