import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()
cursor.execute("SHOW INDEX FROM location_meeland;")
for row in cursor.fetchall(): print(row)
