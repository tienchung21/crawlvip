import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()
cursor.execute("DESCRIBE location_homedy;")
for row in cursor.fetchall(): print(row)
