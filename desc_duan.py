import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()

print("--- duan_homedy ---")
cursor.execute("SHOW TABLES LIKE '%homedy%';")
for row in cursor.fetchall(): print(row)

cursor.execute("DESCRIBE duan_homedy;")
for row in cursor.fetchall(): print(row)

print("--- Other duan tables ---")
cursor.execute("SHOW TABLES LIKE 'duan%';")
for row in cursor.fetchall(): print(row)

