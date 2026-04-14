import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()

print("--- duan ---")
cursor.execute("DESCRIBE duan;")
for row in cursor.fetchall()[:5]: print(row)

print("--- duan_alonhadat_duan_merge ---")
cursor.execute("DESCRIBE duan_alonhadat_duan_merge;")
for row in cursor.fetchall(): print(row)

