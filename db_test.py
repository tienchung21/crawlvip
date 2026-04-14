import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
with conn.cursor() as cursor:
    for table in ["location_meeland", "transaction_city_new", "transaction_city_merge"]:
        print(f"--- {table} ---")
        cursor.execute(f"DESCRIBE {table};")
        for row in cursor.fetchall():
            print(row)
