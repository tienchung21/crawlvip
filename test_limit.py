import pymysql
import pymysql.cursors
conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn.cursor() as cur:
    print("Querying counts...")
    cur.execute("SELECT domain, COUNT(*) as c FROM data_clean_v1 GROUP BY domain")
    print(cur.fetchall())
