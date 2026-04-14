import pymysql

try:
    conn = pymysql.connect(host="127.0.0.1", user="root", password="", database="craw_db")
    cur = conn.cursor()
    cur.execute("SHOW COLUMNS FROM data_clean_v1")
    for row in cur.fetchall():
        print(row)
except Exception as e:
    print("Error:", e)
