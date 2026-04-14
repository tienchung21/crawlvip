import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()
try:
    cursor.execute("ALTER TABLE location_meeland ADD INDEX idx_meey_id (meey_id);")
    conn.commit()
    print("Added index 'idx_meey_id' successfully.")
except Exception as e:
    print("Index might already exist:", e)

