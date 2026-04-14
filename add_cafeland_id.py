import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()
try:
    cursor.execute("ALTER TABLE location_meeland ADD COLUMN cafeland_id INT DEFAULT NULL;")
    conn.commit()
    print("Added column 'cafeland_id' successfully.")
except pymysql.err.OperationalError as e:
    if "Duplicate column name" in str(e):
        print("Column 'cafeland_id' already exists.")
    else:
        raise e
