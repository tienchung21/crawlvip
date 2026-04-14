import pymysql
conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db')
cursor = conn.cursor()
try:
    cursor.execute("ALTER TABLE location_homedy ADD COLUMN cafeland_id INT DEFAULT NULL;")
    cursor.execute("ALTER TABLE location_homedy ADD INDEX idx_location_id (location_id);")
    conn.commit()
    print("Added column 'cafeland_id' & index 'idx_location_id' successfully.")
except pymysql.err.OperationalError as e:
    if "Duplicate column name" in str(e) or "Duplicate key name" in str(e):
        print("Column or Index already exists.")
    else:
        raise e
