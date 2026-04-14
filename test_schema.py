import mysql.connector
import sys

db = mysql.connector.connect(host="localhost", user="root", password="", database="craw_db")
cursor = db.cursor()
cursor.execute("SHOW COLUMNS FROM clean_v1")
for row in cursor.fetchall():
    print(row)
