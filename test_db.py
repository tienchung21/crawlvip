import Config
import pymysql

conn = pymysql.connect(
    host=Config.MYSQL_HOST,
    user=Config.MYSQL_USER,
    password=Config.MYSQL_PASSWORD,
    database=Config.MYSQL_DB
)
cursor = conn.cursor()
for table in ["location_meeland", "transaction_city_new", "transaction_city_merge"]:
    print(f"--- {table} ---")
    cursor.execute(f"DESCRIBE {table};")
    for row in cursor.fetchall(): print(row)
