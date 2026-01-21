import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

with open('schema_check_res.txt', 'w', encoding='utf-8') as f:
    f.write("=== FINDING CITY IDs ===\n")
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_title LIKE '%Ho Chi Minh%' OR city_title LIKE '%Binh Duong%' OR city_title LIKE '%Ba Ria%'")
    for r in cursor.fetchall():
        f.write(str(r) + "\n")

    f.write("\n=== AD_LISTING_DETAIL COLUMNS ===\n")
    cursor.execute("DESCRIBE ad_listing_detail")
    cols = [r[0] for r in cursor.fetchall()]
    f.write(str(cols))

    f.write("\n\n=== DATA_CLEAN COLUMNS ===\n")
    cursor.execute("DESCRIBE data_clean")
    cols = [r[0] for r in cursor.fetchall()]
    f.write(str(cols))

cursor.close()
conn.close()
