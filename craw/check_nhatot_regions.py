import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

with open('nhatot_regions.txt', 'w', encoding='utf-8') as f:
    f.write("=== NHATOT REGIONS (Source: ad_listing_detail) ===\n")
    cursor.execute("SELECT DISTINCT region_v2, region_name FROM ad_listing_detail WHERE region_name LIKE '%Ho Chi Minh%' OR region_name LIKE '%Binh Duong%' OR region_name LIKE '%Ba Ria%'")
    for r in cursor.fetchall():
        f.write(str(r) + "\n")

cursor.close()
conn.close()
