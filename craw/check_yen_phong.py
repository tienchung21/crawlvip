import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

with open("check_yen_phong_res.txt", "w", encoding="utf-8") as f:
    f.write("=== CHECKING YEN PHONG ===\n")

    # 1. Nhatot record
    cursor.execute("SELECT area_id, name, region_id, cafeland_id FROM location_detail WHERE name LIKE '%Yên Phong%' AND level = 2")
    nt_rows = cursor.fetchall()
    f.write(f"Nhatot: {nt_rows}\n")

    # 2. Cafeland record
    cursor.execute("SELECT city_id, city_title, city_parent_id FROM transaction_city WHERE city_title LIKE '%Yên Phong%'")
    cf_rows = cursor.fetchall()
    f.write(f"Cafeland: {cf_rows}\n")

    # Check Parent
    if nt_rows:
        reg_id = nt_rows[0][2]
        cursor.execute(f"SELECT region_id, name, cafeland_id FROM location_detail WHERE region_id = {reg_id} AND level = 1")
        prov = cursor.fetchone()
        f.write(f"Nhatot Province: {prov}\n")
        
        # Check Cafeland Parent Name
        if prov and prov[2]: # Has cafeland_id
            cursor.execute(f"SELECT city_id, city_title FROM transaction_city WHERE city_id = {prov[2]}")
            cf_prov = cursor.fetchone()
            f.write(f"Cafeland Mapped Province: {cf_prov}\n")

cursor.close()
conn.close()
