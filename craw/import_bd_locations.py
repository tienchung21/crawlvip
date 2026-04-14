
import json
import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'
JSON_FILE = "/home/chungnt/Downloads/batdongsan_4cities.json"

def run():
    print(f"Importing {JSON_FILE}...")
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"File Error: {e}")
        return

    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    count = 0
    for city in data:
        c_code = city['city_code']
        c_name = city['city_name']
        print(f"City: {c_name}")
        
        for dist in city['districts']:
            d_info = dist['district_info']
            d_id = d_info.get('id') or d_info.get('districtId')
            d_name = d_info.get('name') or d_info.get('districtName')
            
            for ward in dist['wards']:
                w_id = ward.get('id') or ward.get('wardId')
                w_name = ward.get('name') or ward.get('wardName')
                
                # Insert
                sql = """
                INSERT INTO location_batdongsan (ward_id, ward_name, district_id, district_name, city_code, city_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE ward_name=VALUES(ward_name)
                """
                cursor.execute(sql, (w_id, w_name, d_id, d_name, c_code, c_name))
                count += 1
                
    conn.commit()
    conn.close()
    print(f"Imported {count} Wards for Binh Duong.")

if __name__ == "__main__":
    run()
