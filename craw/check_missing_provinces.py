
import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

# Standard 63 Provinces (Sorted)
STANDARD_63 = [
    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu", "Bắc Ninh", 
    "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước", "Bình Thuận", "Cà Mau", 
    "Cần Thơ", "Cao Bằng", "Đà Nẵng", "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", 
    "Đồng Tháp", "Gia Lai", "Hà Giang", "Hà Nam", "Hà Nội", "Hà Tĩnh", "Hải Dương", 
    "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hưng Yên", "Khánh Hòa", "Kiên Giang", 
    "Kon Tum", "Lai Châu", "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", 
    "Nghệ An", "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình", 
    "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng", "Sơn La", 
    "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa", "Thừa Thiên Huế", "Tiền Giang", 
    "Hồ Chí Minh", "Trà Vinh", "Tuyên Quang", "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
]

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== CHECKING MISSING PROVINCES ===\n")
    
    cursor.execute("SELECT DISTINCT city_name FROM location_batdongsan")
    db_cities = set([r[0] for r in cursor.fetchall()])
    
    print(f"DB Current Count: {len(db_cities)}/63")
    
    missing = []
    for p in STANDARD_63:
        if p not in db_cities:
            # Try fuzzy check? Or just precise match?
            # User DB usually has exact names (accents might correspond).
            # Let's check if 'p' is substring of any db_city?
            found = False
            for db_c in db_cities:
                if p == db_c:
                    found = True
                    break
            if not found:
                missing.append(p)
                
    if missing:
        print(f"MISSING {len(missing)} PROVINCES:")
        for m in missing:
            print(f"  - {m}")
            
    conn.close()

if __name__ == "__main__":
    run()
