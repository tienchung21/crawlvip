import pymysql
import unicodedata

def remove_accents(s):
    if not s:
        return ""
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = remove_accents(name)
    # Bo cac prefix cap xa
    for prefix in ['phuong ', 'xa ', 'thitran ', 'thi tran ']:
        if name.startswith(prefix):
            name = name[len(prefix):]
    # Xu ly so (vd: Phuong 1 -> 1, Phuong 01 -> 1)
    if name.isdigit():
        name = str(int(name))
    return name.strip()

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

# 1. Lay ID HCM tu mapping
cursor.execute("SELECT cafeland_id, cenhomes_id FROM cenhomes_cafeland_province_mapping WHERE cafeland_name LIKE '%Hồ Chí Minh%'")
hcm_ids = cursor.fetchone()
if not hcm_ids:
    print("HCM ID not found!")
    exit()
hcm_cf_id, hcm_ch_id = hcm_ids

# 2. Tao bang mapping cho Phuong/Xa (Ward)
print("=== Creating WARD mapping table ===")
cursor.execute("DROP TABLE IF EXISTS cenhomes_cafeland_ward_mapping")
cursor.execute("""
    CREATE TABLE cenhomes_cafeland_ward_mapping (
        cafeland_id INT PRIMARY KEY,
        cafeland_name VARCHAR(255),
        cafeland_district_id INT,
        cenhomes_id BIGINT,
        cenhomes_name VARCHAR(255),
        cenhomes_district_id BIGINT,
        match_type VARCHAR(20)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
""")

# 3. Lay danh sach Quan/Huyen da match o HCM
cursor.execute("""
    SELECT cafeland_id, cenhomes_id 
    FROM cenhomes_cafeland_district_mapping 
    WHERE cafeland_parent_id = %s AND match_type != 'not_found'
""", (hcm_cf_id,))
districts = cursor.fetchall()

print(f"Total matched districts in HCM: {len(districts)}")

total_wards_matched = 0
total_wards_failed = 0

for cf_dist_id, ch_dist_id in districts:
    # Lay Phuong/Xa Cafeland
    cursor.execute("""
        SELECT ward_id, ward_title 
        FROM transaction_city_merge 
        WHERE new_city_id = %s
    """, (cf_dist_id,)) # Can check lai bang nao chua WARD cua Cafeland
    
    # Kiem tra lai: Cafeland WARD nam o dau?
    # Bang transaction_city chi co cap Tinh & Huyen (city_parent_id)
    # Bang transaction_city_merge co le moi chua Ward?
    pass

# STOP: Can xac dinh lai bang nao chua Ward cua Cafeland
print("\nNeed to verify where Cafeland WARDS are stored.")
cursor.execute("SHOW TABLES LIKE '%ward%'")
print("Tables with 'ward':", cursor.fetchall())

cursor.execute("SELECT * FROM transaction_city LIMIT 1")
desc = cursor.description
print("cols in transaction_city:", [x[0] for x in desc])

cursor.close()
conn.close()
