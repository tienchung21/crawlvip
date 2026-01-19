import pymysql
import unicodedata
import difflib

# Cấu hình Database
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'craw_db',
    'charset': 'utf8mb4'
}

def get_conn():
    return pymysql.connect(**DB_CONFIG)

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
    name = name.replace("la ", "ia ").replace("xi ", "si ").replace("qui ", "quy ")
    name = name.replace("-", " ")
    for prefix in ['tinh ', 'thanh pho ', 'tp. ', 'tp ', 'thi xa ', 'tx. ', 'tx ', 'quan ', 'huyen ', 'thi tran ', 'phuong ', 'xa ', 'thitran ']:
        if name.startswith(prefix):
            name = name[len(prefix):]
    if name.isdigit():
        name = str(int(name)) # '07' -> '7'
    return name.strip()

# --- 1. PROVINCES ---
def match_provinces():
    print("\n=== 1. MATCHING PROVINCES ===")
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS cenhomes_cafeland_province_mapping")
    cursor.execute("""
        CREATE TABLE cenhomes_cafeland_province_mapping (
            cafeland_id INT PRIMARY KEY,
            cafeland_name VARCHAR(255),
            cenhomes_id BIGINT,
            cenhomes_name VARCHAR(255),
            match_type VARCHAR(20)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """)
    
    cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = 0")
    cafeland_provinces = cursor.fetchall()
    cursor.execute("SELECT id, name FROM cenhomes_locations WHERE level = 'province'")
    cenhomes_provinces = cursor.fetchall()
    
    matched = 0
    for cf_id, cf_name in cafeland_provinces:
        cf_norm = normalize_name(cf_name)
        best_match = None
        for ch_id, ch_name in cenhomes_provinces:
            if cf_norm == normalize_name(ch_name):
                best_match = (ch_id, ch_name, 'exact')
                break
        
        if best_match:
            cursor.execute("INSERT INTO cenhomes_cafeland_province_mapping VALUES (%s, %s, %s, %s, %s)", (cf_id, cf_name, best_match[0], best_match[1], best_match[2]))
            matched += 1
        else:
            cursor.execute("INSERT INTO cenhomes_cafeland_province_mapping VALUES (%s, %s, NULL, NULL, 'not_found')", (cf_id, cf_name))
    
    conn.commit()
    print(f"Matched Provinces: {matched}/63")
    cursor.close()
    conn.close()

# --- 2. DISTRICTS ---
def match_districts():
    print("\n=== 2. MATCHING DISTRICTS (with Advanced Fix) ===")
    conn = get_conn()
    cursor = conn.cursor()
    
    # Create Table
    cursor.execute("DROP TABLE IF EXISTS cenhomes_cafeland_district_mapping")
    cursor.execute("""
        CREATE TABLE cenhomes_cafeland_district_mapping (
            cafeland_id INT PRIMARY KEY,
            cafeland_name VARCHAR(255),
            cafeland_parent_id INT,
            cenhomes_id BIGINT,
            cenhomes_name VARCHAR(255),
            cenhomes_province_id BIGINT,
            match_type VARCHAR(20)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """)
    
    # Load Prov Map
    cursor.execute("SELECT cafeland_id, cenhomes_id FROM cenhomes_cafeland_province_mapping WHERE cenhomes_id IS NOT NULL")
    prov_map = cursor.fetchall()
    
    # Load All Cenhomes Districts (for cache)
    cursor.execute("SELECT id, name, province_id FROM cenhomes_locations WHERE level = 'district'")
    all_ch_districts = cursor.fetchall()
    
    total_matched = 0
    for cf_prov_id, ch_prov_id in prov_map:
        cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = %s", (cf_prov_id,))
        cf_districts = cursor.fetchall()
        
        # Filter CH districts for this prov
        ch_districts_this_prov = [d for d in all_ch_districts if d[2] == ch_prov_id]
        
        for cf_id, cf_name in cf_districts:
            cf_norm = normalize_name(cf_name)
            best_match = None
            
            # Step 1: Base Match (Same Province)
            for ch_id, ch_name, _ in ch_districts_this_prov:
                ch_norm = normalize_name(ch_name)
                if cf_norm == ch_norm:
                    best_match = (ch_id, ch_name, ch_prov_id, 'exact')
                    break
            
            # Step 2: Advanced Fix (Cross Province) if not found
            if not best_match:
                best_score = 0.0
                for ch_id, ch_name, ch_p_id in all_ch_districts:
                    ch_norm = normalize_name(ch_name)
                    # Priority for same province
                    is_same_prov = (ch_p_id == ch_prov_id)
                    score = 0.0
                    
                    if cf_norm == ch_norm:
                        score = 1.0 if is_same_prov else 0.95
                    elif (cf_norm in ch_norm or ch_norm in cf_norm) and len(cf_norm) > 3:
                        score = 0.9 if is_same_prov else 0.7
                    
                    # Special Replacements (La -> Ia, etc.)
                    cf_fix = cf_norm.replace('la ', 'ia ').replace('xi ', 'si ')
                    if cf_fix == ch_norm:
                         score = max(score, 0.95 if is_same_prov else 0.9)
                        
                    if score > best_score:
                        best_score = score
                        best_match = (ch_id, ch_name, ch_p_id, 'manual_fix' if not is_same_prov else 'fuzzy')
                
                if best_score < 0.8: best_match = None

            if best_match:
                cursor.execute("INSERT INTO cenhomes_cafeland_district_mapping VALUES (%s, %s, %s, %s, %s, %s, %s)",
                               (cf_id, cf_name, cf_prov_id, best_match[0], best_match[1], best_match[2], best_match[3]))
                total_matched += 1
            else:
                cursor.execute("INSERT INTO cenhomes_cafeland_district_mapping VALUES (%s, %s, %s, NULL, NULL, %s, 'not_found')",
                               (cf_id, cf_name, cf_prov_id, ch_prov_id))

    conn.commit()
    print(f"Districts Matched: {total_matched}")
    cursor.close()
    conn.close()

# --- 3. WARDS ---
def match_wards():
    print("\n=== 3. MATCHING WARDS (with Advanced Fix) ===")
    conn = get_conn()
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS cenhomes_cafeland_ward_mapping")
    cursor.execute("""
        CREATE TABLE cenhomes_cafeland_ward_mapping (
            cafeland_id INT PRIMARY KEY,
            cafeland_name VARCHAR(255),
            cafeland_district_id INT,
            cenhomes_id BIGINT,
            cenhomes_name VARCHAR(255),
            cenhomes_district_id BIGINT,
            cenhomes_province_id BIGINT,
            match_type VARCHAR(20)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """)
    
    # Load Matched Districts
    cursor.execute("SELECT cafeland_id, cenhomes_id, cenhomes_province_id FROM cenhomes_cafeland_district_mapping WHERE cenhomes_id IS NOT NULL")
    matched_districts = cursor.fetchall()
    
    # Load All Wards (Cache by Province)
    cursor.execute("SELECT id, name, district_id, province_id FROM cenhomes_locations WHERE level = 'ward'")
    all_wards = cursor.fetchall()
    wards_by_prov = {}
    for w in all_wards:
        if w[3] not in wards_by_prov: wards_by_prov[w[3]] = []
        wards_by_prov[w[3]].append(w)
        
    total_matched = 0
    batch_data = []
    
    for cf_dist_id, ch_dist_id, prov_id in matched_districts:
        cursor.execute("SELECT city_id, city_title FROM transaction_city WHERE city_parent_id = %s", (cf_dist_id,))
        cf_wards = cursor.fetchall()
        
        candidates = wards_by_prov.get(prov_id, [])
        
        for cf_id, cf_name in cf_wards:
            cf_norm = normalize_name(cf_name)
            best_match = None
            best_score = 0.0
            
            for ch_id, ch_name, ch_d_id, _ in candidates:
                ch_norm = normalize_name(ch_name)
                
                # Logic: Exact > Contain > Fuzzy
                is_same_dist = (ch_d_id == ch_dist_id)
                score = 0
                
                if cf_norm == ch_norm:
                    score = 1.0 if is_same_dist else 0.95 # Cross-district exact match
                elif (cf_norm in ch_norm or ch_norm in cf_norm) and len(cf_norm) > 4:
                    score = 0.9 if is_same_dist else 0.7
                
                if score > best_score:
                    best_score = score
                    best_match = (ch_id, ch_name, ch_d_id, 'exact' if is_same_dist else 'manual_fix')

            if best_score >= 0.85 and best_match:
                batch_data.append((cf_id, cf_name, cf_dist_id, best_match[0], best_match[1], best_match[2], prov_id, best_match[3]))
                total_matched += 1
            else:
                batch_data.append((cf_id, cf_name, cf_dist_id, None, None, ch_dist_id, prov_id, 'not_found'))
                
            if len(batch_data) >= 1000:
                cursor.executemany("INSERT INTO cenhomes_cafeland_ward_mapping VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", batch_data)
                conn.commit()
                batch_data = []

    if batch_data:
        cursor.executemany("INSERT INTO cenhomes_cafeland_ward_mapping VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", batch_data)
        conn.commit()
        
    print(f"Wards Matched: {total_matched}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    match_provinces()
    match_districts()
    match_wards()
    print("\nDONE! All matching tables updated.")
