import pymysql
import re
import unicodedata
from thefuzz import fuzz

def normalize_text(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ascii', 'ignore').decode('utf-8')
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def main():
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="craw_db",
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    # Tạo bảng mapping
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `duan_homedy_duan_merge` (
      `homedy_project_id` bigint(20) NOT NULL,
      `homedy_project_name` varchar(500) DEFAULT NULL,
      `homedy_project_url` varchar(1000) DEFAULT NULL,
      `homedy_city_id` int(11) DEFAULT NULL,
      `homedy_district_id` int(11) DEFAULT NULL,
      `duan_id` int(11) NOT NULL,
      `duan_ten` varchar(250) DEFAULT NULL,
      `duan_tinh_moi` int(11) DEFAULT NULL,
      `match_type` varchar(32) NOT NULL,
      `score` double DEFAULT NULL,
      `created_at` timestamp NULL DEFAULT current_timestamp(),
      `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
      PRIMARY KEY (`homedy_project_id`),
      KEY `idx_duan_id` (`duan_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    cursor.execute(create_table_sql)
    cursor.execute("TRUNCATE TABLE `duan_homedy_duan_merge`")

    # Load All Dự Án từ hệ thống data gốc (bảng duan + location)
    # Tuy nhiên vì `duan` không có `tinh_id` nên thường sẽ phải join với `duan_bando`
    # Hoặc dựa vào project mapping các project đã biết. 
    # Nhưng vì yêu cầu đơn giản, tao sẽ get `duan` và `duan_bando` nếu cần
    cursor.execute("SELECT duan_id, duan_title FROM duan")
    sys_projects = cursor.fetchall()
    
    sys_projects_dict = {}
    sys_projects_norm = {}
    for p in sys_projects:
        did = p['duan_id']
        title = p['duan_title']
        sys_projects_dict[did] = p
        norm = normalize_text(title)
        if norm:
            sys_projects_norm[did] = norm

    # Get duan_homedy
    cursor.execute("""
        SELECT h.project_id, h.homedy_id, h.project_name, h.project_url, h.city_id, h.district_id,
               c.cafeland_id AS mapped_city_id
        FROM duan_homedy h
        LEFT JOIN location_homedy c ON h.city_id = c.location_id AND c.level_type = 'city'
    """)
    homedy_projects = cursor.fetchall()

    insert_data = []
    
    for hp in homedy_projects:
        # scraped_details_flat.thuocduan stores Homedy Product.ProjectId, which maps to duan_homedy.homedy_id.
        # DO NOT use duan_homedy.project_id here because that is "Code" from project API and does not
        # match listing ProjectId.
        homedy_id = hp.get('homedy_id')
        if not homedy_id:
            continue
        name = hp['project_name']
        norm_name = normalize_text(name)
        
        best_match_id = None
        best_score = 0
        best_match_type = ''
        
        # Exact match
        for did, dnorm in sys_projects_norm.items():
            if dnorm == norm_name:
                best_match_id = did
                best_score = 100
                best_match_type = 'EXACT'
                break
                
        # Fuzzy match nếu không có exact match
        if not best_match_id and norm_name:
            for did, dnorm in sys_projects_norm.items():
                # Dùng token_set_ratio thay vì ratio để tăng tỷ lệ tìm kiếm (VD: "Asiana Riverside" vs "Asiana Riverside (Shizen Home)")
                score = fuzz.token_set_ratio(norm_name, dnorm)
                if score > best_score:
                    best_score = score
                    best_match_id = did
            
            if best_score >= 90:
                best_match_type = 'FUZZY_HIGH'
            else:
                best_match_id = None
                
        if best_match_id:
            insert_data.append((
                homedy_id,
                name,
                hp['project_url'],
                hp['city_id'],
                hp['district_id'],
                best_match_id,
                sys_projects_dict[best_match_id]['duan_title'],
                hp['mapped_city_id'],
                best_match_type,
                best_score
            ))

    if insert_data:
        sql = """
        INSERT INTO duan_homedy_duan_merge (
            homedy_project_id, homedy_project_name, homedy_project_url, 
            homedy_city_id, homedy_district_id, duan_id, duan_ten, 
            duan_tinh_moi, match_type, score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # chunking
        batch_size = 1000
        for i in range(0, len(insert_data), batch_size):
            cursor.executemany(sql, insert_data[i:i+batch_size])
        
    conn.commit()
    print(f"Mapped {len(insert_data)} / {len(homedy_projects)} projects.")
    
    conn.close()

if __name__ == "__main__":
    main()
