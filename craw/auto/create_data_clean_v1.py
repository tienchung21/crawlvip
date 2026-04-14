import mysql.connector
from mysql.connector import Error

def create_table():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='craw_db',
            user='root',
            password=''
        )

        if connection.is_connected():
            cursor = connection.cursor()
            
            # Drop table if exists to start fresh (for dev purpose, be careful in prod)
            cursor.execute("DROP TABLE IF EXISTS data_clean_v1")
            
            create_table_sql = """
            CREATE TABLE data_clean_v1 (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ad_id VARCHAR(50) NOT NULL UNIQUE COMMENT 'ID tin đăng gốc từ Nhatot',
                
                -- 3 Cột ID Nguồn (Source - Nhatot)
                src_province_id VARCHAR(50) COMMENT 'ID Tỉnh gốc (regionmapped)',
                src_district_id VARCHAR(50) COMMENT 'ID Huyện gốc (areamapped)',
                src_ward_id VARCHAR(50) COMMENT 'ID Xã gốc (ward)',
                
                -- 4 Cột ID Đích (Target - Cafeland Chuẩn)
                cf_province_id INT COMMENT 'ID Tỉnh Cafeland',
                cf_district_id INT COMMENT 'ID Huyện Cafeland',
                cf_ward_id INT COMMENT 'ID Xã Cafeland',
                cf_street_id BIGINT COMMENT 'ID Đường Cafeland',
                project_id BIGINT COMMENT 'ID dự án (nếu có)',
                
                -- Dữ liệu Gốc (Source Data - để đối chiếu)
                src_size VARCHAR(50) COMMENT 'Diện tích gốc (text)',
                unit VARCHAR(20) COMMENT 'Đơn vị diện tích gốc (m2, md, thang, ...)',
                src_price VARCHAR(50) COMMENT 'Giá gốc (text)',
                src_category_id VARCHAR(50) COMMENT 'ID Category gốc',
                src_type VARCHAR(50) COMMENT 'Loại tin gốc',
                
                -- Các cột dữ liệu chuẩn hóa (Standardized - std)
                std_area FLOAT COMMENT 'Diện tích chuẩn (m2) - std_size',
                std_category VARCHAR(50) COMMENT 'Loại hình BĐS chuẩn',
                std_trans_type VARCHAR(20) COMMENT 'Loại giao dịch (ban/thue)',
                std_date DATE COMMENT 'Ngày đăng chuẩn YYYY-MM-DD',
                
                -- Các cột dữ liệu quan trọng khác
                price_vnd BIGINT COMMENT 'Giá trị tuyệt đối (VND)',
                price_m2 DECIMAL(18,2) COMMENT 'Đơn giá trên m2',
                
                -- Metadata và Time
                orig_list_time BIGINT COMMENT 'Time tạo tin gốc',
                update_time BIGINT COMMENT 'Time cập nhật tin (list_time)',
                url TEXT COMMENT 'Link tới tin đăng',
                domain VARCHAR(50) COMMENT 'Nguồn (nhatot, mogi, ...)',
                last_script VARCHAR(100) COMMENT 'Script xử lý cuối cùng',
                process_status TINYINT DEFAULT 0 COMMENT 'Trạng thái xử lý (0: Chưa, 1-6: Các bước)',
                
                INDEX idx_process (process_status),
                INDEX idx_cf_district (cf_district_id),
                INDEX idx_std_date (std_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            
            cursor.execute(create_table_sql)
            print("Table 'data_clean_v1' created successfully.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_table()
