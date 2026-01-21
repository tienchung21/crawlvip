import pymysql
import time

# Config
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    print("=== RECREATING DATA_CLEAN FROM AD_LISTING_DETAIL ===")
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()

    try:
        # 1. Truncate data_clean
        print("Truncating data_clean...")
        cursor.execute("TRUNCATE TABLE data_clean")
        conn.commit()

        # 2. Insert Data
        # Mapping:
        # data_clean -> ad_listing_detail
        # ad_id -> ad_id
        # list_id -> list_id
        # list_time -> list_time
        # orig_list_time -> orig_list_time
        # region_v2 -> region_v2
        # area_v2 -> area_v2
        # ward -> ward
        # street_name -> street_name
        # street_number -> street_number
        # unique_street_id -> unique_street_id
        # category -> category
        # size -> size
        # price -> price
        # type -> type
        # time_crawl -> time_crawl
        # price_m2_vnd -> price_million_per_m2 * 1000000
        
        print("Inserting data (Ho Chi Minh, Binh Duong, Ba Ria - Vung Tau)...")
        sql = """
        INSERT INTO data_clean (
            ad_id, list_id, list_time, orig_list_time, 
            region_v2, area_v2, ward, 
            street_name, street_number, unique_street_id, 
            category, size, price, type, time_crawl, 
            price_m2_vnd
        )
        SELECT 
            ad_id, list_id, list_time, orig_list_time, 
            region_v2, area_v2, ward, 
            street_name, street_number, unique_street_id, 
            category, size, price, type, time_crawl, 
            (price_million_per_m2 * 1000000)
        FROM ad_listing_detail
        WHERE region_name LIKE '%Ho Chi Minh%' 
           OR region_name LIKE '%Binh Duong%' 
           OR region_name LIKE '%Ba Ria%'
        """
        cursor.execute(sql)
        rows = cursor.rowcount
        conn.commit()
        print(f"Inserted {rows} rows.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run()
