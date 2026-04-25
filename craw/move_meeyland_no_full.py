import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import Database

def move_meeyland_crawl_accounts_to_no_full():
    db = Database()
    conn = db.get_connection()
    try:
        with conn.cursor() as cursor:
            print("Step 1: Counting target records...")
            cursor.execute("""
                SELECT COUNT(*) as total 
                FROM data_full 
                WHERE source IN ('meeyland.com', 'meeymap.com') 
                AND broker_name LIKE '%Tài khoản Tin Crawl%'
            """)
            total = cursor.fetchone()['total']
            print(f"Total target records to move: {total}")

            if total == 0:
                print("No records found to move. Exiting.")
                return

            print("Step 2: Copying records to data_no_full...")
            # We copy all columns excluding auto-increment ID to avoid collision
            # but keep the original source_post_id
            columns = [
                "title", "address", "posted_at", "img", "price", "area", "description", 
                "property_type", "type", "house_direction", "floors", "bathrooms", 
                "road_width", "living_rooms", "bedrooms", "legal_status", "lat", "`long`", 
                "broker_name", "phone", "source", "time_converted_at", "source_post_id", 
                "width", "length", "city", "district", "ward", "street", "province_id", 
                "district_id", "ward_id", "street_id", "id_img", "project_name", 
                "slug_name", "images_status", "stratum_id", "cat_id", "type_id", 
                "unit", "project_id"
            ]
            cols_str = ", ".join(columns)
            
            insert_query = f"""
                INSERT IGNORE INTO data_no_full ({cols_str})
                SELECT {cols_str}
                FROM data_full
                WHERE source IN ('meeyland.com', 'meeymap.com') 
                AND broker_name LIKE '%Tài khoản Tin Crawl%'
            """
            cursor.execute(insert_query)
            inserted = cursor.rowcount
            print(f"Finished copying. Rows affected/inserted: {inserted}")
            
            print("Step 3: Deleting copied records from data_full...")
            delete_query = """
                DELETE FROM data_full 
                WHERE source IN ('meeyland.com', 'meeymap.com') 
                AND broker_name LIKE '%Tài khoản Tin Crawl%'
            """
            cursor.execute(delete_query)
            deleted = cursor.rowcount
            print(f"Finished deleting. Rows removed: {deleted}")
            
            conn.commit()
            print("All operations committed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error during move operation: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    move_meeyland_crawl_accounts_to_no_full()
