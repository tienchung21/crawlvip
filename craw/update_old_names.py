
import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()
    
    print("=== UPDATING OLD WARD NAMES ===\n")
    
    # Update cafeland_ward_name_old from transaction_city based on cafeland_ward_id_old
    
    # We can use a JOIN UPDATE for speed (MySQL specific)
    # UPDATE location_batdongsan l
    # JOIN transaction_city t ON l.cafeland_ward_id_old = t.city_id
    # SET l.cafeland_ward_name_old = t.city_title
    # WHERE l.cafeland_ward_id_old IS NOT NULL;
    
    sql = """
    UPDATE location_batdongsan l
    JOIN transaction_city t ON l.cafeland_ward_id_old = t.city_id
    SET l.cafeland_ward_name_old = t.city_title
    WHERE l.cafeland_ward_id_old IS NOT NULL
    """
    
    print("Executing JOIN Update...")
    cursor.execute(sql)
    conn.commit()
    print(f"Updated {cursor.rowcount} rows with Old Names.")
    
    conn.close()

if __name__ == "__main__":
    run()
