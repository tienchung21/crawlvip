
import sys
import os
import time

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def delete_null_area():
    print("=== DELETING ROWS WITH NULL AREA ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        start_t = time.time()
        cursor.execute("DELETE FROM data_full WHERE area IS NULL")
        deleted_count = cursor.rowcount
        conn.commit()
        print(f">>> Deleted {deleted_count} rows with NULL area in {time.time() - start_t:.2f}s.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    delete_null_area()
