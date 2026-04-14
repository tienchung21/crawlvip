
import sys
import os
import time

sys.path.append(os.getcwd())
from craw.database import Database

def add_index():
    print("=== ADDING INDEX ON scraped_details_flat (matin) ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        start_time = time.time()
        print("Executing CREATE INDEX (This may take a while)...")
        # Check if index exists first to avoid error?
        # Or just use try/except
        try:
             cursor.execute("CREATE INDEX idx_sdf_matin ON scraped_details_flat (matin)")
             print("Index idx_sdf_matin created successfully.")
        except Exception as e:
             if "Duplicate key name" in str(e):
                 print("Index already exists.")
             else:
                 raise e
        
        duration = time.time() - start_time
        print(f"Time taken: {duration:.2f}s")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    add_index()
