
from database import Database
import sys

try:
    print("Connecting...")
    db = Database(database='craw_db')
    conn = db.get_connection()
    print(f"Connected. DB: {db.database}")
    cursor = conn.cursor()
    print("Executing query...")
    cursor.execute("SELECT MAX(id) FROM scraped_details_flat")
    res = cursor.fetchone()
    print(f"Result: {res}")
    if res:
        print(f"Max ID: {res[0]}")
    conn.close()
except Exception as e:
    print(f"Exception type: {type(e)}")
    print(f"Exception: {e}")
    import traceback
    traceback.print_exc()
