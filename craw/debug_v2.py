
import sys
import os
# Add current dir to path just in case
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from database import Database
except ImportError:
    # Try parent import
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from craw.database import Database

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
except Exception as e:
    print(f"Exception: {e}")
    import traceback
    traceback.print_exc()
