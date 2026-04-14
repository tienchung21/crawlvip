import sys
import os
sys.path.append('/home/chungnt/crawlvip/craw')
from database import Database

try:
    print("Initializing Database class...")
    db = Database()
    print("Getting connection...")
    conn = db.get_connection()
    if conn:
        print("Connection object obtained. Testing query...")
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print("DB CONNECT SUCCESS: Query executed.")
        conn.close()
    else:
        print("DB CONNECT FAIL: Connection object is None")
except Exception as e:
    print(f"DB CONNECT FAIL: {e}")
