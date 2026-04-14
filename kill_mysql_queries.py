import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'craw'))
from database import Database

def kill_stuck_queries():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("Checking processlist...")
    cursor.execute('SHOW PROCESSLIST')
    rows = cursor.fetchall()
    
    stuck_found = False
    for row in rows:
        # Check for Long running queries related to images
        pid = row['Id'] if isinstance(row, dict) else row[0]
        info = row['Info'] if isinstance(row, dict) else row[7]
        time_sec = row['Time'] if isinstance(row, dict) else row[5]
        
        if info and ('scraped_detail_images' in info or 'DELETE' in info or 'INSERT' in info) and time_sec > 60:
            print(f"KILLING Id {pid} (Time: {time_sec}s): {info[:100]}...")
            try:
                cursor.execute(f'KILL {pid}')
                print(" -> KILLED")
                stuck_found = True
            except Exception as e:
                print(f" -> Failed to kill: {e}")

    if not stuck_found:
        print("No stuck queries found.")
    
    conn.close()

if __name__ == "__main__":
    kill_stuck_queries()
