
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def get_url():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT url FROM collected_links WHERE domain='mogi' ORDER BY id DESC LIMIT 2")
        rows = cursor.fetchall()
        
        urls = []
        for row in rows:
            u = list(row.values())[0] if isinstance(row, dict) else row[0]
            urls.append(u)
            
        print(f"URLS: {urls}")
        if not urls:
             print("No Mogi URL found.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    get_url()
