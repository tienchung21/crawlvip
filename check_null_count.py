
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    # Manual import if needed
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def check_nulls():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM data_full WHERE id_img IS NULL")
    row = cursor.fetchone()
    
    if isinstance(row, dict):
        count = list(row.values())[0] # First value
    else:
        count = row[0]
        
    cursor.execute("SELECT COUNT(*) FROM data_full")
    row_total = cursor.fetchone()
    if isinstance(row_total, dict):
        total = list(row_total.values())[0]
    else:
        total = row_total[0]
        
    print(f"Total Rows: {total}")
    print(f"Rows with NULL id_img: {count}")

if __name__ == "__main__":
    check_nulls()
