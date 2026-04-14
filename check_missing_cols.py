
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    # Manual import if needed
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def check_missing_columns():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Check property_type and type
    print("Checking NULLs for property_type and type...")
    cursor.execute("SELECT COUNT(*) FROM data_full WHERE property_type IS NULL OR type IS NULL")
    row = cursor.fetchone()
    if isinstance(row, dict): count = list(row.values())[0]
    else: count = row[0]
    
    print(f"Rows with NULL property_type/type: {count}")

if __name__ == "__main__":
    check_missing_columns()
