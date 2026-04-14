
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def check_add_column():
    print("=== CHECKING thuocduan COLUMN ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("DESCRIBE scraped_details_flat")
        columns = []
        rows = cursor.fetchall()
        for row in rows:
            # Handle both dict and tuple cursor
            col_name = list(row.values())[0] if isinstance(row, dict) else row[0]
            columns.append(col_name)
        
        if 'thuocduan' in columns:
            print("Column 'thuocduan' ALREADY EXISTS.")
        else:
            print("Column 'thuocduan' MISSING. Adding it...")
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN thuocduan VARCHAR(255) DEFAULT NULL")
            conn.commit()
            print("Column 'thuocduan' ADDED successfully.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_add_column()
