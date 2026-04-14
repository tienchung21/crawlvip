
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def add_column():
    print("=== ADDING project_name TO data_full ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # Check if exists first
        cursor.execute("DESCRIBE data_full")
        columns = [row['Field'] if isinstance(row, dict) else row[0] for row in cursor.fetchall()]
        
        if 'project_name' in columns:
            print("Column 'project_name' ALREADY EXISTS.")
        else:
            print("Adding 'project_name' column...")
            cursor.execute("ALTER TABLE data_full ADD COLUMN project_name VARCHAR(255) DEFAULT NULL")
            conn.commit()
            print("Column added successfully.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    add_column()
