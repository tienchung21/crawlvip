
import sys
import os

sys.path.append(os.getcwd())
from craw.database import Database

def add_column():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        print("Checking if id_img column exists...")
        cursor.execute("DESCRIBE data_full")
        rows = cursor.fetchall()
        
        has_id_img = False
        for row in rows:
            field = row['Field'] if isinstance(row, dict) else row[0]
            if field == 'id_img':
                has_id_img = True
                break
        
        if has_id_img:
            print("id_img column already exists.")
        else:
            print("Adding id_img column to data_full...")
            cursor.execute("ALTER TABLE data_full ADD COLUMN id_img INT DEFAULT NULL")
            print("Column added successfully.")
            
            # Add index for performance
            # cursor.execute("CREATE INDEX idx_data_full_id_img ON data_full (id_img)")
            # print("Index added.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    add_column()
