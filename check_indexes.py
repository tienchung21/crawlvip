
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    from craw.database import Database

def check_indexes():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    tables = ['data_full', 'scraped_details_flat']
    
    for table in tables:
        print(f"--- INDEXES ON {table} ---")
        cursor.execute(f"SHOW INDEX FROM {table}")
        rows = cursor.fetchall()
        for row in rows:
            # Row dict keys: Table, Non_unique, Key_name, Seq_in_index, Column_name...
            if isinstance(row, dict):
                 print(f"Key: {row['Key_name']} | Column: {row['Column_name']}")
            else:
                 print(f"Key: {row[2]} | Column: {row[4]}")
        print("")

if __name__ == "__main__":
    check_indexes()
