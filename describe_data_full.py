
import sys
import os

sys.path.append(os.getcwd())
from craw.database import Database

def describe_table():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("DESCRIBE data_full")
    rows = cursor.fetchall()
    
    print("--- SCHEMA of data_full ---")
    for row in rows:
        # row matches (Field, Type, Null, Key, Default, Extra)
        if isinstance(row, dict):
             print(f"{row['Field']} | {row['Type']}")
        else:
             print(f"{row[0]} | {row[1]}")

if __name__ == "__main__":
    describe_table()
