
import os
import sys

sys.path.append(os.getcwd())
from craw.database import Database

def main():
    print("Listing ALL Tables:")
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SHOW TABLES;")
    rows = cur.fetchall()
    for r in rows:
        # Tuple (name,) or Dict
        if isinstance(r, dict):
            print(f" - {list(r.values())[0]}")
        else:
            print(f" - {r[0]}")
    conn.close()

if __name__ == "__main__":
    main()
