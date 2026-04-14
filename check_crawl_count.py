
import os
import sys

sys.path.append(os.getcwd())
from craw.database import Database

def main():
    print("Init DB...")
    db = Database()
    print("Get Conn...")
    conn = db.get_connection()
    if conn is None:
        print("CONN IS NONE!")
        return

    cur = conn.cursor()
    
    print("Exec Query...")
    # Check Today's Links
    query = "SELECT count(*) FROM collected_links WHERE date(created_at) = CURRENT_DATE;"
    cur.execute(query)
    res = cur.fetchone()
    print(f"Result Raw: {res}")
    count = res[0]
    
    print(f"Total Links Collected Today: {count}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
