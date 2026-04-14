
import os
import sys

sys.path.append(os.getcwd())
from craw.database import Database

def main():
    print("Adding column prj_id...")
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    
    # Check if exists
    try:
        cur.execute("SELECT prj_id FROM collected_links LIMIT 1;")
        print("Column prj_id already exists.")
    except Exception:
        # Add column
        print("Column missing. Adding...")
        # Since transaction might be aborted by error, we need new cursor or rollback
        conn.rollback()
        try:
            cur.execute("ALTER TABLE collected_links ADD COLUMN prj_id BIGINT;")
            conn.commit()
            print("Added prj_id (BIGINT) successfully.")
        except Exception as e:
            print(f"Failed to ADD COLUMN: {e}")
            conn.rollback()

    conn.close()

if __name__ == "__main__":
    main()
