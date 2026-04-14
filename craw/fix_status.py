
import os
import sys

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    pass

def main():
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    
    # Sync Status: If link_id exists in scraped_details_flat, Mark collected_links as DONE
    query = """
        UPDATE collected_links c
        JOIN scraped_details_flat s ON c.prj_id = s.link_id
        SET c.status = 'done'
        WHERE c.domain = 'batdongsan.com.vn' 
          AND (c.status IS NULL OR c.status != 'done')
    """
    
    print("Executing Sync Status (Mark DONE for existing details)...")
    cur.execute(query)
    rows = cur.rowcount
    conn.commit()
    
    print(f"Updated {rows} listings to 'done' (Fixed Duplicates).")
    conn.close()

if __name__ == "__main__":
    main()
