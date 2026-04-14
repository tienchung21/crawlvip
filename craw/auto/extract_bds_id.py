
import os
import sys
import re
import time

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
    from craw.database import Database

def main():
    print("Extracting BDS Project IDs from URLs...")
    db = Database()
    conn = db.get_connection()
    if not conn:
        print("DB FAILED")
        return
        
    cur = conn.cursor()
    
    # regex for 'pr' followed by digits. 
    # Usually at end of string or before query param
    # e.g. -pr123456
    # some urls end with .htm? No, Batdongsan usually no extension or just slug.
    # regex: pr(\d+)
    
    pattern = re.compile(r'pr(\d+)')
    
    # Fetch chunk by chunk or limit
    # We loop until no more NULLs
    
    batch_size = 50000
    total_updated = 0
    
    while True:
        # Fetch Next Batch of NULLs
        print(f"Fetching next {batch_size} un-id links...")
        query = """
            SELECT id, url 
            FROM collected_links 
            WHERE (domain = 'batdongsan.com.vn' OR url LIKE '%%batdongsan.com.vn%%')
              AND prj_id IS NULL
            LIMIT %s
        """
        cur.execute(query, (batch_size,))
        rows = cur.fetchall()
        
        if not rows:
            print("No more missing IDs. Done.")
            break
            
        print(f"Processing {len(rows)} rows...")
        updates = []
        
        for row in rows:
            if isinstance(row, dict):
                lid = row['id']
                url = row['url']
            else:
                lid = row[0]
                url = row[1]
            
            # Extract
            # Search from right to left?
            # usually pr... at end
            match = pattern.search(url)
            if match:
                pr_id = match.group(1)
                updates.append((pr_id, lid))
            else:
                # If no 'pr', try finding just last digits if user url format is diff?
                # User's sample: 40422702.
                # Usually pr + digits.
                # If fail, ignore or mark?
                pass
                
        if updates:
            print(f"Updating {len(updates)} IDs...")
            # Chunk Update
            chunk_s = 5000
            update_query = "UPDATE collected_links SET prj_id = %s WHERE id = %s"
            
            for i in range(0, len(updates), chunk_s):
                batch = updates[i:i+chunk_s]
                try:
                    cur.executemany(update_query, batch)
                    conn.commit()
                except Exception as e:
                    print(f"Update Error: {e}")
                    conn.rollback()
            
            total_updated += len(updates)
            print(f"Total Updated so far: {total_updated}")
        else:
            print("No matches in this batch (weird).")
            # If we don't update prj_id, we will fetch them again forever!
            # We must mark them as processed or something?
            # Or assume most have IDs.
            # If loop stuck -> Break
            # But query selects IS NULL.
            # If we fail to find ID, prj_id stays NULL.
            # Infinite Loop Hazard!
            
            # Fix: Update prj_id = 0 for failed ones
            # So they are not fetched again
            failed_ids = []
            matched_ids = set(u[1] for u in updates)
            for row in rows:
                rid = row['id'] if isinstance(row, dict) else row[0]
                if rid not in matched_ids:
                    failed_ids.append((0, rid))
            
            if failed_ids:
                print(f"Marking {len(failed_ids)} as ID=0 (Not Found)...")
                for i in range(0, len(failed_ids), chunk_s):
                     batch = failed_ids[i:i+chunk_s]
                     try:
                         cur.executemany(update_query, batch)
                         conn.commit()
                     except: pass

    conn.close()

if __name__ == "__main__":
    main()
