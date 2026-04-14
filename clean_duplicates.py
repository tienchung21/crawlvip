import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'craw'))
import time
from database import Database

def clean_duplicates():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("=== STARTING CLEANUP ===")
    
    try:
        # 1. Create unique_images table if not exists with correct data
        print("Creating unique_images temp table...")
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS unique_images")
        cursor.execute("""
            CREATE TEMPORARY TABLE unique_images AS
            SELECT * FROM (
                SELECT id, detail_id, image_url, 
                       ROW_NUMBER() OVER (
                           PARTITION BY detail_id, image_url 
                           ORDER BY 
                               CASE status 
                                   WHEN 'UPLOADED' THEN 1 
                                   WHEN 'PROCESSING' THEN 2 
                                   WHEN 'PENDING' THEN 3 
                                   ELSE 4 
                               END,
                               id ASC
                       ) as rn
                FROM scraped_detail_images
            ) t WHERE rn = 1
        """)
        # Index on ID for faster join
        cursor.execute("CREATE INDEX idx_id ON unique_images(id)")
        print("Temp table created.")
        
        # 2. Count
        cursor.execute("SELECT COUNT(*) FROM scraped_detail_images")
        total = cursor.fetchone()
        total = total['COUNT(*)'] if isinstance(total, dict) else total[0]
        
        cursor.execute("SELECT COUNT(*) FROM unique_images")
        unique = cursor.fetchone()
        unique = unique['COUNT(*)'] if isinstance(unique, dict) else unique[0]
        
        to_delete = total - unique
        print(f"Total: {total}, Unique: {unique}, To Delete: {to_delete}")
        
        if to_delete == 0:
            print("No duplicates to delete.")
            return

        # 3. Delete in chunks
        deleted_count = 0
        while True:
            # Delete rows NOT in unique_images
            # Note: MySQL DELETE with JOIN and LIMIT can be tricky.
            # Standard syntax: DELETE t1 FROM t1 ...
            
            sql = """
                DELETE sdi 
                FROM scraped_detail_images sdi 
                LEFT JOIN unique_images ui ON sdi.id = ui.id 
                WHERE ui.id IS NULL
            """
            # LIMIT is not directly supported in multi-table DELETE in some versions, 
            # but usually works in MariaDB. If not, we have to use WHERE id IN (SELECT id from ... limit)
            
            # Let's try simple DELETE JOIN first without LIMIT to see if index helps.
            # If we want LIMIT, we might need a subquery which is slow on DELETE.
            # A better chunking strategy if DELETE JOIN is slow:
            # DELETE FROM scraped_detail_images WHERE id IN (
            #    SELECT id FROM scraped_detail_images sdi LEFT JOIN unique_images ui ON sdi.id = ui.id WHERE ui.id IS NULL LIMIT 5000
            # ) 
            # But "You can't specify target table 'scraped_detail_images' for update in FROM clause".
            
            # So, plain DELETE JOIN is best if we trust the index.
            # To chunk, we can iterate ID ranges or just hope DELETE JOIN is fast with Index.
            # Since we killed previous one, maybe it was slow.
            
            # FASTEST STRATEGY: 
            # DELETE FROM scraped_detail_images WHERE id NOT IN (SELECT id FROM unique_images)
            # This is slow.
            
            # Let's try Loop Delete with LIMIT via ID list
            
            # Fetch IDs to delete
            cursor.execute("""
                SELECT sdi.id 
                FROM scraped_detail_images sdi 
                LEFT JOIN unique_images ui ON sdi.id = ui.id 
                WHERE ui.id IS NULL 
                LIMIT 5000
            """)
            rows = cursor.fetchall()
            if not rows:
                break
                
            ids = [r['id'] if isinstance(r, dict) else r[0] for r in rows]
            placeholders = ','.join(['%s'] * len(ids))
            
            cursor.execute(f"DELETE FROM scraped_detail_images WHERE id IN ({placeholders})", ids)
            conn.commit()
            
            deleted_count += len(ids)
            print(f"Deleted {deleted_count} / {to_delete} duplicates...")
            time.sleep(0.5) # Breath
            
        print("=== CLEANUP COMPLETED ===")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    clean_duplicates()
