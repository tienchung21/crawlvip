
import sys
import os
import time

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    # Manual import if needed
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def backfill():
    print("=== STARTING BACKFILL id_img -> data_full (BATCHED ID RANGE) ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # Get Min/Max ID
        print("Getting ID range...")
        cursor.execute("SELECT MIN(id), MAX(id) FROM data_full")
        row = cursor.fetchone()
        if isinstance(row, dict):
             min_id = row['MIN(id)']
             max_id = row['MAX(id)']
        else:
             min_id = row[0]
             max_id = row[1]
             
        if min_id is None:
            print("Table empty.")
            return

        print(f"ID Range: {min_id} -> {max_id}")
        
        batch_size = 5000
        current_id = min_id
        total_updated = 0
        
        start_global = time.time()
        
        while current_id <= max_id:
            end_id = current_id + batch_size
            
            # Update range
            sql = f"""
                UPDATE data_full df
                JOIN scraped_details_flat sdf ON df.source_post_id = sdf.matin
                SET df.id_img = sdf.id
                WHERE df.id >= {current_id} AND df.id < {end_id}
                AND df.id_img IS NULL
            """
            try:
                t0 = time.time()
                cursor.execute(sql)
                cnt = cursor.rowcount
                conn.commit()
                dt = time.time() - t0
                
                total_updated += cnt
                if cnt > 0:
                     print(f"Updated {cnt} rows in range [{current_id}, {end_id}) - {dt:.2f}s")
                else:
                     # print(f"No update in range [{current_id}, {end_id})")
                     pass
                     
            except Exception as e:
                print(f"Error in batch {current_id}: {e}")
                # Try to continue?
                pass
            
            current_id = end_id
            # time.sleep(0.1) # Sleep slightly to yield locks

        print(f"\n=== COMPLETED ===")
        print(f"Total Updated: {total_updated}")
        print(f"Total Time: {time.time() - start_global:.2f}s")

    except Exception as e:
        print(f"Fatal Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    backfill()
