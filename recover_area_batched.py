
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

def recover_area():
    print("=== STARTING AREA RECOVERY (BATCHED ID RANGE) ===")
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
            # Logic: If area IS NULL, try to set it from served_details_flat (via id_img)
            # Using the same REGEX logic as SQL convert
            sql = f"""
                UPDATE data_full df
                JOIN scraped_details_flat sdf ON df.id_img = sdf.id
                SET df.area = CAST(
                    CASE 
                        WHEN sdf.dientichsudung IS NULL OR sdf.dientichsudung = '' THEN NULL
                        WHEN LENGTH(REPLACE(REGEXP_SUBSTR(sdf.dientichsudung, '[0-9]+([.,][0-9]+)?'), ',', '.')) > 8 THEN NULL 
                        ELSE NULLIF(REPLACE(REGEXP_SUBSTR(sdf.dientichsudung, '[0-9]+([.,][0-9]+)?'), ',', '.'), '')
                    END
                AS DECIMAL(10,2))
                WHERE df.id >= {current_id} AND df.id < {end_id}
                AND df.area IS NULL
                AND (sdf.dientichsudung IS NOT NULL AND sdf.dientichsudung != '')
            """
            try:
                t0 = time.time()
                cursor.execute(sql)
                cnt = cursor.rowcount
                conn.commit()
                dt = time.time() - t0
                
                total_updated += cnt
                if cnt > 0:
                     print(f"Recovered {cnt} rows in range [{current_id}, {end_id}) - {dt:.2f}s")
                     
            except Exception as e:
                print(f"Error in batch {current_id}: {e}")
                pass
            
            current_id = end_id

        print(f"\n=== RECOVERY COMPLETED ===")
        print(f"Total Rows Recovered: {total_updated}")
        print(f"Total Time: {time.time() - start_global:.2f}s")

    except Exception as e:
        print(f"Fatal Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    recover_area()
