
import sys
import os
import time
from database import Database

def update_fields():
    print("=== UPDATING data_full FROM scraped_details_flat ===")
    
    # Initialize DB (using 'craw_db' as discovered)
    db = Database(database='craw_db')
    conn = db.get_connection()
    cursor = conn.cursor()
    
    batch_size = 5000
    
    # Get Max ID to determine range
    try:
        cursor.execute("SELECT MAX(id) as m FROM scraped_details_flat")
        res = cursor.fetchone()
        if isinstance(res, dict):
            max_id = res.get('m', 0)
        elif res and len(res) > 0:
            max_id = res[0]
        else:
            max_id = 0
        
        print(f"Max SDF ID: {max_id}")
    except Exception as e:
        print(f"Error getting max ID: {e}")
        return

    if max_id == 0:
        print("No data in scraped_details_flat.")
        return

    total_updated = 0
    start_time = time.time()
    
    # Iterate in chunks
    for i in range(1, max_id + 1, batch_size):
        end = i + batch_size - 1
        # print(f"Processing chunk {i} - {end} ...")
        
        sql = """
        UPDATE data_full df
        JOIN scraped_details_flat sdf ON df.source_post_id = sdf.matin
        SET 
            df.project_name = sdf.thuocduan,
            df.id_img = sdf.id,
            df.lat = CAST(SUBSTRING_INDEX(sdf.map, ',', 1) AS DECIMAL(12,8)),
            df.`long` = CAST(SUBSTRING_INDEX(sdf.map, ',', -1) AS DECIMAL(12,8)),
            df.floors = sdf.sotang,
            df.house_direction = sdf.huongnha,
            df.road_width = sdf.duongvao
        WHERE 
            df.source = 'mogi' 
            AND sdf.id BETWEEN %s AND %s
            -- Update only if missing or potentially outdated
            AND (df.project_name IS NULL OR df.lat IS NULL OR df.id_img IS NULL)
        """
        
        try:
            cursor.execute(sql, (i, end))
            cnt = cursor.rowcount
            conn.commit()
            if cnt > 0:
                print(f"Chunk {i}-{end}: Updated {cnt} rows.")
                total_updated += cnt
            
            # Brief pause to be nice to DB
            time.sleep(0.05)
            
        except Exception as e:
            print(f"Error in chunk {i}-{end}: {e}")
            # Try to reconnect if lost
            try:
                conn.close()
                conn = db.get_connection()
                cursor = conn.cursor()
            except:
                pass

    duration = time.time() - start_time
    print(f"Done in {duration:.2f}s. Total updated: {total_updated}")
    conn.close()

if __name__ == "__main__":
    update_fields()
