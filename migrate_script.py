import pymysql
import time

def migrate_data():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='',
        database='craw_db',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with conn.cursor() as cursor:
            # 1. Create new table
            print("Creating new table...")
            cursor.execute("DROP TABLE IF EXISTS scraped_details_flat_temp_v2")
            cursor.execute("CREATE TABLE scraped_details_flat_temp_v2 LIKE scraped_details_flat")
            
            # 2. Add UNIQUE index BEFORE inserting (to handle distinct)
            # Or AFTER? BEFORE is better to filter duplicates on fly.
            print("Adding UNIQUE index...")
            try:
                cursor.execute("ALTER TABLE scraped_details_flat_temp_v2 ADD UNIQUE INDEX idx_sdf_link_unique (link_id)")
            except Exception as e:
                print(f"Warning adding index: {e}")

            # 3. Get ID range
            cursor.execute("SELECT MIN(id) as min_id, MAX(id) as max_id FROM scraped_details_flat")
            row = cursor.fetchone()
            min_id = row['min_id'] or 0
            max_id = row['max_id'] or 0
            print(f"Migrating rows from ID {min_id} to {max_id}...")

            # 4. Loop Copy
            batch_size = 5000
            total_copied = 0
            
            for start_id in range(min_id, max_id + 1, batch_size):
                end_id = start_id + batch_size - 1
                
                # INSERT IGNORE INTO new SELECT * FROM old ...
                # We need to list columns? No, * is fine if schema matches.
                # Use ORDER BY id DESC to ensure if multiple items exist in block, we ideally get latest?
                # Actually INSERT IGNORE on UNIQUE index: first one wins.
                # So we should ORDER BY id DESC?
                # "INSERT IGNORE ... SELECT ... ORDER BY id DESC" might not work as expected for bulk.
                # But duplicates usually appear at different times.
                # If we process chunk by chunk, checking duplicates across chunks?
                # No, UNIQUE index enforces global uniqueness.
                # The issue: If a duplicate exists in chunk A (id=10) and chunk B (id=1000000).
                # We process chunk A first -> inserts id=10.
                # We process chunk B later -> inserts id=1000000 -> Ignored (duplicate).
                # Result: We kept the OLD one (id=10). We want the NEW one!
                
                # Strategy fix: Process from MAX_ID down to MIN_ID!
                # If we go backwards:
                # Chunk B (latest) -> Inserts id=1000000. Success.
                # Chunk A (old) -> Inserts id=10. Fail (Duplicate).
                # Result: We kept the NEW one. Perfect!
                
                pass 
            
            # Re-implement loop for descending order
            current_max = max_id
            while current_max >= min_id:
                start_id = max(min_id, current_max - batch_size + 1)
                end_id = current_max
                
                sql = f"""
                    INSERT IGNORE INTO scraped_details_flat_temp_v2 
                    SELECT * FROM scraped_details_flat 
                    WHERE id BETWEEN {start_id} AND {end_id} 
                    ORDER BY id DESC
                """
                cursor.execute(sql)
                count = cursor.rowcount
                conn.commit()
                
                total_copied += count
                # print(f"Processed range {start_id}-{end_id}. Inserted {count} rows.")
                
                current_max = start_id - 1
                time.sleep(0.1) # Yield
            
            print(f"Coyping complete. Total inserted: {total_copied}")
            
            # 5. Swap Tables
            print("Swapping tables...")
            # We rename old to backup, new to main.
            # But what about rows inserted into OLD table during migration?
            # They are lost. 
            # To minimize this: Run one last small sync for rows > max_id_init?
            
            # Get new max_id
            cursor.execute("SELECT MAX(id) FROM scraped_details_flat")
            new_real_max = cursor.fetchone()['MAX(id)'] or max_id
            
            if new_real_max > max_id:
                print(f"Catching up {new_real_max - max_id} new rows...")
                sql = f"""
                    INSERT IGNORE INTO scraped_details_flat_new 
                    SELECT * FROM scraped_details_flat 
                    WHERE id > {max_id}
                    ORDER BY id DESC
                """
                cursor.execute(sql)
                conn.commit()
            
            cursor.execute("""
                RENAME TABLE scraped_details_flat TO scraped_details_flat_backup, 
                             scraped_details_flat_new TO scraped_details_flat
            """)
            print("Swap complete!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_data()
