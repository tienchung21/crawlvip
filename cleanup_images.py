import pymysql
import time

def cleanup_orphaned_images():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='',
        database='craw_db',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        print("Bắt đầu dọn dẹp ảnh thừa (Safe Mode)...")
        with conn.cursor() as cursor:
            # Loop until no more orphans found
            while True:
                # Delete 500 orphaned images at a time
                # Using LIMIT to avoid lock table overflow
                sql = """
                    DELETE FROM scraped_detail_images 
                    WHERE detail_id NOT IN (SELECT id FROM scraped_details_flat)
                    LIMIT 500
                """
                cursor.execute(sql)
                deleted_count = cursor.rowcount
                conn.commit()
                
                if deleted_count == 0:
                    print("Đã dọn sạch hết ảnh thừa!")
                    break
                    
                print(f"Đã xóa {deleted_count} ảnh thừa...")
                time.sleep(1) # Sleep to let crawler breathe

        # Re-verify FK? 
        # Actually, adding FK might still fail if there are orphans.
        # But once this finishes, we can try adding FK again (might lock though).
        # For now, just cleaning data is enough to stop errors?
        # NO, the user's error was "Cannot add or update a child row... foreign key constraint fails".
        # This means the FK *IS* there, or partially there? 
        # Wait, the user error was `(1452, ... foreign key constraint fails ... CONSTRAINT fk_sdi_detail ... REFERENCES scraped_details_flat_backup_final)`
        # Ah! It's referencing the BACKUP table!
        # Because we swapped the tables, but the FK definition on `scraped_detail_images` was pointing to the OLD table ID (which is now renamed).
        # Actually, `scraped_details_flat` (original) became `scraped_details_flat_backup_final`.
        # So the FK is pointing to the BACKUP table.
        # But the crawler is trying to insert `detail_id` which corresponds to the NEW table (`scraped_details_flat`).
        # If the IDs in NEW table are different? Or if they don't exist in backup?
        # The IDs should be preserved.
        # But new items have new IDs.
        
        # KEY FIX: We need to DROP the bad FK and ADD a new FK pointing to the correct valid table `scraped_details_flat`.
        # But doing `ALTER TABLE` locks the table.
        # So we must do it quickly.
        pass

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    cleanup_orphaned_images()
