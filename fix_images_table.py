import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'craw'))
from database import Database

def fix_table():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("=== STARTING TABLE FIX ===")
    
    try:
        # 1. Check if old table exists (in case ran twice)
        cursor.execute("SHOW TABLES LIKE 'scraped_detail_images_backup'")
        if cursor.fetchone():
            print("Backup table already exists. We will populate from main table but NOT rename it again to avoid losing data if main table is already new/empty.")
            # Verify if main table has unique index
            cursor.execute("SHOW INDEX FROM scraped_detail_images WHERE Key_name = 'idx_unique_detail_url'")
            if cursor.fetchone():
               print("Main table already has unique index. Skipping rename.")
            else:
               print("Main table missing unique index. Proceeding with caution.")
        else:
            # RENAME
            print("Renaming scraped_detail_images -> scraped_detail_images_backup...")
            cursor.execute("RENAME TABLE scraped_detail_images TO scraped_detail_images_backup")
            print("Renamed.")

        # 2. Create NEW table with UNIQUE INDEX
        print("Creating new table with UNIQUE constraints...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_detail_images (
                id INT AUTO_INCREMENT PRIMARY KEY,
                detail_id INT NOT NULL,
                image_url VARCHAR(2000) NOT NULL,
                idx INT DEFAULT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
                ftp_path VARCHAR(2000) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_sdi_detail (detail_id),
                INDEX idx_sdi_status (status),
                INDEX idx_sdi_url (image_url(150)),
                UNIQUE INDEX idx_unique_detail_url (detail_id, image_url(255)),
                CONSTRAINT fk_sdi_detail FOREIGN KEY (detail_id) REFERENCES scraped_details_flat(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        print("New table created.")

        # 3. Backfill data
        print("Migrating data from backup (Unique only, prioritizing UPLOADED)...")
        # Note: We process in chunks to avoid timeout, leveraging python loop OR single query?
        # Single query INSERT IGNORE ... SELECT ... ORDER BY might be slow but let's try.
        # ORDER BY in INSERT SELECT is valid.
        
        # To avoid timeout, we might need to do it without ORDER BY?
        # No, ORDER BY is critical for preference.
        
        # Let's try single query first. If timeout, we split.
        cursor.execute('''
            INSERT IGNORE INTO scraped_detail_images (detail_id, image_url, idx, status, ftp_path, created_at)
            SELECT detail_id, image_url, idx, status, ftp_path, created_at
            FROM scraped_detail_images_backup
            ORDER BY 
                CASE status 
                    WHEN 'UPLOADED' THEN 1 
                    WHEN 'PROCESSING' THEN 2 
                    ELSE 3 
                END, 
                id ASC
        ''')
        row_count = cursor.rowcount
        conn.commit()
        print(f"Migrated {row_count} unique rows.")

        # 4. Verify count
        cursor.execute("SELECT COUNT(*) FROM scraped_detail_images")
        count = cursor.fetchone()
        count = count['COUNT(*)'] if isinstance(count, dict) else count[0]
        print(f"New table count: {count}")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    fix_table()
