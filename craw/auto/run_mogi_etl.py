
import sys
import os
import time
import unicodedata
import re

# Add parent directory to path to import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from craw.database import Database
    # Import the address parsing logic directly
    from craw.auto.run_address_parser import run_parsing as run_address_parsing
except ImportError:
    # Handle running from different directories
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from database import Database
    from run_address_parser import run_parsing as run_address_parsing

def remove_accents(s):
    if not s: return ""
    s = str(s)
    s = s.replace('đ', 'd').replace('Đ', 'd') 
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

def normalize_slug(name):
    if not name: return ""
    name = remove_accents(name)
    name = name.lower().strip()
    name = re.sub(r'[^a-z0-9\s-]', '', name)
    name = re.sub(r'\s+', '-', name)
    return name

def backfill_slug_names(batch_size: int = 1000):
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    total_updated = 0
    try:
        while True:
            cursor.execute(
                """
                SELECT id, title
                FROM data_full
                WHERE (slug_name IS NULL OR slug_name = '')
                  AND title IS NOT NULL AND title <> ''
                LIMIT %s
                """,
                (batch_size,)
            )
            rows = cursor.fetchall()
            if not rows:
                break
            updates = []
            for row in rows:
                if isinstance(row, tuple):
                    row_id, title = row[0], row[1]
                else:
                    row_id, title = row.get('id'), row.get('title')
                slug = normalize_slug(title)
                if slug:
                    updates.append((slug, row_id))
            if updates:
                cursor.executemany(
                    "UPDATE data_full SET slug_name=%s WHERE id=%s",
                    updates
                )
                conn.commit()
                total_updated += len(updates)
        if total_updated:
            print(f">>> Backfill slug_name updated: {total_updated} rows")
        else:
            print(">>> Backfill slug_name: nothing to update")
    except Exception as e:
        print(f">>> Backfill slug_name FAILED: {e}")
    finally:
        cursor.close()
        conn.close()

def ensure_datafull_converted_column():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW COLUMNS FROM scraped_details_flat LIKE 'datafull_converted'")
        if not cursor.fetchone():
            cursor.execute(
                "ALTER TABLE scraped_details_flat "
                "ADD COLUMN datafull_converted TINYINT(1) NOT NULL DEFAULT 0"
            )
            conn.commit()
            print(">>> Added column scraped_details_flat.datafull_converted")

        cursor.execute("SHOW INDEX FROM scraped_details_flat WHERE Key_name='idx_sdf_mogi_datafull_conv'")
        if not cursor.fetchone():
            cursor.execute(
                "ALTER TABLE scraped_details_flat "
                "ADD INDEX idx_sdf_mogi_datafull_conv (domain, datafull_converted, matin)"
            )
            conn.commit()
            print(">>> Added index idx_sdf_mogi_datafull_conv")
    finally:
        cursor.close()
        conn.close()

def mark_existing_mogi_rows_as_converted():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE scraped_details_flat sdf
            JOIN data_full df
              ON df.source = 'mogi'
             AND df.source_post_id = sdf.matin
            SET sdf.datafull_converted = 1
            WHERE sdf.domain = 'mogi'
              AND (sdf.datafull_converted IS NULL OR sdf.datafull_converted = 0)
              AND sdf.matin IS NOT NULL
              AND sdf.matin <> ''
            """
        )
        conn.commit()
        print(f">>> Backfill datafull_converted updated: {cursor.rowcount} rows")
    finally:
        cursor.close()
        conn.close()

def run_etl():
    print("=== STARTING MOGI DATA PROCESSING (Direct ID Mapping) ===\n")
    print(">>> STEP 0: Ensuring datafull_converted schema...")
    ensure_datafull_converted_column()
    print(">>> STEP 0.1: Backfilling existing converted rows...")
    mark_existing_mogi_rows_as_converted()
    
    # STEP 1: Conversion to data_full
    print(">>> STEP 1: Converting/Inserting into data_full table...")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    sql_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql_convert_mogi.sql')
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file not found at {sql_file_path}")
        return

    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        start_time = time.time()
        
        # Execute the INSERT statement
        cursor.execute(sql_content)
        row_count = cursor.rowcount
        conn.commit()
        duration = time.time() - start_time
        
        print(f">>> Conversion DONE in {duration:.2f}s.")
        print(f">>> Rows inserted into data_full: {row_count}")
        print(">>> STEP 1.1: Marking newly converted rows in scraped_details_flat...")
        cursor.execute(
            """
            UPDATE scraped_details_flat sdf
            JOIN data_full df
              ON df.source = 'mogi'
             AND df.source_post_id = sdf.matin
            SET sdf.datafull_converted = 1
            WHERE sdf.domain = 'mogi'
              AND (sdf.datafull_converted IS NULL OR sdf.datafull_converted = 0)
              AND sdf.matin IS NOT NULL
              AND sdf.matin <> ''
            """
        )
        marked_count = cursor.rowcount
        conn.commit()
        print(f">>> datafull_converted marked: {marked_count} rows")
        
    except Exception as e:
        print(f">>> Conversion FAILED: {e}")
    finally:
        cursor.close()
        conn.close()

    # STEP 1.2: Backfill slug_name for old + new rows
    print(">>> STEP 1.2: Backfilling slug_name...")
    backfill_slug_names()

    print("\n=== MOGI DATA PROCESSING COMPLETED ===")

if __name__ == "__main__":
    run_etl()
