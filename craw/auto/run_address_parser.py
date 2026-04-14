
import sys
import os

# Add parent directory to path to import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from craw.database import Database
except ImportError:
    from database import Database

def run_parsing():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        print("Checking schema...")
        # Check if columns exist
        needed_columns = ['city_ext', 'district_ext', 'ward_ext', 'street_ext']
        
        for col in needed_columns:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'scraped_details_flat' 
                AND COLUMN_NAME = '{col}'
            """)
            exists = cursor.fetchone()[0]
            
            if not exists:
                print(f"Adding column {col}...")
                cursor.execute(f"ALTER TABLE scraped_details_flat ADD COLUMN {col} VARCHAR(255) NULL AFTER diachi")
                cursor.execute(f"CREATE INDEX idx_{col} ON scraped_details_flat({col})")
            else:
                print(f"Column {col} already exists. Ensuring size is 255...")
                cursor.execute(f"ALTER TABLE scraped_details_flat MODIFY COLUMN {col} VARCHAR(255) NULL")

        print("Running Address Parsing (UPDATE)...")
        # Direct UPDATE query
        update_sql = """
            UPDATE scraped_details_flat
            SET 
                city_ext = TRIM(SUBSTRING_INDEX(diachi, ',', -1)),
                
                district_ext = CASE 
                    WHEN LENGTH(diachi) - LENGTH(REPLACE(diachi, ',', '')) >= 1 
                    THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(diachi, ',', -2), ',', 1))
                    ELSE NULL 
                END,

                ward_ext = CASE 
                    WHEN LENGTH(diachi) - LENGTH(REPLACE(diachi, ',', '')) >= 2 
                    THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(diachi, ',', -3), ',', 1))
                    ELSE NULL 
                END,

                street_ext = CASE 
                    WHEN LENGTH(diachi) - LENGTH(REPLACE(diachi, ',', '')) >= 3 
                    THEN TRIM(
                        SUBSTRING(
                            diachi, 
                            1, 
                            LENGTH(diachi) - LENGTH(SUBSTRING_INDEX(diachi, ',', -3)) - 1
                        )
                    )
                    ELSE NULL 
                END
            WHERE diachi IS NOT NULL AND (city_ext IS NULL OR city_ext = '')
        """
        cursor.execute(update_sql)
        row_count = cursor.rowcount
        conn.commit()
        print(f"Address parsing completed. Updated {row_count} rows.")
        
    except Exception as e:
        print(f"Critical Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_parsing()
