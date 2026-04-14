
import sys
import os

# Add parent directory to path to import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from craw.database import Database
except ImportError:
    from database import Database

def recreate_table():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("Dropping old data_full table...")
    cursor.execute("DROP TABLE IF EXISTS data_full")
    
    print("Creating new data_full table...")
    create_sql = """
    CREATE TABLE IF NOT EXISTS data_full (
      id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
      title VARCHAR(255) NULL,
      slug_name VARCHAR(255) NULL,
      address VARCHAR(255) NULL,
      posted_at DATETIME NULL,
      img TEXT NULL,
      price DECIMAL(20,2) NULL,
      area DECIMAL(20,2) NULL,
      description LONGTEXT NULL,
      property_type VARCHAR(100) NULL,
      type VARCHAR(50) NULL,
      house_direction VARCHAR(50) NULL,
      floors INT NULL,
      bathrooms INT NULL,
      road_width DECIMAL(10,2) NULL,
      living_rooms INT NULL,
      bedrooms INT NULL,
      legal_status VARCHAR(100) NULL,
      lat DECIMAL(10,7) NULL,
      `long` DECIMAL(10,7) NULL,
      broker_name VARCHAR(255) NULL,
      phone VARCHAR(50) NULL,
      source VARCHAR(50) NULL,
      time_converted_at DATETIME NULL,
      source_post_id VARCHAR(100) NULL,
      INDEX idx_source (source),
      INDEX idx_source_post_id (source_post_id),
      INDEX idx_posted_at (posted_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(create_sql)
    
    print("Adding width and length columns...")
    alter_sql = """
    ALTER TABLE data_full
    ADD COLUMN width DECIMAL(10,2) NULL,
    ADD COLUMN length DECIMAL(10,2) NULL
    """
    cursor.execute(alter_sql)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Successfully recreated data_full table.")

if __name__ == "__main__":
    recreate_table()
