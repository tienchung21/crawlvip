
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def check_lat_long():
    print("=== CHECKING LAT/LONG DATA ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # Check data_full
        cursor.execute("SELECT COUNT(*) FROM data_full")
        total = cursor.fetchone()
        total = total['COUNT(*)'] if isinstance(total, dict) else total[0]
        
        cursor.execute("SELECT COUNT(*) FROM data_full WHERE lat IS NOT NULL AND `long` IS NOT NULL")
        has_coords = cursor.fetchone()
        has_coords = has_coords['COUNT(*)'] if isinstance(has_coords, dict) else has_coords[0]
        
        print(f"Table 'data_full':")
        print(f"- Total Rows: {total}")
        print(f"- Rows with Lat/Long: {has_coords} ({has_coords/total*100:.2f}%)")
        
        # Check scraped_details_flat (Mogi)
        cursor.execute("SELECT COUNT(*) FROM scraped_details_flat WHERE domain='mogi'")
        total_mogi = cursor.fetchone()
        total_mogi = total_mogi['COUNT(*)'] if isinstance(total_mogi, dict) else total_mogi[0]
        
        cursor.execute("SELECT COUNT(*) FROM scraped_details_flat WHERE domain='mogi' AND map IS NOT NULL AND map != ''")
        has_map = cursor.fetchone()
        has_map = has_map['COUNT(*)'] if isinstance(has_map, dict) else has_map[0]
        
        print(f"\nTable 'scraped_details_flat' (Mogi):")
        print(f"- Total Rows: {total_mogi}")
        print(f"- Rows with 'map' data: {has_map} ({has_map/total_mogi*100:.2f}%)")
        
        # Show sample
        if has_map > 0:
            cursor.execute("SELECT map FROM scraped_details_flat WHERE domain='mogi' AND map IS NOT NULL LIMIT 3")
            print("\nSample 'map' data from Mogi:")
            for row in cursor.fetchall():
                print(f"- {list(row.values())[0] if isinstance(row, dict) else row[0]}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_lat_long()
