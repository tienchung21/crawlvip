
import sys
import os

# Add parent directory to path to import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from craw.database import Database
except ImportError:
    # Handle running from different directories
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from database import Database

def verify_mapping():
    print("=== VERIFYING ID MAPPING ===\n")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()

    try:
        # Total converted rows
        cursor.execute("SELECT COUNT(*) FROM data_full")
        total = cursor.fetchone()[0]
        
        # Rows with valid Province ID
        cursor.execute("SELECT COUNT(*) FROM data_full WHERE province_id IS NOT NULL")
        mapped_prov = cursor.fetchone()[0]
        
        # Rows with valid Ward ID
        cursor.execute("SELECT COUNT(*) FROM data_full WHERE ward_id IS NOT NULL")
        mapped_ward = cursor.fetchone()[0]
        
        print(f"Total Rows in data_full: {total}")
        print(f"Mapped Province IDs:     {mapped_prov} ({mapped_prov/total*100:.2f}%)" if total else "0%")
        print(f"Mapped Ward IDs:         {mapped_ward} ({mapped_ward/total*100:.2f}%)" if total else "0%")
        
        # Show some unmapped examples
        if mapped_ward < total:
            print("\n--- Examples of Unmapped Addresses ---")
            cursor.execute("SELECT id, city, district, ward FROM data_full WHERE ward_id IS NULL LIMIT 10")
            unmapped = cursor.fetchall()
            for r in unmapped:
                print(f"ID {r[0]}: {r[3]}, {r[2]}, {r[1]}")
                
    except Exception as e:
        print(f"Verification Failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    verify_mapping()
