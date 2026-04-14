
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from craw.database import Database
except ImportError:
    # Try alternate path if running from root
    sys.path.append(os.path.join(os.getcwd(), 'craw'))
    from database import Database

def check_today():
    try:
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"Checking records for Created Date: {today}")
        
        # 1. Check collected_links
        # Note: Mogi usually has domain='mogi.vn' or similar. 
        # We'll check ALL first, then filter by domain if needed.
        query_collected = f"SELECT count(*) as cnt, domain FROM collected_links WHERE DATE(created_at) = '{today}' GROUP BY domain"
        cursor.execute(query_collected)
        rows_collected = cursor.fetchall()
        
        print("\n--- TABLE: collected_links (Listing) ---")
        if not rows_collected:
            print("No records found for today.")
        else:
            total = 0
            for row in rows_collected:
                # row is dict if DictCursor, or tuple if standard
                if isinstance(row, dict):
                    d = row.get('domain') or 'Unknown'
                    c = row.get('cnt')
                else:
                    c = row[0]
                    d = row[1] if len(row) > 1 else 'Unknown'
                print(f"Domain: {d} | Count: {c}")
                total += c
            print(f"TOTAL LISTINGS TODAY: {total}")

        # 2. Check scraped_details
        query_details = f"SELECT count(*) as cnt, success FROM scraped_details WHERE DATE(created_at) = '{today}' GROUP BY success"
        cursor.execute(query_details)
        rows_details = cursor.fetchall()
        
        print("\n--- TABLE: scraped_details (Details) ---")
        if not rows_details:
             print("No records found for today.")
        else:
            total_det = 0
            for row in rows_details:
                if isinstance(row, dict):
                    s = row.get('success')
                    c = row.get('cnt')
                else:
                    c = row[0]
                    s = row[1] if len(row) > 1 else 'Unknown'
                status = "Success" if s else "Failed"
                print(f"Status: {status} | Count: {c}")
                total_det += c
            print(f"TOTAL DETAILS TODAY: {total}")

        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error checking DB: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_today()
