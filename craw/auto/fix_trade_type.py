import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from database import Database
except ImportError:
    from craw.database import Database

def backfill_trade_type():
    print("=== BACKFILLING TRADE_TYPE IN SCRAPED_DETAILS_FLAT ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # Update scraped_details_flat from collected_links based on link_id or url
        # Use simple join on link_id if available, else url
        
        print("Executing Update Query...")
        start = time.time()
        
        # 1. Update based on link_id (Faster)
        sql_id = """
        UPDATE scraped_details_flat sdf
        JOIN collected_links cl ON sdf.link_id = cl.id
        SET sdf.trade_type = cl.trade_type
        WHERE (sdf.trade_type IS NULL OR sdf.trade_type = '')
          AND cl.trade_type IS NOT NULL
        """
        cursor.execute(sql_id)
        rows_id = cursor.rowcount
        conn.commit()
        print(f"Updated {rows_id} rows using Link ID.")
        
        # 2. Update based on URL (Fallback)
        sql_url = """
        UPDATE scraped_details_flat sdf
        JOIN collected_links cl ON sdf.url = cl.url
        SET sdf.trade_type = cl.trade_type
        WHERE (sdf.trade_type IS NULL OR sdf.trade_type = '')
          AND cl.trade_type IS NOT NULL
        """
        cursor.execute(sql_url)
        rows_url = cursor.rowcount
        conn.commit()
        print(f"Updated {rows_url} rows using URL.")
        
        print(f"Done in {time.time() - start:.2f}s")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    backfill_trade_type()
