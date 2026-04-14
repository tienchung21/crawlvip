
import sys
import os
import time

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    # Manual import if needed
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def clean_and_analyze():
    print("=== STARTING CLEANUP & ANALYSIS ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Delete Orphans
        print("1. Deleting rows with id_img IS NULL...")
        start_t = time.time()
        cursor.execute("DELETE FROM data_full WHERE id_img IS NULL")
        deleted_count = cursor.rowcount
        conn.commit()
        print(f">>> Deleted {deleted_count} orphan rows in {time.time() - start_t:.2f}s.")
        
        # 2. Analyze Null Area
        print("\n2. Analyzing NULL area rows...")
        cursor.execute("SELECT COUNT(*) FROM data_full WHERE area IS NULL")
        null_area_count = cursor.fetchone()
        if isinstance(null_area_count, dict): null_area_count = list(null_area_count.values())[0]
        else: null_area_count = null_area_count[0]
        
        print(f">>> Total rows with NULL area: {null_area_count}")
        
        if null_area_count > 0:
            # Check recoverable count (has dientichsudung but area is null)
            # Use regex logic similar to SQL convert
            print("   Checking how many have 'dientichsudung' in source (recoverable)...")
            sql_check = """
                SELECT COUNT(*)
                FROM data_full df
                JOIN scraped_details_flat sdf ON df.id_img = sdf.id
                WHERE df.area IS NULL
                  AND (sdf.dientichsudung IS NOT NULL AND sdf.dientichsudung != '')
            """
            cursor.execute(sql_check)
            recoverable = cursor.fetchone()
            if isinstance(recoverable, dict): recoverable = list(recoverable.values())[0]
            else: recoverable = recoverable[0]
            
            print(f">>> Rows recoverable from 'dientichsudung': {recoverable}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    clean_and_analyze()
