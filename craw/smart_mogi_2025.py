
import sys
import os
import time
import subprocess
from datetime import datetime

# Setup path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database

def run_smart_update():
    print("=== SMART MOGI UPDATE 2025-2026 ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # 1. Identify Target URLs (2025/2026 + Missing Data)
    # We look at data_full (or scraped_details_flat) to find items that need update.
    # We join with collected_links to reset status.
    
    print("1. Searching for incomplete records (2025-2026)...")
    
    # Query logic:
    # Get URLs where:
    # - Posted date is 2025 or 2026
    # - AND (lat is NULL OR long is NULL OR project_name is NULL) -- actually project_name might be null validly, but user said 'update missing'
    # - But Lat/Long should definitively be present for Mogi (via Map)
    
    # Note: data_full has 'posted_at' (DATE) and 'ngaydang' (VARCHAR).
    # scraped_details_flat has 'ngaydang'.
    
    # Let's target scraped_details_flat for "raw" source truth
    sql_reset = """
        UPDATE collected_links cl
        JOIN scraped_details_flat sdf ON cl.url = sdf.url
        SET cl.status = 'PENDING'
        WHERE 
            (sdf.ngaydang LIKE '%2025%' OR sdf.ngaydang LIKE '%2026%')
            AND (
                sdf.map IS NULL 
                OR sdf.map = ''
                OR sdf.thuocduan IS NULL 
            )
            AND cl.domain = 'mogi.vn'
    """
    
    try:
        # We might want to be more aggressive and just re-crawl ALL 2025/2026 to be safe?
        # User said: "update nhung truong ... con thieu" (update missing fields).
        # Let's stick to missing fields to save resources, but include those with empty map.
        
        cursor.execute(sql_reset)
        count = cursor.rowcount
        conn.commit()
        print(f"-> Reset {count} links to PENDING for re-crawling.")
        
    except Exception as e:
        print(f"Error resetting links: {e}")
        return

    # 2. Run Detail Crawler
    if count > 0:
        print("2. Starting Detail Crawler (Subprocess)...")
        cmd = [
            sys.executable, 
            "craw/mogi_fast_crawler.py", 
            "--threads", "25", 
            "--batch", "50",
            "--delay-min", "0.5",
            "--delay-max", "1"
        ]
        
        try:
            subprocess.run(cmd, check=True)
            print("Detail crawl completed.")
        except Exception as e:
            print(f"Error running detail crawl: {e}")
    else:
        print("No records matched the criteria. Nothing to update.")

    conn.close()

if __name__ == "__main__":
    run_smart_update()
