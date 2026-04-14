
import time
import sys
import os
sys.path.insert(0, 'craw')
from database import Database

def fix_mismatched_status():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("Fixing mismatched links (Status != DONE but Detail exists)...")
    
    total_fixed = 0
    while True:
        # Update in batches of 500 to avoid lock size error
        cursor.execute('''
            UPDATE collected_links c
            JOIN scraped_details_flat s ON c.url = s.url
            SET c.status = 'DONE'
            WHERE c.status != 'DONE' AND c.status != 'FAILED'
            LIMIT 500
        ''')
        count = cursor.rowcount
        conn.commit()
        
        total_fixed += count
        print(f"Fixed batch: {count} rows")
        
        if count == 0:
            break
            
        time.sleep(1) # Breathe
        
    print(f"Total fixed: {total_fixed}")
    conn.close()

if __name__ == "__main__":
    fix_mismatched_status()
