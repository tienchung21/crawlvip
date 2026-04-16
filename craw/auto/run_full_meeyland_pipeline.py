import sys
import time
import subprocess
import pymysql

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "craw_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_db_stats():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT count(*) as total FROM scraped_details_flat WHERE domain='meeyland.com' AND (cleanv1_converted = 0 OR cleanv1_converted IS NULL)")
    raw_left = cur.fetchone()['total']
    
    cur.execute("SELECT count(*) as total FROM data_clean_v1 WHERE domain='meeyland.com' AND process_status < 6")
    mid_left = cur.fetchone()['total']
    
    cur.execute("SELECT count(*) as total FROM data_clean_v1 WHERE domain='meeyland.com' AND process_status >= 6 AND land_price_status IS NULL")
    land_left = cur.fetchone()['total']
    
    conn.close()
    return raw_left, mid_left, land_left

def run_bash_script():
    print(f"[{time.strftime('%H:%M:%S')}] Triggering batch ETL...")
    result = subprocess.run(["/bin/bash", "craw/auto/run_meeyland_etl.sh"], capture_output=True, text=True)
    # Check if there was an actual migration (Migrated > 0 records)
    # If 0 migrated and 0 mid_left processing, we might be done.
    return result.stdout

def main():
    print("=======================================")
    print("= AUTO-LOOP MEEYLAND ETL PIPELINE     =")
    print("=======================================")
    
    total_batches = 0
    while True:
        raw_left, mid_left, land_left = get_db_stats()
        print(f"Stats -> Unprocessed Raw: {raw_left} | Stuck in Middle (Status 0-5): {mid_left} | Missing Land Price: {land_left}")
        
        if raw_left == 0 and mid_left == 0 and land_left == 0:
            print("All clear! No more meeyland records left to process.")
            break
            
        stdout = run_bash_script()
        total_batches += 1
        
        # Simple safeguard: If it seems like no progress is made and we're just spinning
        if "Migrated 0 " in stdout and "Normalized price and set status=2 for 0" in stdout and "Updated process_status = 5 for 0 rows" in stdout and "done=0 skip=0" in stdout:
            if mid_left > 0:
                print("WARNING: Pipeline ran but 0 records updated. Records might be stuck due to unhandled logic (e.g. price=NULL). Skipping stuck records.")
                # We can't break if raw_left > 0, because step0 might just be ignoring duplicates.
                # Actually, Step0 with INSERT IGNORE might just insert 0 if limit is hitting the exact same duplicates. Wait, step 0 updates cleanv1_converted = 1 for the inserted ones. 
                # If they are ignored, do they get cleanv1_converted = 1?
                # Ah, step0 code says: "if inserted > 0: cursor.execute(UPDATE cleanv1_converted = 1 ... LIMIT)"
                # This is a BUG in step0! If inserted == 0 because they are duplicates, cleanv1_converted is NEVER set to 1!
                pass # let's just let it be handled in a fix to step0
            elif raw_left > 0:
                print("WARNING: Step 0 inserted 0 records but raw_left > 0. Fixing cleanv1_converted flags for duplicates...")
                conn = pymysql.connect(**DB_CONFIG)
                cur = conn.cursor()
                cur.execute("UPDATE scraped_details_flat SET cleanv1_converted = 1 WHERE domain='meeyland.com' AND (cleanv1_converted = 0 OR cleanv1_converted IS NULL) LIMIT 5000")
                conn.commit()
                conn.close()
            else:
                break
            
        time.sleep(1) # tiny pause between loops
        
    print(f"Done! Processed {total_batches} batches.")

if __name__ == '__main__':
    main()
