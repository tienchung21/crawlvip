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

PYTHON_BIN = sys.executable

def get_count_raw():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT count(*) as total FROM scraped_details_flat WHERE domain='homedy.com' AND (cleanv1_converted = 0 OR cleanv1_converted IS NULL)")
    cnt = cur.fetchone()['total']
    conn.close()
    return cnt

def get_count_step(status):
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) as total FROM data_clean_v1 WHERE domain='homedy.com' AND process_status = {status}")
    cnt = cur.fetchone()['total']
    conn.close()
    return cnt

def get_count_land():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) as total FROM data_clean_v1 WHERE domain='homedy.com' AND process_status >= 6 AND land_price_status IS NULL")
    cnt = cur.fetchone()['total']
    conn.close()
    return cnt

def run_script_in_loop(script_path, limit, get_count_fn, name_log, extra_args=None):
    print(f"\n--- Running {name_log} until completion ---")
    while True:
        left = get_count_fn()
        print(f"[{name_log}] Rows remaining: {left}")
        if left == 0:
            print(f"[{name_log}] Done!")
            break
            
        cmd = [PYTHON_BIN, script_path, "--limit", str(limit)]
        if extra_args:
            cmd.extend(extra_args)
            
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout.strip())
        if result.returncode != 0:
            err = (result.stderr or "").strip()
            if err:
                print(err)
            print(f"[{name_log}] Error: script exited with code {result.returncode}. Breaking to avoid infinite loop.")
            break

        if result.returncode != 0:
            print(result.stderr.strip())
            print(f"[{name_log}] Error: step script exited with code {result.returncode}. Breaking to avoid infinite loop.")
            break
        
        # Anti-freeze: if output says "0 records" we must break to avoid infinite loop
        if "for 0 homedy " in result.stdout or "Migrated 0 homedy " in result.stdout or "for 0 rows" in result.stdout or "done=0 skip=0" in result.stdout or "No rows to process" in result.stdout:
            if "Total Migrated" not in result.stdout: # handle step 0 uniqueness
                print(f"[{name_log}] Warning: Processed 0 records but {left} left. Breaking to prevent infinite loop. Please check DB for stuck records.")
                break

def main():
    print("=======================================")
    print("= HOMEDY ETL LINEAR PIPELINE        =")
    print("=======================================")
    
    limit = 5000
    
    # Step 0
    run_script_in_loop("craw/auto/homedy_step0_recreate.py", limit, get_count_raw, "Step 0 (Migrate Raw)")
    
    # Step 1
    run_script_in_loop("craw/auto/homedy_step1_mergekhuvuc.py", limit, lambda: get_count_step(0), "Step 1 (Location)")
    
    # Step 2
    run_script_in_loop("craw/auto/homedy_step2_normalize_price.py", limit, lambda: get_count_step(1), "Step 2 (Price)")
    
    # Step 3
    run_script_in_loop("craw/auto/homedy_step3_normalize_size.py", limit, lambda: get_count_step(2), "Step 3 (Size)")
    
    # Step 4
    run_script_in_loop("craw/auto/homedy_step4_normalize_type.py", limit, lambda: get_count_step(3), "Step 4 (Type)")
    
    # Step 5
    run_script_in_loop("craw/auto/homedy_step5_group_median.py", limit, lambda: get_count_step(4), "Step 5 (Median Group)")
    
    # Step 6
    run_script_in_loop("craw/auto/homedy_step6_normalize_date.py", limit, lambda: get_count_step(5), "Step 6 (Date)")
    
    # Step 7
    print(f"\n--- Running Step 7 (Land Price) until completion ---")
    while True:
        left = get_count_land()
        print(f"[Step 7] Rows remaining: {left}")
        if left == 0:
            print("[Step 7] Done!")
            break
        cmd = [PYTHON_BIN, "craw/auto/step7_apply_land_price.py", "--domain", "homedy.com", "--batch-size", str(limit)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout.strip())
        if result.returncode != 0:
            print(result.stderr.strip())
            print(f"[Step 7] Error: script exited with code {result.returncode}. Breaking.")
            break
        if "done=0 skip=0" in result.stdout:
            print("[Step 7] Processed 0 records. Breaking infinite loop.")
            break

    print("\n=======================================")
    print("= LINEAR PIPELINE FINISHED COMPLETELY =")
    print("=======================================")

if __name__ == '__main__':
    main()
