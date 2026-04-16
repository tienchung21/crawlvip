import pymysql
import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print("=== homedy_step6_normalize_date.py ===")
    
    # The standard is to map update_time to std_date and median_flag
    # Wait, we already parsed update_time and orig_list_time as Unix epochs in Step 0.
    # Now we just set standard date field, and finalize.
    
    start = time.time()
    
    # Step 6 query: assign std_date and mark complete. No complicated processing needed for this simple step as timestamp already parsed.
    
    update_sql = """
        UPDATE data_clean_v1 
        SET 
            std_date = DATE(FROM_UNIXTIME(orig_list_time)),
            median_flag = IF(orig_list_time IS NOT NULL, 1, NULL),
            process_status = IF(orig_list_time IS NOT NULL, 6, -6), 
            last_script = 'homedy_step6_normalize_date.py'
        WHERE domain = 'homedy.com' 
          AND process_status = 5
        LIMIT %s
    """
    
    cursor.execute(update_sql, (args.limit,))
    updated = cursor.rowcount
    conn.commit()
    
    print(f"-> Assigned std_date, median_flag=1 and finalized status=6 for {updated} homedy records in {time.time()-start:.2f}s.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
