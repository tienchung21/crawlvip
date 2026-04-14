
import sys
import os
import time

# Add parent directory to path to import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from craw.database import Database
except ImportError:
    # Handle running from different directories
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from database import Database

def run_etl_batched():
    print("=== STARTING MOGI DATA PROCESSING (BATCHED 5000) ===\n")
    
    # STEP 1: Conversion to data_full
    print(">>> STEP 1: Converting/Inserting into data_full table...")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    sql_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql_convert_mogi.sql')
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file not found at {sql_file_path}")
        return

    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read().strip()
            
        # Remove trailing semicolon if present to append LIMIT
        if sql_content.endswith(';'):
            sql_content = sql_content[:-1]
            
        total_inserted = 0
        batch_size = 5000
        batch_num = 1
        
        while True:
            start_time = time.time()
            
            # Construct batched SQL
            # We assume the SQL is an INSERT ... SELECT statement.
            # Adding LIMIT 5000 to INSERT ... SELECT works in MySQL.
            # Since the query has "AND NOT EXISTS", subsequent runs will pick up next items.
            batched_sql = f"{sql_content} LIMIT {batch_size}"
            
            cursor.execute(batched_sql)
            row_count = cursor.rowcount
            conn.commit()
            
            duration = time.time() - start_time
            total_inserted += row_count
            
            print(f"Batch {batch_num}: Inserted {row_count} rows in {duration:.2f}s.")
            
            if row_count == 0:
                print(">>> No more rows to insert. Finished.")
                break
                
            batch_num += 1
            # Optional: Short sleep to prevent absolute lock if running huge batches
            time.sleep(0.5)

        print(f"\n>>> Total Rows inserted into data_full: {total_inserted}")
        
    except Exception as e:
        print(f">>> Conversion FAILED: {e}")
    finally:
        cursor.close()
        conn.close()

    print("\n=== MOGI DATA PROCESSING COMPLETED ===")

if __name__ == "__main__":
    run_etl_batched()
