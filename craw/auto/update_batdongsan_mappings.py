import pymysql
import time

BATCH_SIZE = 10000

def main():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='craw_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    print("=== Starting Update Batdongsan Mappings (Old -> New) ===")
    print("Source: location_batdongsan.cafeland_ward_id_old")
    print("Mapping: transaction_city_merge (old_city_id -> new_city_id)")
    start_time = time.time()

    # Update cafeland_ward_id_new and cafeland_province_id_new
    print("\nUpdating location_batdongsan...")
    total_updated = 0
    while True:
        # Update records where old ID exists but new ID is NULL or different
        sql = f"""
        UPDATE location_batdongsan lb
        JOIN transaction_city_merge tm ON lb.cafeland_ward_id_old = tm.old_city_id
        SET 
            lb.cafeland_ward_id_new = tm.new_city_id,
            lb.cafeland_province_id_new = tm.new_city_parent_id,
            lb.cafeland_ward_name_new = tm.new_city_name
        WHERE lb.cafeland_ward_id_old IS NOT NULL
          AND tm.new_city_id IS NOT NULL
          AND (lb.cafeland_ward_id_new IS NULL 
               OR lb.cafeland_ward_id_new != tm.new_city_id 
               OR lb.cafeland_ward_name_new != tm.new_city_name)
        LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql)
        rows = cursor.rowcount
        conn.commit()
        total_updated += rows
        print(f"  Updated batch: {rows} records (Total: {total_updated})")
        if rows < BATCH_SIZE:
            break

    end_time = time.time()
    print(f"\n=== Completed in {end_time - start_time:.2f}s ===")
    print(f"Total Updated: {total_updated}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
