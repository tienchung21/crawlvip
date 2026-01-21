import pymysql

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'
BATCH_SIZE = 10000

def run():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()

    # Get total count first
    cursor.execute("SELECT COUNT(*) FROM ad_listing_detail WHERE region_v2 IN (13000, 12000, 2010)")
    total = cursor.fetchone()[0]
    print(f"Total rows to insert: {total}")

    offset = 0
    total_inserted = 0

    while offset < total:
        sql = f"""
        INSERT INTO data_clean (
            ad_id, list_id, list_time, orig_list_time, 
            region_v2, area_v2, ward, 
            street_name, street_number, unique_street_id, 
            category, size, price, type, time_crawl, 
            price_m2_vnd
        )
        SELECT 
            ad_id, list_id, list_time, orig_list_time, 
            region_v2, area_v2, ward, 
            street_name, street_number, unique_street_id, 
            category, size, price, type, time_crawl, 
            (price_million_per_m2 * 1000000)
        FROM ad_listing_detail
        WHERE region_v2 IN (13000, 12000, 2010)
        LIMIT {BATCH_SIZE} OFFSET {offset}
        """
        cursor.execute(sql)
        conn.commit()
        inserted = cursor.rowcount
        total_inserted += inserted
        print(f"Batch done: +{inserted} rows | Total: {total_inserted}/{total} | Offset: {offset}")
        offset += BATCH_SIZE

    print(f"\n=== COMPLETED: {total_inserted} rows inserted ===")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run()
