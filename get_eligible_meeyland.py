import pymysql

try:
    conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db')
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        for tbl in ['data_full', 'data_no_full']:
            print(f"[{tbl}] Meeyland Eligible for upload:")
            
            # Eligible condition:
            # - source = 'meeyland.com'
            # - has city (province_id), ward (ward_id), price
            # - images_status is NOT 'LISTING_UPLOADED'
            
            cur.execute(f"""
                SELECT images_status, COUNT(*) as c
                FROM {tbl}
                WHERE source = 'meeyland.com'
                  AND province_id IS NOT NULL 
                  AND ward_id IS NOT NULL
                  AND price IS NOT NULL
                  AND (images_status != 'LISTING_UPLOADED' OR images_status IS NULL)
                GROUP BY images_status
                ORDER BY c DESC
            """)
            res = cur.fetchall()
            
            total = 0
            for r in res:
                st = r['images_status'] if r['images_status'] else 'NULL'
                count = r['c']
                total += count
                print(f"  - {st}: {count:,}")
            
            if not res:
                print("  - 0")
            print(f"  -> TỔNG ({tbl}): {total:,}\n")

finally:
    conn.close()
