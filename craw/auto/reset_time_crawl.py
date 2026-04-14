import pymysql
import time

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                       charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cur:
        # Lấy min/max ID
        cur.execute("SELECT MAX(id) as max_id FROM data_clean_v1")
        max_id = cur.fetchone()['max_id']
        
    last_id = 0
    batch_size = 50000
    while last_id < max_id:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE data_clean_v1 
                SET time_crawl = NULL 
                WHERE id > %s AND id <= %s
            """, (last_id, last_id + batch_size))
        conn.commit()
        last_id += batch_size
        print(f"Reset {last_id}/{max_id}")
        time.sleep(0.01)
        
    with conn.cursor() as cur:
        # Cập nhật structure cột time_crawl để các dòng MỚI có DEFAULT CURRENT_TIMESTAMP
        # Trong khi các dòng CŨ sẽ vẫn giữ nguyên (tức là đã được đặt = NULL ở trên)
        cur.execute("""
            ALTER TABLE data_clean_v1 
            MODIFY COLUMN time_crawl DATETIME DEFAULT CURRENT_TIMESTAMP;
        """)
    conn.commit()
    print("Done resetting and setting DEFAULT CURRENT_TIMESTAMP.")
finally:
    conn.close()
