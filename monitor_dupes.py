import time
import pymysql
import sys
from datetime import datetime

def check_new_duplicates():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='',
        database='craw_db',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    start_time = datetime.now()
    print(f"[{start_time}] Bắt đầu theo dõi trùng lặp trong 5 phút...")
    
    try:
        for i in range(10): # 10 lần x 30s = 5 phút
            # Check for duplicates created AFTER start_time
            with conn.cursor() as cursor:
                # Query: Find URLs that have > 1 entry created/updated recently?
                # Actually, simpler: Find any url that has count > 1 in the whole table, 
                # BUT restricted to items created recently? 
                # No, just check if ANY new duplicates appeared.
                
                # Check 1: Total duplicates count change?
                # This is heavy.
                
                # Check 2: Just check for duplicates among recently inserted rows.
                sql = """
                    SELECT url, COUNT(*) as c 
                    FROM scraped_details_flat 
                    WHERE created_at >= %s
                    GROUP BY url 
                    HAVING c > 1
                """
                cursor.execute(sql, (start_time,))
                dupes = cursor.fetchall()
                
                if dupes:
                   print(f"[{datetime.now()}] CẢNH BÁO: Phát hiện {len(dupes)} link bị trùng lặp mới!")
                   for d in dupes[:3]:
                       print(f"  - {d['url']} (x{d['c']})")
                else:
                   print(f"[{datetime.now()}] Ổn. Không có trùng lặp mới.")
                   
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("Đã dừng.")
    finally:
        conn.close()

if __name__ == "__main__":
    check_new_duplicates()
