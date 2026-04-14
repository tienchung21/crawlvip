
import pymysql
import time
from datetime import datetime
import sys

def check_nhatot_clean():
    try:
        conn = pymysql.connect(
            host='localhost', user='root', password='', database='craw_db',
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
        )
        cur = conn.cursor()
        
        # Start of Today (Local) -> UTC Ms
        now = datetime.now()
        start_of_day = datetime(now.year, now.month, now.day)
        start_ms = int(start_of_day.timestamp() * 1000)
        # End of Reasonable Time (Year 2030)
        end_ms = 1900000000000 
        
        print(f"Checking VALID data from {start_of_day} to Year ~2030")
        
        sql = """
            SELECT region_name, COUNT(*) as cnt
            FROM ad_listing_detail 
            WHERE time_crawl >= %s AND time_crawl < %s
            GROUP BY region_name
            ORDER BY cnt DESC
        """
        cur.execute(sql, (start_ms, end_ms))
        rows = cur.fetchall()
        
        print("\n=== PHAN BO TIN NHATOT MOI (REAL) ===")
        print(f"{'Tinh / Thanh Pho':<30} | {'So Luong'}")
        print("-" * 45)
        
        total = 0
        for row in rows:
            rname = row['region_name'] or 'Unknown'
            count = row['cnt']
            print(f"{rname:<30} | {count}")
            total += count
            
        print("-" * 45)
        print(f"{'TONG CONG':<30} | {total}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    check_nhatot_clean()
