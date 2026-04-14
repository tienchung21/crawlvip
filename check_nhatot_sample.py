
import pymysql
import time
from datetime import datetime
import sys

def check_sample():
    try:
        conn = pymysql.connect(
            host='localhost', user='root', password='', database='craw_db',
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
        )
        cur = conn.cursor()
        
        # Check tables list
        cur.execute("SHOW TABLES LIKE 'ad_listing%'")
        tables = cur.fetchall()
        print("--- TABLES ---")
        for t in tables:
            print(t.values())
            
        # Get Sample from TODAY
        now = datetime.now()
        start_of_day = datetime(now.year, now.month, now.day)
        start_ms = int(start_of_day.timestamp() * 1000)
        end_ms = 1900000000000
        
        print(f"\n--- SAMPLE RECORDS (Crawled Today >= {start_of_day} Local) ---")
        sql = """
            SELECT ad_id, time_crawl, list_time, region_name
            FROM ad_listing_detail 
            WHERE time_crawl >= %s AND time_crawl < %s
            ORDER BY time_crawl DESC
            LIMIT 5
        """
        cur.execute(sql, (start_ms, end_ms))
        rows = cur.fetchall()
        
        for row in rows:
            tc = row['time_crawl']
            lt = row['list_time'] # often massive int or ms
            
            tc_str = datetime.fromtimestamp(tc/1000).strftime('%Y-%m-%d %H:%M:%S')
            
            # List time handling
            lt_str = "N/A"
            if lt:
                try:
                    lt_val = float(lt)
                    # Detect if ms or sec
                    if lt_val > 1000000000000: lt_val /= 1000
                    lt_str = datetime.fromtimestamp(lt_val).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    lt_str = str(lt)
            
            print(f"ID: {row['ad_id']} | Crawled: {tc_str} | Listed: {lt_str} | Region: {row['region_name']}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    check_sample()
