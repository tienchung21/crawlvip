
import pymysql
import sys

def check_schema():
    try:
        conn = pymysql.connect(
            host='localhost', user='root', password='', database='craw_db',
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
        )
        cur = conn.cursor()
        
        print("--- SHOW CREATE TABLE ad_listing_detail ---")
        cur.execute("SHOW CREATE TABLE ad_listing_detail")
        row = cur.fetchone()
        if row:
            # Usually returns {'Table': '...', 'Create Table': '...'}
            print(row.get('Create Table') or row)
        else:
            print("Table not found!")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
