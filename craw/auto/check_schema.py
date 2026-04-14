import pymysql

def main():
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='',
            database='craw_db',
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        print("Checking scraped_details_flat schema...")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'scraped_details_flat';
        """)
        cols = cursor.fetchall()
        if not cols:
            print("Table scraped_details_flat NOT FOUND.")
        else:
            for c in cols:
                # Handle Tuple/Dict
                if isinstance(c, dict): print(f" - {c['column_name']} ({c['data_type']})")
                else: print(f" - {c[0]} ({c[1]})")
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
