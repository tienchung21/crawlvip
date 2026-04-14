import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'craw'))
from database import Database

def check_status():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()

    print('Checking tables...')
    cursor.execute("SHOW TABLES LIKE 'scraped_detail_images%'")
    tables = cursor.fetchall()
    print('Tables found:')
    for t in tables:
        print(t)

    print('\nChecking row counts...')
    for table in ['scraped_detail_images', 'scraped_detail_images_backup']:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            res = cursor.fetchone()
            count = res['COUNT(*)'] if isinstance(res, dict) else res[0]
            print(f'{table} count: {count}')
        except Exception as e:
            print(f'{table} error: {e}')

    conn.close()

if __name__ == "__main__":
    check_status()
