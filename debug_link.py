
import sys
import os
sys.path.insert(0, 'craw')
from database import Database

def debug_link():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    url_segment = 'id22726248'
    
    print(f"Searching for segment: {url_segment}")

    # Check DETAIL
    print('\n--- CHECKING scraped_details_flat ---')
    cursor.execute("SELECT id, url, title FROM scraped_details_flat WHERE url LIKE %s", ('%' + url_segment + '%',))
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f"Found in DETAIL: ID={row['id']}")
            print(f"  URL={row['url']}")
            # Also check if this detail has a corresponding collected_link_id ?
            # Assuming id in flat table might map to collected_links? No, usually separate.
    else:
        print("Not found in DETAIL")

    # Check COLLECT
    print('\n--- CHECKING collected_links ---')
    cursor.execute("SELECT id, url, status FROM collected_links WHERE url LIKE %s", ('%' + url_segment + '%',))
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f"Found in COLLECT: ID={row['id']}")
            print(f"  Link={row['link']}")
            print(f"  Status={row['status']}")
    else:
        print("Not found in COLLECT")
        
    conn.close()

if __name__ == "__main__":
    debug_link()
