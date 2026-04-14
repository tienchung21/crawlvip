import pymysql
import time

def get_conn():
    return pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db',
                           charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

conn = get_conn()
BATCH_SIZE = 10000

try:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(id) as max_id FROM data_clean_v1")
        max_id = cur.fetchone()['max_id']
except:
    max_id = 4200000

last_id = max_id

while last_id > 0:
    try:
        with conn.cursor() as cur:
            start_id = last_id - BATCH_SIZE
            cur.execute("""
                SELECT id, domain, ad_id, url 
                FROM data_clean_v1 
                WHERE id > %s AND id <= %s AND time_crawl IS NULL
            """, (start_id, last_id))
            rows = cur.fetchall()
            
            if not rows:
                last_id -= BATCH_SIZE
                continue
            
            updates = []
            nhatot_ids = []
            other_urls = []
            
            for r in rows:
                if r['domain'] == 'nhatot':
                    list_id_str = r['ad_id'].replace('nhatot_', '')
                    nhatot_ids.append(list_id_str)
                else:
                    other_urls.append(r['url'])
                    
            nhatot_map = {}
            if nhatot_ids:
                format_strings = ','.join(['%s'] * len(nhatot_ids))
                cur.execute(f"SELECT list_id, cleanv1_converted_at FROM ad_listing_detail WHERE list_id IN ({format_strings})", tuple(nhatot_ids))
                for nr in cur.fetchall():
                    nhatot_map[str(nr['list_id'])] = nr['cleanv1_converted_at']
                    
            other_map = {}
            if other_urls:
                format_strings = ','.join(['%s'] * len(other_urls))
                cur.execute(f"SELECT url, created_at FROM scraped_details_flat WHERE url IN ({format_strings})", tuple(other_urls))
                for orw in cur.fetchall():
                    other_map[orw['url']] = orw['created_at']
                    
            for r in rows:
                new_time = None
                if r['domain'] == 'nhatot':
                    lid = r['ad_id'].replace('nhatot_', '')
                    new_time = nhatot_map.get(lid)
                else:
                    new_time = other_map.get(r['url'])
                    
                if new_time:
                    updates.append((new_time, r['id']))
            
            if updates:
                cur.executemany("UPDATE data_clean_v1 SET time_crawl = %s WHERE id = %s", updates)
                conn.commit()
                print(f"Updated {len(updates)} rows in block {start_id} -> {last_id}")
            else:
                pass
                
        last_id -= BATCH_SIZE
        time.sleep(0.01)
    except Exception as e:
        print(f"Error {e}, reconnecting...")
        time.sleep(1)
        try:
            conn.close()
        except:
            pass
        conn = get_conn()

conn.close()
