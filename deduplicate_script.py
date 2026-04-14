import pymysql
import time

def deduplicate():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='',
        database='craw_db',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        print("Đang quét các link bị trùng lặp...")
        # Step 1: Find all link_ids that have duplicates
        # We only need link_id. We will resolve Keep_ID per item or in bulk?
        # Bulk is better.
        # "SELECT link_id, MAX(id) as keep_id FROM scraped_details_flat GROUP BY link_id HAVING COUNT(*) > 1"
        
        with conn.cursor() as cursor:
            # Get list of duplicates
            cursor.execute("""
                SELECT link_id, MAX(id) as keep_id 
                FROM scraped_details_flat 
                GROUP BY link_id 
                HAVING COUNT(*) > 1
            """)
            dupes = cursor.fetchall()
            
            total_dupes = len(dupes)
            print(f"Tìm thấy {total_dupes} nhóm link bị trùng.")
            
            if total_dupes == 0:
                print("Không có trùng lặp nào.")
                return

            # Step 2: Delete in batches
            batch_size = 1000
            deleted_total = 0
            
            for i in range(0, total_dupes, batch_size):
                batch_dupes = dupes[i : i + batch_size]
                
                # For each dupe group, we want to delete all rows with that link_id except keep_id
                # DELETE FROM table WHERE link_id = X AND id != Y
                
                # To make it efficient, we can use IN clause?
                # No, because keep_id is different for each link_id.
                # So we have to loop? Or construct a complex DELETE?
                # Loop is safest for avoiding locks. 
                # Or: DELETE FROM scraped_details_flat WHERE link_id IN (list_of_ids) AND id NOT IN (list_of_keep_ids)
                # -> This assumes strict mapping, which is unsafe if link_ids are reused (unlikely).
                
                # Let's try to delete one by one in a transaction block? No, too slow for 300k.
                # Construct a BIG delete statement?
                # DELETE FROM scraped_details_flat WHERE (link_id = X AND id != X_id) OR (link_id = Y AND id != Y_id) ...
                
                # Actually, "DELETE FROM scraped_details_flat WHERE link_id = %s AND id < %s" 
                # (Assuming we keep MAX(id), preventing newer inserts from being deleted? 
                # If crawler inserts NEW row (Status > keep_id), we shouldn't delete it?
                # Wait, if crawler inserts NEW row, it becomes the NEW Max ID.
                # My 'keep_id' is a snapshot from the start.
                # So if I delete "id != keep_id", I might delete a brand new row that appeared 1ms ago!
                # DANGER!
                # So I MUST delete "id < keep_id". 
                # Because keep_id was the MAX at scan time. Anything smaller is definitely older and duplicate.
                # Anything larger is new (and maybe duplicate, but safe to keep for now).
                
                params = []
                # We can't batch "link_id=%s AND id < %s" easily in one query without multiple ORs.
                # MySQL optimization for ORs is okay on primary keys/indexes.
                
                sql_parts = []
                for item in batch_dupes:
                    lid = item['link_id']
                    kid = item['keep_id']
                    if lid and kid:
                        sql_parts.append(f"(link_id = {lid} AND id < {kid})")
                
                if not sql_parts:
                    continue
                    
                full_sql = "DELETE FROM scraped_details_flat WHERE " + " OR ".join(sql_parts)
                
                # Run delete
                cursor.execute(full_sql)
                deleted_count = cursor.rowcount
                conn.commit()
                
                deleted_total += deleted_count
                print(f"Processing {i}/{total_dupes} groups. Deleted {deleted_count} rows...")
                
                # Sleep a tiny bit to be nice to crawler
                time.sleep(0.1)

            print(f"Hoàn thành! Tổng cộng đã xóa {deleted_total} dòng trùng lặp.")

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    deduplicate()
