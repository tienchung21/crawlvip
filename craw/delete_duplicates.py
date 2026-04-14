#!/usr/bin/env python3
"""Delete duplicate images in batches"""
import pymysql
import time

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db')
cursor = conn.cursor()

total_deleted = 0
batch = 0

print("Starting duplicate deletion...")

while True:
    batch += 1
    cursor.execute("""
        SELECT sdi1.id
        FROM scraped_detail_images sdi1
        INNER JOIN scraped_detail_images sdi2 
            ON sdi1.detail_id = sdi2.detail_id 
            AND sdi1.image_url = sdi2.image_url 
            AND sdi1.id > sdi2.id
        LIMIT 5000
    """)
    ids = [row[0] for row in cursor.fetchall()]
    
    if not ids:
        print(f"DONE! Total deleted: {total_deleted}")
        break
    
    cursor.execute(f"DELETE FROM scraped_detail_images WHERE id IN ({','.join(map(str, ids))})")
    conn.commit()
    total_deleted += cursor.rowcount
    print(f"Batch {batch}: deleted {cursor.rowcount}, total: {total_deleted}")
    time.sleep(0.5)

# Add unique index after cleanup
print("Adding unique index...")
try:
    cursor.execute("ALTER TABLE scraped_detail_images ADD UNIQUE INDEX idx_unique_image (detail_id, image_url(255))")
    conn.commit()
    print("Unique index added!")
except Exception as e:
    print(f"Index error (may already exist): {e}")

cursor.close()
conn.close()
