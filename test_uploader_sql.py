import pymysql
from datetime import date, timedelta

conn = pymysql.connect(host="127.0.0.1", user="root", password="", database="craw_db", cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()

today = date.today() # e.g. 2026-04-12
sunday = target_date = today - timedelta(days=(today.weekday() + 1) % 7)
print("Sunday:", sunday)

# Let's count data_full_uploaded from data_full
cur.execute("""
    SELECT d.cf_province_id,
           COUNT(DISTINCT df.id) as data_full_uploaded,
           COUNT(DISTINCT dnf.id) as data_no_full_uploaded
    FROM data_clean_v1 d
    -- JOIN scraped_details_flat exactly like in report script
    INNER JOIN scraped_details_flat s 
       ON d.url = s.url AND d.domain = s.domain AND s.created_at >= %s
    LEFT JOIN data_full df 
       ON df.source = d.domain 
      AND df.source_post_id = SUBSTRING_INDEX(d.ad_id, '_', -1)
      AND df.images_status = 'LISTING_UPLOADED'
    LEFT JOIN data_no_full dnf
       ON dnf.source = d.domain 
      AND dnf.source_post_id = SUBSTRING_INDEX(d.ad_id, '_', -1)
      AND dnf.images_status = 'LISTING_UPLOADED'
    WHERE d.cf_province_id IN (1, 63, 43)
    GROUP BY d.cf_province_id
""", (sunday,))
for r in cur.fetchall(): print("SDF:", r)

# For nhatot
cur.execute("""
    SELECT d.cf_province_id,
           COUNT(DISTINCT df.id) as data_full_uploaded,
           COUNT(DISTINCT dnf.id) as data_no_full_uploaded
    FROM data_clean_v1 d
    INNER JOIN ad_listing_detail a
       ON d.ad_id = a.list_id OR d.ad_id = CONCAT('nhatot_', a.list_id) 
      AND a.list_time REGEXP '^[0-9]+$' 
      AND CAST(a.list_time as UNSIGNED) >= %s
    LEFT JOIN data_full df 
       ON df.source = 'nhatot' AND df.source_post_id = a.list_id
      AND df.images_status = 'LISTING_UPLOADED'
    LEFT JOIN data_no_full dnf
       ON dnf.source = 'nhatot' AND dnf.source_post_id = a.list_id
      AND dnf.images_status = 'LISTING_UPLOADED'
    WHERE d.domain = 'nhatot'
      AND d.cf_province_id IN (1, 63, 43)
    GROUP BY d.cf_province_id
""", (int(sunday.strftime("%s")) * 1000,))
for r in cur.fetchall(): print("Nhatot:", r)

