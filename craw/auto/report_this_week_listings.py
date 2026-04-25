import pymysql

try:
    conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db')
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        # 1. Thống kê scraped_details_flat
        print("======== BẢNG SCRAPED_DETAILS_FLAT ========")
        cur.execute("""
            SELECT domain, COUNT(*) as c
            FROM scraped_details_flat
            WHERE created_at >= '2026-04-12 00:00:00'
            GROUP BY domain
            ORDER BY c DESC
        """)
        flat_results = cur.fetchall()
        
        flat_total = 0
        for r in flat_results:
            domain = r['domain'] if r['domain'] else 'Unknown'
            count = r['c']
            flat_total += count
            print(f"Domain: {domain:20} - Số lượng: {count:,}")
            
        print(f"-> TỔNG (scraped_details_flat): {flat_total:,}\n")

        # 2. Thống kê ad_listing_detail (time_crawl tính bằng milliseconds)
        print("======== BẢNG AD_LISTING_DETAIL ========")
        cur.execute("""
            SELECT 'nhatot.com' as domain, COUNT(*) as c
            FROM ad_listing_detail
            WHERE time_crawl >= UNIX_TIMESTAMP('2026-04-12 00:00:00') * 1000
        """)
        ad_results = cur.fetchall()
        ad_total = 0
        for r in ad_results:
            domain = r['domain']
            count = r['c']
            ad_total += count
            print(f"Domain: {domain:20} - Số lượng: {count:,}")
            
        print(f"-> TỔNG (ad_listing_detail): {ad_total:,}\n")

        print("======== TỔNG CỘNG ========")
        print(f"Tổng số tin thu thập được trong tuần này (từ Chủ Nhật 12/04/2026): {flat_total + ad_total:,}")
finally:
    conn.close()
