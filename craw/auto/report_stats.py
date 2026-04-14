import csv
from datetime import date, timedelta
from pathlib import Path
import pymysql
import pymysql.cursors

def main():
    order_file = Path('/home/chungnt/crawlvip/uploaded_listing_province_2026-03-15_to_now.tsv')
    out_file = Path('/home/chungnt/crawlvip/uploaded_stats_since_sunday.tsv')

    # Get Sunday date
    today = date(2026, 4, 12)  # the current date in context
    sunday = today - timedelta(days=(today.weekday() + 1) % 7)
    start_dt = f"{sunday.isoformat()} 00:00:00"
    start_ms = int(sunday.strftime("%s")) * 1000

    provinces = []
    with order_file.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for r in reader:
            provinces.append((int(r['province_id']), r['province_name']))

    conn = pymysql.connect(
        host='127.0.0.1', user='root', password='', database='craw_db',
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

    results = []

    try:
        with conn.cursor() as cur:
            for pid, name in provinces:
                
                # count from scraped_details_flat domains
                df_count = 0
                dnf_count = 0
                
                domains_sdf = ['batdongsan.com.vn', 'mogi', 'alonhadat.com.vn', 'guland.vn', 'nhadat']
                
                for domain in domains_sdf:
                    if domain == 'nhadat':
                        # nhadat links to nhadat_data
                        cur.execute("""
                            SELECT 
                                COUNT(DISTINCT df.id) as df_c,
                                COUNT(DISTINCT dnf.id) as dnf_c
                            FROM data_clean_v1 d
                            JOIN nhadat_data n ON d.ad_id = CONCAT('nhadat_', n.realestate_id)
                            LEFT JOIN data_full df ON df.source = 'nhadat' AND df.source_post_id = n.realestate_id AND df.images_status = 'LISTING_UPLOADED'
                            LEFT JOIN data_no_full dnf ON dnf.source = 'nhadat' AND dnf.source_post_id = n.realestate_id AND dnf.images_status = 'LISTING_UPLOADED'
                            WHERE d.cf_province_id = %s
                              AND d.domain = 'nhadat'
                              AND STR_TO_DATE(n.orig_list_time, '%%d/%%m/%%Y') >= %s
                        """, (pid, sunday.isoformat()))
                    else:
                        cur.execute("""
                            SELECT 
                                COUNT(DISTINCT df.id) as df_c,
                                COUNT(DISTINCT dnf.id) as dnf_c
                            FROM data_clean_v1 d
                            JOIN scraped_details_flat s FORCE INDEX (idx_sdf_domain) ON s.url = d.url AND s.domain = %s
                            LEFT JOIN data_full df ON df.source = %s AND df.source_post_id = SUBSTRING_INDEX(d.ad_id, '_', -1) AND df.images_status = 'LISTING_UPLOADED'
                            LEFT JOIN data_no_full dnf ON dnf.source = %s AND dnf.source_post_id = SUBSTRING_INDEX(d.ad_id, '_', -1) AND dnf.images_status = 'LISTING_UPLOADED'
                            WHERE d.cf_province_id = %s
                              AND d.domain = %s
                              AND s.created_at >= %s
                        """, (domain, domain, domain, pid, domain, start_dt))
                        
                    res = cur.fetchone()
                    if res:
                        df_count += (res['df_c'] or 0)
                        dnf_count += (res['dnf_c'] or 0)

                # Nhatot
                cur.execute("""
                    SELECT 
                        COUNT(DISTINCT df.id) as df_c,
                        COUNT(DISTINCT dnf.id) as dnf_c
                    FROM data_clean_v1 d
                    JOIN ad_listing_detail a ON (d.ad_id = a.list_id OR d.ad_id = CONCAT('nhatot_', a.list_id))
                    LEFT JOIN data_full df ON df.source = 'nhatot' AND df.source_post_id = a.list_id AND df.images_status = 'LISTING_UPLOADED'
                    LEFT JOIN data_no_full dnf ON dnf.source = 'nhatot' AND dnf.source_post_id = a.list_id AND dnf.images_status = 'LISTING_UPLOADED'
                    WHERE d.domain = 'nhatot'
                      AND d.cf_province_id = %s
                      AND a.list_time REGEXP '^[0-9]+$'
                      AND CAST(a.list_time as UNSIGNED) >= %s
                """, (pid, start_ms))
                res = cur.fetchone()
                if res:
                    df_count += (res['df_c'] or 0)
                    dnf_count += (res['dnf_c'] or 0)

                total = df_count + dnf_count
                results.append((pid, name, df_count, dnf_count, total))
                print(f"{pid}\t{name}\t{df_count}\t{dnf_count}\t{total}", flush=True)

    finally:
        conn.close()

    # Write output
    with out_file.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(['province_id', 'province_name', 'data_full_uploaded', 'data_no_full_uploaded', 'total_uploaded'])
        for r in results:
            writer.writerow(r)
            
    print(f"\nDone! File written to {out_file}")

if __name__ == '__main__':
    main()
