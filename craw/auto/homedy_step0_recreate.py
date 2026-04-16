import pymysql
import argparse
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    print("=== homedy_step0_recreate.py ===")
    start = time.time()

    # Get a batch of ids straight from raw
    cursor.execute("""
        SELECT id FROM scraped_details_flat 
        WHERE domain = 'homedy.com' 
          AND (cleanv1_converted = 0 OR cleanv1_converted IS NULL)
        LIMIT %s
    """, (args.limit,))
    
    rows = cursor.fetchall()
    if not rows:
        print("No raw homedy records left to migrate.")
        return

    ids = [str(r['id']) for r in rows]
    id_list_str = ', '.join(ids)

    sql = f"""
        INSERT IGNORE INTO data_clean_v1 (
            domain, url, ad_id, src_province_id, src_district_id, src_ward_id,
            src_size, src_price, src_type, src_category_id,
            update_time, orig_list_time, 
            project_id, process_status, last_script
        )
        SELECT 
            sc.domain, sc.url, sc.matin, sc.city_ext, sc.district_ext, sc.ward_ext, 
            sc.dientich, sc.khoanggia, sc.trade_type, sc.loaihinh,
            IF(sc.ngaydang IS NULL OR sc.ngaydang = '', UNIX_TIMESTAMP(sc.created_at), UNIX_TIMESTAMP(STR_TO_DATE(sc.ngaydang, '%%Y-%%m-%%dT%%H:%%i:%%s'))), 
            UNIX_TIMESTAMP(sc.created_at),
            dm.duan_id,
            0, 'homedy_step0_recreate.py'
        FROM scraped_details_flat sc
        LEFT JOIN duan_homedy_duan_merge dm ON sc.thuocduan COLLATE utf8mb4_general_ci = CAST(dm.homedy_project_id AS CHAR) COLLATE utf8mb4_general_ci
        WHERE sc.id IN ({id_list_str})
    """
    
    cursor.execute(sql)
    inserted = cursor.rowcount
    conn.commit()

    cursor.execute(f"UPDATE scraped_details_flat SET cleanv1_converted = 1 WHERE id IN ({id_list_str})")
    conn.commit()

    print(f"-> Migrated {inserted} homedy records into data_clean_v1 out of {len(rows)} raw fetched, in {time.time()-start:.2f}s.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
