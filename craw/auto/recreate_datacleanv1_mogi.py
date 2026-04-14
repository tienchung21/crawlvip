import pymysql
import time

# Config
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'

# Set to True if you want to clear data_clean_v1 before import
TRUNCATE_FIRST = False

# Batch + limit controls
BATCH_SIZE = 2000
LIMIT_TOTAL = 10000000

def run():
    print("=== RECREATE DATA_CLEAN_V1 FROM SCRAPED_DETAILS_FLAT (MOGI) ===")
    print("Mapping plan (detail flat -> data_clean_v1):")
    print("  ad_id            <- matin (fallback: id)")
    print("  src_province_id  <- mogi_city_id (fallback: city_ext)")
    print("  src_district_id  <- mogi_district_id (fallback: district_ext)")
    print("  src_ward_id      <- mogi_ward_id (fallback: ward_ext)")
    print("  src_size         <- dientich (fallback: dientichsudung)")
    print("  src_price        <- khoanggia")
    print("  src_category_id  <- loaihinh (fallback: loaibds)")
    print("  src_type         <- trade_type")
    print("  orig_list_time   <- UNIX_TIMESTAMP(ngaydang, dd/mm/YYYY) if parse ok")
    print("  update_time      <- NULL")
    print("  cf_street_id     <- mogi_street_id")
    print("  url              <- url")
    print("  domain           <- domain")

    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    try:
        if TRUNCATE_FIRST:
            print("Truncating data_clean_v1...")
            cursor.execute("TRUNCATE TABLE data_clean_v1")
            conn.commit()

        print("Importing Mogi detail flat -> data_clean_v1...")
        total_inserted = 0
        last_id = 0

        while True:
            limit_left = BATCH_SIZE
            if LIMIT_TOTAL is not None:
                limit_left = min(BATCH_SIZE, max(LIMIT_TOTAL - total_inserted, 0))
                if limit_left <= 0:
                    break

            cursor.execute(
                """
                SELECT id
                FROM scraped_details_flat
                WHERE domain = 'mogi'
                  AND (flag IS NULL OR flag = 0)
                  AND id > %s
                ORDER BY id
                LIMIT %s
                """,
                (last_id, limit_left),
            )
            id_rows = cursor.fetchall()
            if not id_rows:
                break

            ids = [r[0] for r in id_rows]
            last_id = ids[-1]
            placeholders = ",".join(["%s"] * len(ids))

            sql_insert = f"""
            INSERT IGNORE INTO data_clean_v1 (
                ad_id,
                src_province_id, src_district_id, src_ward_id,
                src_size, src_price,
                src_category_id, src_type,
                orig_list_time, update_time,
                cf_street_id,
                url, domain
            )
            SELECT
                COALESCE(matin, CAST(id AS CHAR)),
                COALESCE(CAST(mogi_city_id AS CHAR), city_ext),
                COALESCE(CAST(mogi_district_id AS CHAR), district_ext),
                COALESCE(CAST(mogi_ward_id AS CHAR), ward_ext),
                COALESCE(dientich, dientichsudung),
                khoanggia,
                COALESCE(loaihinh, loaibds),
                trade_type,
                UNIX_TIMESTAMP(STR_TO_DATE(ngaydang, '%%d/%%m/%%Y')),
                NULL,
                mogi_street_id,
                url,
                domain
            FROM scraped_details_flat
            WHERE id IN ({placeholders})
            """
            cursor.execute(sql_insert, ids)
            inserted = cursor.rowcount
            total_inserted += inserted

            sql_flag = f"""
            UPDATE scraped_details_flat
            SET flag = 1
            WHERE id IN ({placeholders})
            """
            cursor.execute(sql_flag, ids)
            conn.commit()

            print(f"  Batch: ids={len(ids)}, inserted={inserted}, total={total_inserted}")

        print(f"Inserted total {total_inserted} rows.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run()
