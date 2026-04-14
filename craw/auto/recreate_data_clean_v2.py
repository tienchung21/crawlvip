import argparse
import pymysql
import time

# Config
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'


def run(domain: str, truncate_all: bool):
    print("=== RECREATING DATA_CLEAN_V1 (PHASE 1: IMPORT RAW) ===")
    print(f"Target domain: {domain}")
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4')
    cursor = conn.cursor()

    try:
        # Track conversion status on source table (ad_listing_detail) so we can convert incrementally.
        try:
            cursor.execute("ALTER TABLE ad_listing_detail ADD COLUMN cleanv1_converted TINYINT(1) NOT NULL DEFAULT 0")
            conn.commit()
            print("Added column ad_listing_detail.cleanv1_converted")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE ad_listing_detail ADD COLUMN cleanv1_converted_at DATETIME NULL")
            conn.commit()
            print("Added column ad_listing_detail.cleanv1_converted_at")
        except Exception:
            pass
        try:
            cursor.execute("CREATE INDEX idx_ad_cleanv1_converted ON ad_listing_detail(cleanv1_converted)")
            conn.commit()
            print("Created index idx_ad_cleanv1_converted")
        except Exception:
            pass

        try:
            cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN transfer_time BIGINT NULL")
            conn.commit()
            print("Added column transfer_time to data_clean_v1")
        except Exception:
            pass

        # Ensure index on domain for faster deletes on large tables (ignore if exists).
        try:
            cursor.execute("CREATE INDEX idx_domain ON data_clean_v1(domain)")
            conn.commit()
            print("Created index idx_domain on data_clean_v1(domain)")
        except Exception:
            pass

        # NOTE: User requirement: do NOT delete/truncate anything in data_clean_v1.
        # Keep the flag for backward compatibility but make it a no-op.
        if truncate_all:
            print("[WARN] --truncate-all is ignored (no delete/truncate will be executed).")
        print("Cleanup: skipped (no delete/truncate).")

        # 2. Insert Data
        # Mapping as per mapping_v1.md
        # data_clean_v1         <- ad_listing_detail
        # ad_id                 <- list_id
        # src_province_id       <- region_v2
        # src_district_id       <- area_v2
        # src_ward_id           <- ward
        # src_size              <- size
        # src_price             <- price_string
        # src_category_id       <- category
        # src_type              <- type
        # orig_list_time        <- orig_list_time
        # update_time           <- list_time
        # url                   <- 'https://www.nhatot.com/' + list_id + '.htm'

        if domain != "nhatot":
            raise ValueError("This script currently supports domain='nhatot' only.")

        print("Inserting raw data (ALL regions)...")
        transfer_ts = int(time.time())
        sql = """
        INSERT IGNORE INTO data_clean_v1 (
            ad_id, 
            src_province_id, src_district_id, src_ward_id, 
            src_size, src_price, 
            src_category_id, src_type,
            orig_list_time, update_time, 
            url, domain,
            transfer_time
        )
        SELECT 
            d.list_id, 
            d.region_v2, d.area_v2, d.ward, 
            d.size, d.price_string, 
            d.category, d.type,
            d.orig_list_time, d.list_time,
            CONCAT('https://www.nhatot.com/', d.list_id, '.htm'),
            %s,
            %s
        FROM ad_listing_detail d
        WHERE COALESCE(d.cleanv1_converted,0)=0
        """
        cursor.execute(sql, (domain, transfer_ts))
        rows = cursor.rowcount
        conn.commit()
        print(f"Inserted (IGNORE duplicates) {rows} rows.")

        # Mark as converted only if the row exists in data_clean_v1 after insert.
        print("Marking ad_listing_detail as converted (cleanv1_converted=1)...")
        cursor.execute(
            """
            UPDATE ad_listing_detail d
            JOIN data_clean_v1 c
              ON c.domain=%s
             AND c.ad_id = d.list_id
            SET d.cleanv1_converted=1,
                d.cleanv1_converted_at=COALESCE(d.cleanv1_converted_at, NOW())
            WHERE COALESCE(d.cleanv1_converted,0)=0
            """,
            (domain,),
        )
        conn.commit()
        print(f"Marked converted: {cursor.rowcount} rows.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Recreate/import raw data into data_clean_v1 (safe by domain; use --truncate-all to wipe all)."
    )
    parser.add_argument("--domain", default="nhatot", help="Domain to import (default: nhatot)")
    parser.add_argument(
        "--truncate-all",
        action="store_true",
        help="(IGNORED) Previously: TRUNCATE entire data_clean_v1. Now: no delete/truncate is executed.",
    )
    args = parser.parse_args()
    run(domain=args.domain, truncate_all=args.truncate_all)
