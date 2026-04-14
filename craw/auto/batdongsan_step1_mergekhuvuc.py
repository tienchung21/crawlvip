
import argparse
import time

import pymysql


DEFAULT_BATCH_SIZE = 5000
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "craw_db"
DEFAULT_DOMAIN = "batdongsan.com.vn"
SCRIPT_NAME = "batdongsan_step1_mergekhuvuc.py"


def connect():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def count_pending(cursor, domain):
    cursor.execute(
        """
        SELECT COUNT(*) AS c
        FROM data_clean_v1
        WHERE domain=%s AND process_status=0
        """,
        (domain,),
    )
    return int((cursor.fetchone() or {}).get("c") or 0)


def count_ready_to_finalize(cursor, domain):
    cursor.execute(
        """
        SELECT COUNT(*) AS c
        FROM data_clean_v1
        WHERE domain=%s
          AND process_status=0
          AND cf_ward_id IS NOT NULL AND cf_ward_id > 0
        """,
        (domain,),
    )
    return int((cursor.fetchone() or {}).get("c") or 0)


def run_step1(domain, batch_size, max_batches=0):
    conn = connect()
    cursor = conn.cursor()

    started_at = time.time()
    print(f"=== START {SCRIPT_NAME} | domain={domain} | batch_size={batch_size} ===")

    pending_before = count_pending(cursor, domain)
    ready_before = count_ready_to_finalize(cursor, domain)
    print(f"[Init] pending_status0={pending_before:,} | ready_finalize={ready_before:,}")

    # 1) Map ward + province IDs from location_batdongsan
    print("[Step1] Mapping cf_ward_id + cf_province_id from location_batdongsan...")
    total_mapped = 0
    batch_no = 0
    while True:
        batch_no += 1
        sql_map = f"""
        UPDATE data_clean_v1 d
        JOIN location_batdongsan l
          ON CAST(d.src_ward_id AS UNSIGNED) = l.ward_id
        SET
          d.cf_ward_id = l.cafeland_ward_id_new,
          d.cf_province_id = l.cafeland_province_id_new
        WHERE d.domain = %s
          AND d.process_status = 0
          AND (d.cf_ward_id IS NULL OR d.cf_ward_id = 0)
          AND l.cafeland_ward_id_new IS NOT NULL
          AND l.cafeland_ward_id_new > 0
        LIMIT {int(batch_size)}
        """
        cursor.execute(sql_map, (domain,))
        affected = cursor.rowcount
        conn.commit()
        total_mapped += affected
        print(f"[Step1][Batch {batch_no}] mapped={affected:,} | total_mapped={total_mapped:,}")
        if max_batches > 0 and batch_no >= max_batches:
            print(f"[Step1] Reached max_batches={max_batches}, stopping map phase.")
            break
        if affected < batch_size:
            break

    # 2) Finalize step 1 only for rows with valid ward mapping
    print("[Step1] Finalizing process_status=1 for rows with cf_ward_id > 0...")
    total_finalized = 0
    batch_no = 0
    while True:
        batch_no += 1
        sql_finalize = f"""
        UPDATE data_clean_v1
        SET process_status = 1,
            last_script = %s
        WHERE domain = %s
          AND process_status = 0
          AND cf_ward_id IS NOT NULL
          AND cf_ward_id > 0
        LIMIT {int(batch_size)}
        """
        cursor.execute(sql_finalize, (SCRIPT_NAME, domain))
        affected = cursor.rowcount
        conn.commit()
        total_finalized += affected
        print(f"[Finalize][Batch {batch_no}] finalized={affected:,} | total_finalized={total_finalized:,}")
        if max_batches > 0 and batch_no >= max_batches:
            print(f"[Finalize] Reached max_batches={max_batches}, stopping finalize phase.")
            break
        if affected < batch_size:
            break

    pending_after = count_pending(cursor, domain)
    ready_after = count_ready_to_finalize(cursor, domain)
    duration = time.time() - started_at

    print("=== DONE STEP 1 ===")
    print(f"mapped_total={total_mapped:,}")
    print(f"finalized_total={total_finalized:,}")
    print(f"pending_before={pending_before:,} | pending_after={pending_after:,}")
    print(f"ready_before={ready_before:,} | ready_after={ready_after:,}")
    print(f"duration={duration:.2f}s")

    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Step 1 map khu vuc for batdongsan domain")
    parser.add_argument("--domain", default=DEFAULT_DOMAIN, help="Domain in data_clean_v1")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Update limit per batch")
    parser.add_argument("--max-batches", type=int, default=0, help="Safety cap for test (0=run to completion)")
    args = parser.parse_args()
    run_step1(domain=args.domain, batch_size=args.batch_size, max_batches=args.max_batches)


if __name__ == "__main__":
    main()
