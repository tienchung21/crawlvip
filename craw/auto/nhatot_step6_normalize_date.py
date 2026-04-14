import pymysql
import time
from datetime import datetime

BATCH_SIZE = 5000

def convert_timestamp_to_date(ts):
    if not ts:
        return None
    try:
        # Giả sử ts là miliseconds hoặc seconds.
        # Nhatot thường dùng miliseconds (13 digits) hoặc seconds (10 digits)
        # Check độ dài
        s_ts = str(ts)
        if len(s_ts) > 10:
            ts = float(ts) / 1000.0
        else:
            ts = float(ts)
            
        dt = datetime.fromtimestamp(ts)
        return dt.strftime('%Y-%m-%d')
    except:
        return None

def main():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='craw_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    script_name = "nhatot_step6_normalize_date.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN transfer_time BIGINT NULL")
        conn.commit()
        print("Added column transfer_time to data_clean_v1")
    except Exception:
        pass

    total_rows = 0
    count_updated = 0

    while True:
        sql_get = f"""
            SELECT id, orig_list_time, update_time, transfer_time
            FROM data_clean_v1
            WHERE domain = 'nhatot'
              AND process_status = 5
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_get)
        rows = cursor.fetchall()

        if not rows:
            break

        total_rows += len(rows)
        now_ts = int(time.time())

        for row in rows:
            record_id = row.get('id')
            ts = row.get('orig_list_time') or row.get('update_time')
            std_date = convert_timestamp_to_date(ts)

            sql_update = """
                UPDATE data_clean_v1
                SET std_date = %s,
                    transfer_time = COALESCE(transfer_time, %s)
                WHERE id = %s
            """
            cursor.execute(sql_update, (std_date, now_ts, record_id))
            if std_date:
                count_updated += 1

        conn.commit()
        print(f"  Batch: +{len(rows)} rows (Total: {total_rows})")

        ids = [str(r['id']) for r in rows]
        id_list = ",".join(ids)
        sql_final = f"""
            UPDATE data_clean_v1
            SET process_status = 6,
                last_script = '{script_name}',
                median_flag = COALESCE(median_flag, 0)
            WHERE domain = 'nhatot'
              AND id IN ({id_list})
        """
        cursor.execute(sql_final)
        conn.commit()

        if len(rows) < BATCH_SIZE:
            break

    print(f"-> Normalized Date for {count_updated}/{total_rows} rows.")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
