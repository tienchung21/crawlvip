#!/usr/bin/env bash
set -euo pipefail
cd /home/chungnt/crawlvip
python3 -u - <<'PY'
import pymysql, time
conn_params=dict(host='127.0.0.1',user='root',password='',database='craw_db',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor,autocommit=True)
start=time.time()
batch_size=2000
sleep_sec=0.1

def run_update(sql, label):
    batch=0
    total=0
    retries=0
    while True:
        try:
            conn=pymysql.connect(**conn_params)
            with conn.cursor() as cur:
                cur.execute(sql.format(limit=batch_size))
                rows=cur.rowcount
            conn.close()
            batch += 1
            total += rows
            retries = 0
            print(f'{label} batch={batch} rows={rows} total={total}')
            if rows == 0:
                break
            time.sleep(sleep_sec)
        except pymysql.err.OperationalError as e:
            code = e.args[0] if e.args else None
            if code in (1206, 1213):
                retries += 1
                wait = min(5, retries)
                print(f'{label} retry lock_error code={code} retries={retries} wait={wait}s batch_size={batch_size}')
                time.sleep(wait)
                continue
            raise

# clear data_median once more to be safe
conn=pymysql.connect(**conn_params)
with conn.cursor() as cur:
    cur.execute('DELETE FROM data_median')
    print('deleted_data_median', cur.rowcount)
conn.close()

# reset all flag1->0 in chunks
run_update(
    'UPDATE data_clean_v1 SET median_flag=0 WHERE median_flag=1 ORDER BY id LIMIT {limit}',
    'flag1_to_0'
)

# initialize any eligible nulls
run_update(
    'UPDATE data_clean_v1 SET median_flag=0 WHERE median_flag IS NULL AND std_month IS NOT NULL ORDER BY id LIMIT {limit}',
    'null_to_0'
)

conn=pymysql.connect(**conn_params)
with conn.cursor() as cur:
    cur.execute('SELECT COUNT(*) total FROM data_median')
    print('data_median_after', cur.fetchone()['total'])
    cur.execute("SELECT SUM(median_flag=1) flag1, SUM(median_flag=0) flag0, SUM(median_flag IS NULL) flagnull FROM data_clean_v1")
    print('flags_after', cur.fetchone())
conn.close()
print('elapsed_sec', round(time.time()-start,2))
PY
