import pymysql

conn = pymysql.connect(host='127.0.0.1', user='root', password='', database='craw_db')
cur = conn.cursor(pymysql.cursors.DictCursor)

def get_columns(table):
    cur.execute(f"DESCRIBE {table}")
    return [row['Field'] for row in cur.fetchall()]

print("data_full columns:", get_columns('data_full'))
print("data_no_full columns:", get_columns('data_no_full'))

