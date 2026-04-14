#!/usr/bin/env python3
import argparse
import csv
import os
import subprocess
import tempfile
from pathlib import Path

import pymysql

MYSQL = dict(host='127.0.0.1', port=3306, user='root', password='', database='craw_db', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
PGHOST='118.69.81.54'
PGPORT='35432'
PGUSER='reportuser'
PGDATABASE='report'
PGPASSWORD='klcjalksjc1n1c1k1cckn1n'
SCHEMA='public'
TABLE='location_neightbor'
STAGE=f'{TABLE}_stage'

COLUMNS = [
    'id','source_osm_id','source_admin_level','source_unit_type','source_name_vi','source_name_en',
    'source_parent_osm_id','source_parent_name','neighbor_osm_id','neighbor_name_vi','neighbor_parent_name',
    'shared_border_m','source_timestamp','source_cafeland_id','source_parent_cafeland_id',
    'neighbor_cafeland_id','neighbor_parent_cafeland_id','created_at'
]

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
  id BIGINT PRIMARY KEY,
  source_osm_id BIGINT NOT NULL,
  source_admin_level INTEGER NULL,
  source_unit_type VARCHAR(50) NULL,
  source_name_vi VARCHAR(255) NULL,
  source_name_en VARCHAR(255) NULL,
  source_parent_osm_id BIGINT NULL,
  source_parent_name VARCHAR(255) NULL,
  neighbor_osm_id BIGINT NOT NULL,
  neighbor_name_vi VARCHAR(255) NULL,
  neighbor_parent_name VARCHAR(255) NULL,
  shared_border_m NUMERIC(18,2) NULL,
  source_timestamp TIMESTAMP NULL,
  source_cafeland_id INTEGER NULL,
  source_parent_cafeland_id INTEGER NULL,
  neighbor_cafeland_id INTEGER NULL,
  neighbor_parent_cafeland_id INTEGER NULL,
  created_at TIMESTAMP NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_location_neightbor_source_neighbor ON {SCHEMA}.{TABLE}(source_osm_id, neighbor_osm_id);
CREATE INDEX IF NOT EXISTS idx_location_neightbor_source_osm ON {SCHEMA}.{TABLE}(source_osm_id);
CREATE INDEX IF NOT EXISTS idx_location_neightbor_neighbor_osm ON {SCHEMA}.{TABLE}(neighbor_osm_id);
CREATE INDEX IF NOT EXISTS idx_location_neightbor_source_type ON {SCHEMA}.{TABLE}(source_unit_type);
CREATE INDEX IF NOT EXISTS idx_location_neightbor_source_cafeland ON {SCHEMA}.{TABLE}(source_cafeland_id);
CREATE INDEX IF NOT EXISTS idx_location_neightbor_neighbor_cafeland ON {SCHEMA}.{TABLE}(neighbor_cafeland_id);
"""


def run_psql(sql: str):
    env = dict(os.environ)
    env['PGPASSWORD'] = PGPASSWORD
    subprocess.run(['psql','-h',PGHOST,'-p',PGPORT,'-U',PGUSER,'-d',PGDATABASE,'-v','ON_ERROR_STOP=1','-c',sql], check=True, env=env)


def export_mysql(csv_path: Path):
    conn = pymysql.connect(**MYSQL)
    try:
        with conn.cursor() as cur, open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            cur.execute(f"SELECT {', '.join(COLUMNS)} FROM {TABLE} ORDER BY id")
            count = 0
            for row in cur:
                writer.writerow([row[c] for c in COLUMNS])
                count += 1
        return count
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--truncate', action='store_true')
    args = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / f'{TABLE}.csv'
        exported = export_mysql(csv_path)

        run_psql(DDL)
        if args.truncate:
            run_psql(f'TRUNCATE TABLE {SCHEMA}.{TABLE};')
        run_psql(f'DROP TABLE IF EXISTS {SCHEMA}.{STAGE}; CREATE TABLE {SCHEMA}.{STAGE} AS SELECT {", ".join(COLUMNS)} FROM {SCHEMA}.{TABLE} WHERE 1=0;')

        env = dict(os.environ)
        env['PGPASSWORD'] = PGPASSWORD
        copy_sql = f"\\copy {SCHEMA}.{STAGE} ({', '.join(COLUMNS)}) FROM '{csv_path}' WITH (FORMAT csv)"
        subprocess.run(['psql','-h',PGHOST,'-p',PGPORT,'-U',PGUSER,'-d',PGDATABASE,'-v','ON_ERROR_STOP=1','-c',copy_sql], check=True, env=env)

        update_assign = ', '.join([f'{c}=EXCLUDED.{c}' for c in COLUMNS if c != 'id'])
        run_psql(f"INSERT INTO {SCHEMA}.{TABLE} ({', '.join(COLUMNS)}) SELECT {', '.join(COLUMNS)} FROM {SCHEMA}.{STAGE} ON CONFLICT (id) DO UPDATE SET {update_assign}; DROP TABLE IF EXISTS {SCHEMA}.{STAGE};")

        out = subprocess.check_output(['psql','-h',PGHOST,'-p',PGPORT,'-U',PGUSER,'-d',PGDATABASE,'-At','-c',f'SELECT COUNT(*) FROM {SCHEMA}.{TABLE};'], env=env, text=True).strip()
        print(f'exported_rows={exported}')
        print(f'pg_rows={out}')

if __name__ == '__main__':
    main()
