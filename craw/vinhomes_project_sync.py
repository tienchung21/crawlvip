#!/usr/bin/env python3
"""
Sync danh sach du an tu API Vinhomes vao bang vinhome_projects.
"""

import os
import sys

from curl_cffi import requests

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    from database import Database


API_URL = (
    "https://apigw.vinhomes.vn/leasing/v1/category/get-list-project"
    "?IsPaged=false&OrderBy=InsertedAt&OrderByDirection=DESC&IsCounting=true&locale=vi"
)


def ensure_table(db: Database) -> None:
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS vinhome_projects (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    id_project VARCHAR(64) NOT NULL,
                    name VARCHAR(255) NULL,
                    UNIQUE KEY uq_vinhome_projects_id_project (id_project)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            try:
                cur.execute("ALTER TABLE vinhome_projects MODIFY COLUMN id_project VARCHAR(64) NOT NULL")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE vinhome_projects ADD UNIQUE KEY uq_vinhome_projects_id_project (id_project)")
            except Exception:
                pass
        conn.commit()
    finally:
        conn.close()


def fetch_items():
    resp = requests.get(API_URL, impersonate="chrome124", timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("items", [])


def upsert_items(db: Database, items) -> int:
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO vinhome_projects (id_project, name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name)
                """,
                [(str(item.get("id") or "").strip(), item.get("name")) for item in items if item.get("id")],
            )
        conn.commit()
        return len(items)
    finally:
        conn.close()


def main() -> int:
    db = Database()
    ensure_table(db)
    items = fetch_items()
    count = upsert_items(db, items)
    print(f"Synced {count} items into vinhome_projects")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
