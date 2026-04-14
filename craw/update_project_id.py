#!/usr/bin/env python3
"""
Backfill data_full.project_id from Mogi project mapping:

data_full.project_name
  -> du_mogi.project_name (unique)
  -> du_mogi_duan_merge.duan_id (Cafeland duan.duan_id)

This script:
1) Adds column data_full.project_id if missing
2) Adds indexes to speed up join
3) Updates project_id in batches

Usage:
  source venv/bin/activate
  python3 craw/update_project_id.py --dry-run
  python3 craw/update_project_id.py
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database  # noqa: E402


def ensure_schema(db: Database) -> None:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        # Add project_id column
        cur.execute("SHOW COLUMNS FROM data_full LIKE 'project_id'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE data_full ADD COLUMN project_id INT NULL DEFAULT NULL")
            conn.commit()

        # Add index on project_id (for filtering/join later)
        cur.execute("SHOW INDEX FROM data_full WHERE Key_name='idx_data_full_project_id'")
        if not cur.fetchone():
            cur.execute("CREATE INDEX idx_data_full_project_id ON data_full (project_id)")
            conn.commit()

        # Add index on project_name (join key)
        cur.execute("SHOW INDEX FROM data_full WHERE Key_name='idx_data_full_project_name'")
        if not cur.fetchone():
            cur.execute("CREATE INDEX idx_data_full_project_name ON data_full (project_name)")
            conn.commit()

        # Ensure du_mogi has index (it has UNIQUE project_name already, but check doesn't hurt)
        cur.execute("SHOW INDEX FROM du_mogi WHERE Key_name='uq_du_mogi_project_name'")
        # If user changed schema, ignore.
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def count_pending(db: Database) -> int:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM data_full
            WHERE project_name IS NOT NULL AND project_name <> ''
              AND (project_id IS NULL OR project_id = 0)
            """
        )
        r = cur.fetchone()
        return int(r["c"] if isinstance(r, dict) else r[0])
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def backfill(db: Database, batch: int, dry_run: bool) -> int:
    """
    Returns total rows updated.
    """
    total = 0
    while True:
        conn = db.get_connection()
        cur = conn.cursor()
        try:
            if dry_run:
                cur.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM data_full df
                    JOIN du_mogi dm ON dm.project_name = df.project_name
                    JOIN du_mogi_duan_merge mm ON mm.du_mogi_id = dm.id
                    WHERE df.project_name IS NOT NULL AND df.project_name <> ''
                      AND (df.project_id IS NULL OR df.project_id = 0)
                    """
                )
                r = cur.fetchone()
                return int(r["c"] if isinstance(r, dict) else r[0])

            cur.execute(
                f"""
                UPDATE data_full df
                JOIN du_mogi dm ON dm.project_name = df.project_name
                JOIN du_mogi_duan_merge mm ON mm.du_mogi_id = dm.id
                SET df.project_id = mm.duan_id
                WHERE df.project_name IS NOT NULL AND df.project_name <> ''
                  AND (df.project_id IS NULL OR df.project_id = 0)
                LIMIT {int(batch)}
                """
            )
            updated = cur.rowcount
            conn.commit()
            total += updated
            if updated == 0:
                break
        finally:
            try:
                cur.close()
            finally:
                conn.close()

        time.sleep(0.02)
    return total


def get_max_data_full_id(db: Database) -> int:
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM data_full")
        r = cur.fetchone()
        return int(r["max_id"] if isinstance(r, dict) else r[0])
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def read_last_processed_id(state_file: str) -> int:
    p = Path(state_file)
    if not p.exists():
        return 0
    try:
        return int(p.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def write_last_processed_id(state_file: str, last_id: int) -> None:
    p = Path(state_file)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(str(int(last_id)), encoding="utf-8")


def backfill_new_only(db: Database, batch: int, dry_run: bool, state_file: str) -> tuple[int, int, int]:
    """
    Only process rows with id in (last_processed_id, snapshot_max_id].
    Returns: (rows_updated_or_will_update, last_processed_id_before, snapshot_max_id)
    """
    last_processed_id = read_last_processed_id(state_file)
    snapshot_max_id = get_max_data_full_id(db)

    if snapshot_max_id <= last_processed_id:
        return 0, last_processed_id, snapshot_max_id

    if dry_run:
        conn = db.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM data_full df
                JOIN du_mogi dm ON dm.project_name = df.project_name
                JOIN du_mogi_duan_merge mm ON mm.du_mogi_id = dm.id
                WHERE df.id > %s
                  AND df.id <= %s
                  AND df.project_name IS NOT NULL AND df.project_name <> ''
                  AND (df.project_id IS NULL OR df.project_id = 0)
                """,
                (last_processed_id, snapshot_max_id),
            )
            r = cur.fetchone()
            return int(r["c"] if isinstance(r, dict) else r[0]), last_processed_id, snapshot_max_id
        finally:
            try:
                cur.close()
            finally:
                conn.close()

    total = 0
    while True:
        conn = db.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE data_full df
                JOIN du_mogi dm ON dm.project_name = df.project_name
                JOIN du_mogi_duan_merge mm ON mm.du_mogi_id = dm.id
                SET df.project_id = mm.duan_id
                WHERE df.id > %s
                  AND df.id <= %s
                  AND df.project_name IS NOT NULL AND df.project_name <> ''
                  AND (df.project_id IS NULL OR df.project_id = 0)
                ORDER BY df.id
                LIMIT {int(batch)}
                """,
                (last_processed_id, snapshot_max_id),
            )
            updated = cur.rowcount
            conn.commit()
            total += updated
            if updated == 0:
                break
        finally:
            try:
                cur.close()
            finally:
                conn.close()
        time.sleep(0.02)

    # Move watermark to current snapshot so next run only scans newer rows.
    write_last_processed_id(state_file, snapshot_max_id)
    return total, last_processed_id, snapshot_max_id


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill data_full.project_id from du_mogi_duan_merge")
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--new-only", action="store_true", help="Only process newly inserted data_full rows based on id watermark")
    ap.add_argument("--state-file", default="/home/chungnt/crawlvip/tmp/project_id_last_id.state")
    args = ap.parse_args()

    db = Database()
    ensure_schema(db)

    pending_before = count_pending(db)
    if args.new_only:
        if args.dry_run:
            will_update, last_id, snap_id = backfill_new_only(
                db, batch=args.batch, dry_run=True, state_file=args.state_file
            )
            print("=== DRY RUN project_id NEW-ONLY ===")
            print(f"state_file: {args.state_file}")
            print(f"last_processed_id: {last_id}")
            print(f"snapshot_max_id: {snap_id}")
            print(f"Pending rows (global): {pending_before}")
            print(f"Rows that can be updated in new window: {will_update}")
            return 0

        t0 = time.time()
        updated, last_id, snap_id = backfill_new_only(
            db, batch=args.batch, dry_run=False, state_file=args.state_file
        )
        pending_after = count_pending(db)
        dur = time.time() - t0
        print("=== BACKFILL project_id NEW-ONLY DONE ===")
        print(f"state_file: {args.state_file}")
        print(f"last_processed_id(before): {last_id}")
        print(f"snapshot_max_id: {snap_id}")
        print(f"updated_in_new_window: {updated}")
        print(f"pending_before(global): {pending_before}")
        print(f"pending_after(global): {pending_after}")
        print(f"duration: {dur:.2f}s")
        return 0

    if args.dry_run:
        will_update = backfill(db, batch=args.batch, dry_run=True)
        print(f"Pending rows (project_name present, project_id empty): {pending_before}")
        print(f"Rows that can be updated now: {will_update}")
        return 0

    t0 = time.time()
    updated = backfill(db, batch=args.batch, dry_run=False)
    pending_after = count_pending(db)
    dur = time.time() - t0

    print("=== BACKFILL project_id DONE ===")
    print(f"updated: {updated}")
    print(f"pending_before: {pending_before}")
    print(f"pending_after: {pending_after}")
    print(f"duration: {dur:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
