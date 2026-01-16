import argparse
import sys
from pathlib import Path

# Allow importing craw/database.py when running from repo root
ROOT_DIR = Path(__file__).resolve().parent
CRAW_DIR = ROOT_DIR / "craw"
if str(CRAW_DIR) not in sys.path:
    sys.path.insert(0, str(CRAW_DIR))

from database import Database  # noqa: E402


DEFAULT_AREA_ID = 13098  # Quan 3 (HCM)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    parser = argparse.ArgumentParser()
    parser.add_argument("--area-id", type=int, default=DEFAULT_AREA_ID)
    args = parser.parse_args()

    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    conn = db.get_connection(use_database=True)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ward_id, name, name_url
            FROM location_detail
            WHERE level = 3 AND area_id = %s
            ORDER BY name
            """,
            (args.area_id,),
        )
        rows = cur.fetchall()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    if not rows:
        print(f"No wards found for area_id={args.area_id}.")
        return 1

    for row in rows:
        if isinstance(row, tuple):
            ward_id, name, name_url = row
        else:
            ward_id = row.get("ward_id")
            name = row.get("name")
            name_url = row.get("name_url")
        print(f"{ward_id}\t{name}\t{name_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
