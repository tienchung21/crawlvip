import re
import unicodedata

from craw.database import Database


PREFIXES = (
    "thanh pho ",
    "thi xa ",
    "thi tran ",
    "phuong ",
    "xa ",
)


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.replace("đ", "d").replace("Đ", "d")


def normalize_name(name: str) -> str:
    if not name:
        return ""
    base = name.split("(", 1)[0].strip()
    base = strip_accents(base).lower()
    for prefix in PREFIXES:
        if base.startswith(prefix):
            base = base[len(prefix):]
            break
    base = re.sub(r"[._,]", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    base = base.replace(" ", "-")
    base = re.sub(r"-{2,}", "-", base).strip("-")
    return base


def main():
    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM location_detail LIKE 'nhatot_slug'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE location_detail ADD COLUMN nhatot_slug VARCHAR(255) NULL")
            conn.commit()

        cur.execute("SELECT region_id, area_id, ward_id, name FROM location_detail")
        rows = cur.fetchall()

        updates = []
        for row in rows:
            if isinstance(row, tuple):
                region_id, area_id, ward_id, name = row
            else:
                region_id = row.get("region_id")
                area_id = row.get("area_id")
                ward_id = row.get("ward_id")
                name = row.get("name")
            slug = normalize_name(name or "")
            updates.append((slug, region_id, area_id or 0, ward_id or 0))

        sql = """
            UPDATE location_detail
            SET nhatot_slug = %s
            WHERE region_id = %s
              AND area_id_n = %s
              AND ward_id_n = %s
        """
        batch = 1000
        for i in range(0, len(updates), batch):
            cur.executemany(sql, updates[i:i + batch])
            conn.commit()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
