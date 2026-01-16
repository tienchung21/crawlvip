import re
import unicodedata

from craw.database import Database


def strip_accents(text: str) -> str:
    if text is None:
        return ""
    text = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn")


def normalize_name(text: str) -> str:
    if not text:
        return ""
    # Remove parenthetical notes like "(xa ABC moi)"
    text = re.sub(r"\s*\([^)]*\)", " ", text)
    text = strip_accents(text).lower()
    text = text.replace(".", " ")
    text = re.sub(r"\s+", " ", text).strip()

    # Remove common administrative prefixes/suffixes after accent removal
    # Keep ASCII-only tokens to avoid Unicode in source.
    tokens = [
        "phuong",
        "xa",
        "thi tran",
        "thi xa",
        "thanh pho",
        "quan",
        "huyen",
        "kp",
        "tt",
        "p",
        "x",
        "moi",
        "cu",
    ]
    for token in tokens:
        text = re.sub(rf"\b{re.escape(token)}\b", " ", text)
    text = re.sub(r"[-_]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace(" ", "")


def ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nhadat_nhatot_ward (
          nt_ward_id BIGINT PRIMARY KEY,
          nt_ward_name VARCHAR(255) NULL,
          nt_ward_slug VARCHAR(255) NULL,
          region_id BIGINT NULL,
          area_id BIGINT NULL,
          cf_ward_id INT NULL,
          cf_ward_name VARCHAR(128) NULL,
          cf_ward_slug VARCHAR(255) NULL,
          match_type VARCHAR(20) NULL,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    )


def main():
    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    conn = db.get_connection()
    cur = conn.cursor()
    try:
        ensure_table(cur)
        cur.execute(
            """
            INSERT IGNORE INTO nhadat_nhatot_ward
              (nt_ward_id, nt_ward_name, nt_ward_slug, region_id, area_id)
            SELECT ward_id, name, name_url, region_id, area_id
            FROM location_detail
            WHERE level = 3 AND region_id = 13000 AND ward_id IS NOT NULL
            """
        )
        conn.commit()

        cur.execute(
            """
            SELECT nt_ward_id, nt_ward_name, nt_ward_slug
            FROM nhadat_nhatot_ward
            WHERE cf_ward_id IS NULL
            """
        )
        nt_rows = cur.fetchall()

        cur.execute(
            """
            SELECT city_id, city_title, city_title_no, city_realias
            FROM transaction_city
            WHERE city_parent_id IN (
              SELECT city_id FROM transaction_city WHERE city_parent_id = 63
            )
            """
        )
        cf_rows = cur.fetchall()

        cf_map = {}
        for row in cf_rows:
            if isinstance(row, tuple):
                city_id, title, title_no, realias = row
            else:
                city_id = row.get("city_id")
                title = row.get("city_title")
                title_no = row.get("city_title_no")
                realias = row.get("city_realias")
            base = title_no or title or realias
            key = normalize_name(base)
            if not key:
                continue
            cf_map.setdefault(key, []).append((city_id, title, realias))

        updated = 0
        ambiguous = 0
        for row in nt_rows:
            if isinstance(row, tuple):
                nt_id, nt_name, nt_slug = row
            else:
                nt_id = row.get("nt_ward_id")
                nt_name = row.get("nt_ward_name")
                nt_slug = row.get("nt_ward_slug")
            base = nt_name or nt_slug
            key = normalize_name(base)
            if not key:
                continue
            candidates = cf_map.get(key)
            if not candidates:
                continue
            if len(candidates) != 1:
                ambiguous += 1
                continue
            cf_id, cf_name, cf_slug = candidates[0]
            cur.execute(
                """
                UPDATE nhadat_nhatot_ward
                SET cf_ward_id=%s,
                    cf_ward_name=%s,
                    cf_ward_slug=%s,
                    match_type='name'
                WHERE nt_ward_id=%s AND cf_ward_id IS NULL
                """,
                (cf_id, cf_name, cf_slug, nt_id),
            )
            updated += cur.rowcount

        conn.commit()

        cur.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(cf_ward_id IS NOT NULL) AS matched,
              SUM(cf_ward_id IS NULL) AS missing
            FROM nhadat_nhatot_ward
            """
        )
        stats = cur.fetchone()
        print("Updated rows:", updated)
        print("Ambiguous names skipped:", ambiguous)
        if isinstance(stats, tuple):
            print("Total:", stats[0], "Matched:", stats[1], "Missing:", stats[2])
        else:
            print(
                "Total:",
                stats.get("total"),
                "Matched:",
                stats.get("matched"),
                "Missing:",
                stats.get("missing"),
            )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
