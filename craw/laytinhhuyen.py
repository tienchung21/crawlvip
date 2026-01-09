import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from database import Database


def ensure_location_detail_table(db: Database, table: str = "location_detail") -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
      region_id    BIGINT NOT NULL,
      area_id      BIGINT NULL,
      ward_id      BIGINT NULL,
      level        TINYINT NOT NULL,
      name         VARCHAR(255) NOT NULL,
      name_url     VARCHAR(255) NULL,
      unit_type    VARCHAR(50) NULL,
      source       VARCHAR(50) NOT NULL DEFAULT 'loadRegionsV2',
      raw_payload  JSON NULL,
      is_active    TINYINT(1) NOT NULL DEFAULT 1,
      created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      area_id_n    BIGINT AS (IFNULL(area_id, 0)) STORED,
      ward_id_n    BIGINT AS (IFNULL(ward_id, 0)) STORED,
      PRIMARY KEY (region_id, area_id_n, ward_id_n),
      INDEX idx_level (level),
      INDEX idx_region_area (region_id, area_id_n),
      INDEX idx_name_url (name_url)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    conn = db.get_connection(use_database=True)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _load_json(json_path: str) -> Dict[str, Any]:
    p = Path(json_path)
    if not p.exists():
        raise FileNotFoundError(f"JSON not found: {json_path}")
    return json.loads(p.read_text(encoding="utf-8"))


def _build_slug_maps(data: Dict[str, Any]) -> Tuple[Dict[int, str], Dict[Tuple[int, int], str]]:
    region_slug_by_id: Dict[int, str] = {}
    district_slug_by_key: Dict[Tuple[int, int], str] = {}

    rfurl = data.get("regionFollowUrl") or {}
    regions = (rfurl.get("entities") or {}).get("regions") or {}

    for region_slug, region_obj in regions.items():
        rid = region_obj.get("id")
        if rid is None:
            continue
        rid_int = int(rid)
        region_slug_by_id[rid_int] = region_slug

        areas = region_obj.get("area") or {}
        for district_slug, district_obj in areas.items():
            did = district_obj.get("id")
            if did is None:
                continue
            district_slug_by_key[(rid_int, int(did))] = district_slug

    return region_slug_by_id, district_slug_by_key


def import_regions_from_json_to_location_detail(
    db: Database,
    json_path: str,
    table: str = "location_detail",
    source: str = "loadRegionsV2",
    batch_size: int = 500,
) -> int:
    data = _load_json(json_path)
    region_slug_by_id, district_slug_by_key = _build_slug_maps(data)

    rows: List[Tuple[Any, Any, Any, int, str, Optional[str], Optional[str], str, str]] = []
    regions = (
        data.get("regionFollowId", {})
            .get("entities", {})
            .get("regions", {})
    )

    for rid_str, region_obj in regions.items():
        try:
            rid = int(rid_str)
        except Exception:
            continue

        region_name = region_obj.get("name") or ""
        region_slug = region_obj.get("name_url") or region_slug_by_id.get(rid)
        rows.append((
            rid, None, None,
            1,
            region_name,
            region_slug,
            None,
            source,
            json.dumps(region_obj, ensure_ascii=False),
        ))

        areas = region_obj.get("area") or {}
        for did_str, district_obj in areas.items():
            try:
                did = int(did_str)
            except Exception:
                continue

            district_name = district_obj.get("name") or ""
            district_slug = (
                district_obj.get("name_url")
                or district_slug_by_key.get((rid, did))
            )
            rows.append((
                rid, did, None,
                2,
                district_name,
                district_slug,
                None,
                source,
                json.dumps(district_obj, ensure_ascii=False),
            ))

    sql = f"""
    INSERT INTO {table}
      (region_id, area_id, ward_id, level, name, name_url, unit_type, source, raw_payload)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      level = VALUES(level),
      name = VALUES(name),
      name_url = VALUES(name_url),
      unit_type = VALUES(unit_type),
      source = VALUES(source),
      raw_payload = VALUES(raw_payload),
      updated_at = CURRENT_TIMESTAMP
    """

    total = 0
    conn = db.get_connection(use_database=True)
    try:
        cur = conn.cursor()
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            cur.executemany(sql, chunk)
            conn.commit()
            total += len(chunk)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return total


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    json_path = base_dir / "template" / "loadRegionsV2.json"

    db = Database(
        host="localhost",
        user="root",
        password="",
        database="craw_db",
        port=3306,
    )

    ensure_location_detail_table(db, table="location_detail")
    count = import_regions_from_json_to_location_detail(
        db=db,
        json_path=str(json_path),
        table="location_detail",
        source="loadRegionsV2",
        batch_size=500,
    )
    print("Upserted rows:", count)
