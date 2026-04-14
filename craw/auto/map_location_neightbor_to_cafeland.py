import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import pymysql

DB = dict(host='127.0.0.1', port=3306, user='root', password='', database='craw_db', charset='utf8mb4', autocommit=False)
REPORT = Path('/home/chungnt/crawlvip/location_neightbor_cafeland_mapping_report.json')

PROVINCE_PREFIXES = [
    'tinh ',
    'thanh pho ',
    'tp ',
]
WARD_PREFIXES = [
    'phuong ',
    'xa ',
    'thi tran ',
    'dac khu ',
]


def norm_text(value: str) -> str:
    if value is None:
        return ''
    s = value.strip().lower()
    s = s.replace('đ', 'd').replace('Ð', 'd')
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def strip_prefixes(value: str, prefixes) -> str:
    s = norm_text(value)
    changed = True
    while changed:
        changed = False
        for prefix in prefixes:
            if s.startswith(prefix):
                s = s[len(prefix):].strip()
                changed = True
    return s


def norm_province(value: str) -> str:
    s = strip_prefixes(value, PROVINCE_PREFIXES)
    if s == 'ha noi':
        return 'ha noi'
    if s == 'ho chi minh':
        return 'ho chi minh'
    if s == 'can tho':
        return 'can tho'
    if s == 'da nang':
        return 'da nang'
    if s == 'hai phong':
        return 'hai phong'
    if s == 'hue' or s == 'thua thien hue':
        return 'hue'
    return s


def norm_ward(value: str) -> str:
    return strip_prefixes(value, WARD_PREFIXES)


def main():
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE location_neightbor
                  ADD COLUMN source_cafeland_id INT NULL,
                  ADD COLUMN source_parent_cafeland_id INT NULL,
                  ADD COLUMN neighbor_cafeland_id INT NULL,
                  ADD COLUMN neighbor_parent_cafeland_id INT NULL
                """
            )
            conn.commit()
    except Exception:
        conn.rollback()
    
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT new_city_id, new_city_name
            FROM transaction_city_merge
            WHERE COALESCE(new_city_parent_id, 0) = 0
            ORDER BY new_city_id
            """
        )
        provinces = cur.fetchall()

        province_map = {}
        province_dupes = defaultdict(list)
        for new_city_id, new_city_name in provinces:
            key = norm_province(new_city_name)
            province_dupes[key].append((new_city_id, new_city_name))
            province_map[key] = new_city_id

        cur.execute(
            """
            SELECT DISTINCT new_city_id, new_city_parent_id, new_city_name
            FROM transaction_city_merge
            WHERE COALESCE(new_city_parent_id, 0) <> 0
            ORDER BY new_city_parent_id, new_city_id
            """
        )
        wards = cur.fetchall()

        wards_by_province = defaultdict(dict)
        ward_dupes = defaultdict(lambda: defaultdict(list))
        for new_city_id, parent_id, new_city_name in wards:
            key = norm_ward(new_city_name)
            ward_dupes[parent_id][key].append((new_city_id, new_city_name))
            wards_by_province[parent_id][key] = new_city_id

        cur.execute(
            """
            SELECT DISTINCT source_osm_id, source_unit_type, source_name_vi, source_parent_name
            FROM location_neightbor
            ORDER BY source_osm_id
            """
        )
        entities = cur.fetchall()

        source_updates = []
        source_mismatch = []
        for osm_id, unit_type, name_vi, parent_name in entities:
            if unit_type == 'province':
                pid = province_map.get(norm_province(name_vi))
                if pid is None:
                    source_mismatch.append({'side': 'source', 'type': unit_type, 'osm_id': osm_id, 'name_vi': name_vi, 'parent_name': parent_name, 'reason': 'province_not_found'})
                source_updates.append((pid, None, osm_id))
            else:
                parent_id = province_map.get(norm_province(parent_name))
                cid = wards_by_province.get(parent_id, {}).get(norm_ward(name_vi)) if parent_id else None
                if parent_id is None or cid is None:
                    source_mismatch.append({'side': 'source', 'type': unit_type, 'osm_id': osm_id, 'name_vi': name_vi, 'parent_name': parent_name, 'reason': 'ward_or_parent_not_found', 'resolved_parent_id': parent_id})
                source_updates.append((cid, parent_id, osm_id))

        cur.executemany(
            "UPDATE location_neightbor SET source_cafeland_id=%s, source_parent_cafeland_id=%s WHERE source_osm_id=%s",
            source_updates,
        )
        conn.commit()

        cur.execute(
            """
            SELECT DISTINCT
                l.source_unit_type,
                l.neighbor_osm_id,
                l.neighbor_name_vi,
                l.neighbor_parent_name
            FROM location_neightbor l
            ORDER BY l.neighbor_osm_id
            """
        )
        neighbors = cur.fetchall()

        neighbor_updates = []
        neighbor_mismatch = []
        for source_unit_type, osm_id, name_vi, parent_name in neighbors:
            if source_unit_type == 'province':
                pid = province_map.get(norm_province(name_vi))
                if pid is None:
                    neighbor_mismatch.append({'side': 'neighbor', 'type': source_unit_type, 'osm_id': osm_id, 'name_vi': name_vi, 'parent_name': parent_name, 'reason': 'province_not_found'})
                neighbor_updates.append((pid, None, osm_id, source_unit_type))
            else:
                parent_id = province_map.get(norm_province(parent_name))
                cid = wards_by_province.get(parent_id, {}).get(norm_ward(name_vi)) if parent_id else None
                if parent_id is None or cid is None:
                    neighbor_mismatch.append({'side': 'neighbor', 'type': source_unit_type, 'osm_id': osm_id, 'name_vi': name_vi, 'parent_name': parent_name, 'reason': 'ward_or_parent_not_found', 'resolved_parent_id': parent_id})
                neighbor_updates.append((cid, parent_id, osm_id, source_unit_type))

        cur.executemany(
            "UPDATE location_neightbor SET neighbor_cafeland_id=%s, neighbor_parent_cafeland_id=%s WHERE neighbor_osm_id=%s AND source_unit_type=%s",
            neighbor_updates,
        )
        conn.commit()

        cur.execute("SELECT COUNT(DISTINCT source_osm_id) FROM location_neightbor WHERE source_unit_type='province' AND source_cafeland_id IS NULL")
        src_province_unmatched = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT source_osm_id) FROM location_neightbor WHERE source_unit_type='ward_or_commune' AND source_cafeland_id IS NULL")
        src_ward_unmatched = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT neighbor_osm_id) FROM location_neightbor WHERE source_unit_type='province' AND neighbor_cafeland_id IS NULL")
        nb_province_unmatched = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT neighbor_osm_id) FROM location_neightbor WHERE source_unit_type='ward_or_commune' AND neighbor_cafeland_id IS NULL")
        nb_ward_unmatched = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT source_osm_id) FROM location_neightbor WHERE source_unit_type='province'")
        src_province_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT source_osm_id) FROM location_neightbor WHERE source_unit_type='ward_or_commune'")
        src_ward_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT neighbor_osm_id) FROM location_neightbor WHERE source_unit_type='province'")
        nb_province_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT neighbor_osm_id) FROM location_neightbor WHERE source_unit_type='ward_or_commune'")
        nb_ward_total = cur.fetchone()[0]

    report = {
        'source': {
            'province_total': src_province_total,
            'province_unmatched': src_province_unmatched,
            'ward_total': src_ward_total,
            'ward_unmatched': src_ward_unmatched,
            'sample_unmatched': source_mismatch[:100],
        },
        'neighbor': {
            'province_total': nb_province_total,
            'province_unmatched': nb_province_unmatched,
            'ward_total': nb_ward_total,
            'ward_unmatched': nb_ward_unmatched,
            'sample_unmatched': neighbor_mismatch[:100],
        },
    }
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))

    conn.close()


if __name__ == '__main__':
    main()
