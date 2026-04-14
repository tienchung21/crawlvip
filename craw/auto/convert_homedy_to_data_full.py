#!/usr/bin/env python3
import argparse
import os
import re
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import pymysql


SALE_TYPE_MAP = {
    57: ("Bán căn hộ chung cư", 1, 5),
    73: ("Bán căn hộ Mini, Dịch vụ", 1, 56),
    70: ("Bán căn hộ chung cư", 1, 5),
    71: ("Bán căn hộ chung cư", 1, 5),
    164: ("Bán căn hộ chung cư", 1, 5),
    165: ("Bán căn hộ chung cư", 1, 5),
    166: ("Bán căn hộ chung cư", 1, 5),
    167: ("Bán căn hộ chung cư", 1, 5),
    68: ("Bán căn hộ chung cư", 1, 5),
    168: ("Bán căn hộ chung cư", 1, 5),
    169: ("Bán căn hộ chung cư", 1, 5),
    62: ("Bán nhà riêng", 1, 2),
    63: ("Bán nhà phố dự án", 1, 1),
    66: ("Bán nhà phố dự án", 1, 1),
    56: ("Bán biệt thự", 1, 3),
    172: ("Bán biệt thự", 1, 3),
    170: ("Bán biệt thự", 1, 3),
    171: ("Bán biệt thự", 1, 3),
    190: ("Bán biệt thự", 1, 3),
    58: ("Bán đất thổ cư", 2, 11),
    78: ("Bán đất nông, lâm nghiệp", 2, 10),
    77: ("Bán đất nông, lâm nghiệp", 2, 10),
    79: ("Bán đất nền dự án", 2, 8),
    83: ("Bán kho, nhà xưởng", 1, 14),
    85: ("Bán nhà hàng - Khách sạn", 1, 13),
}

RENT_TYPE_MAP = {
    57: ("Căn hộ chung cư", 3, 5),
    73: ("Căn hộ chung cư", 3, 5),
    70: ("Căn hộ chung cư", 3, 5),
    71: ("Căn hộ chung cư", 3, 5),
    164: ("Căn hộ chung cư", 3, 5),
    165: ("Căn hộ chung cư", 3, 5),
    166: ("Căn hộ chung cư", 3, 5),
    167: ("Căn hộ chung cư", 3, 5),
    68: ("Căn hộ chung cư", 3, 5),
    168: ("Căn hộ chung cư", 3, 5),
    169: ("Căn hộ chung cư", 3, 5),
    76: ("Căn hộ chung cư", 3, 5),
    62: ("Nhà riêng", 3, 2),
    63: ("Nhà phố", 3, 1),
    66: ("Nhà phố", 3, 1),
    56: ("Biệt thự", 3, 3),
    172: ("Biệt thự", 3, 3),
    170: ("Biệt thự", 3, 3),
    171: ("Biệt thự", 3, 3),
    190: ("Biệt thự", 3, 3),
    81: ("Phòng trọ", 3, 15),
    59: ("Văn phòng", 3, 6),
    86: ("Mặt bằng", 3, 12),
    87: ("Nhà Kho - Xưởng", 3, 14),
}


def parse_args():
    p = argparse.ArgumentParser(description="Convert homedy.com rows from scraped_details_flat to data_full")
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--start-id", type=int, default=0)
    p.add_argument("--max-batches", type=int, default=0, help="0 = no limit")
    p.add_argument("--insert", action="store_true")
    p.add_argument("--socket", type=str, default="/opt/lampp/var/mysql/mysql.sock")
    p.add_argument("--host", type=str, default="localhost")
    p.add_argument("--user", type=str, default="root")
    p.add_argument("--password", type=str, default="")
    p.add_argument("--database", type=str, default="craw_db")
    return p.parse_args()


def connect(args):
    kwargs = dict(
        user=args.user,
        password=args.password,
        database=args.database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    if args.socket and os.path.exists(args.socket):
        kwargs["unix_socket"] = args.socket
    else:
        kwargs["host"] = args.host
    return pymysql.connect(**kwargs)


def parse_num(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        out = Decimal(m.group(0))
    except InvalidOperation:
        return None
    return None if out == 0 else out


def parse_int(v):
    n = parse_num(v)
    return int(n) if n is not None else None


def parse_lat(v):
    n = parse_num(v)
    if n is None:
        return None
    if n < Decimal("-90") or n > Decimal("90"):
        return None
    return n


def parse_lng(v):
    n = parse_num(v)
    if n is None:
        return None
    if n < Decimal("-180") or n > Decimal("180"):
        return None
    return n


def parse_price(v):
    if v is None:
        return None
    s = str(v).lower().strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    num = Decimal(m.group(1))
    mult = Decimal("1")
    if "ty" in s or "tỷ" in s:
        mult = Decimal("1000000000")
    elif "trieu" in s or "triệu" in s or "tr" in s:
        mult = Decimal("1000000")
    elif "nghin" in s or "nghìn" in s or "ngan" in s or "k" in s:
        mult = Decimal("1000")
    out = (num * mult).quantize(Decimal("0.01"))
    if out <= 0:
        return None
    # Guard DECIMAL(20,2) overflow and absurd crawler values.
    if out > Decimal("999999999999999999.99"):
        return None
    return out


def parse_posted_at(raw):
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    candidates = [
        ("%Y-%m-%d %H:%M:%S", s),
        ("%Y-%m-%d", s),
        ("%d/%m/%Y", s),
        ("%d-%m-%Y", s),
        ("%Y-%m-%dT%H:%M:%S", s[:19]),
    ]
    for fmt, value in candidates:
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            pass
    return None


def map_type(trade_type, loaihinh):
    lid = parse_int(loaihinh)
    if lid is None:
        return None
    if trade_type == "s":
        return SALE_TYPE_MAP.get(lid)
    if trade_type == "u":
        return RENT_TYPE_MAP.get(lid)
    return None


def main():
    args = parse_args()
    conn = connect(args)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT location_id,cafeland_id FROM location_homedy WHERE level_type='city' AND cafeland_id IS NOT NULL"
        )
        city_map = {str(r["location_id"]): int(r["cafeland_id"]) for r in cur.fetchall()}
        cur.execute(
            "SELECT location_id,cafeland_id FROM location_homedy WHERE level_type='ward' AND cafeland_id IS NOT NULL"
        )
        ward_map = {str(r["location_id"]): int(r["cafeland_id"]) for r in cur.fetchall()}
        cur.execute("SELECT homedy_project_id,duan_id,duan_ten FROM duan_homedy_duan_merge")
        project_map = {
            str(r["homedy_project_id"]): (r["duan_id"], r["duan_ten"])
            for r in cur.fetchall()
            if r.get("homedy_project_id")
        }

    last_id = args.start_id
    batch_no = 0
    seen = inserted = updated = skip_exists = skip_type = skip_date = skip_region = 0
    six_months_ago = datetime.now() - timedelta(days=183)

    while True:
        if args.max_batches and batch_no >= args.max_batches:
            break
        batch_no += 1
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id,matin,title,mota,khoanggia,dientich,sophongngu,sophongvesinh,lat,lng,tenmoigioi,sodienthoai,
                       diachi,ngaydang,trade_type,loaihinh,thuocduan,city_ext,ward_ext
                FROM scraped_details_flat
                WHERE id > %s
                  AND domain='homedy.com'
                  AND matin IS NOT NULL
                  AND title IS NOT NULL
                  AND trade_type IN ('s','u')
                ORDER BY id ASC
                LIMIT %s
                """,
                (last_id, args.batch_size),
            )
            rows = cur.fetchall()
        if not rows:
            break

        last_id = rows[-1]["id"]
        seen += len(rows)

        ids = [r["id"] for r in rows]
        mats = [str(r["matin"]) for r in rows]

        first_img = {}
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(ids))
            cur.execute(
                f"""
                SELECT detail_id, SUBSTRING_INDEX(GROUP_CONCAT(image_url ORDER BY idx,id SEPARATOR '||'),'||',1) AS first_img
                FROM scraped_detail_images
                WHERE detail_id IN ({ph})
                GROUP BY detail_id
                """,
                ids,
            )
            for rr in cur.fetchall():
                first_img[int(rr["detail_id"])] = rr["first_img"]

        existing = {}
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(mats))
            cur.execute(
                f"SELECT id,source_post_id FROM data_full WHERE source='homedy.com' AND source_post_id IN ({ph})",
                mats,
            )
            for rr in cur.fetchall():
                existing[str(rr["source_post_id"])] = int(rr["id"])

        to_insert = []
        to_update = []
        converted_ids = []

        for r in rows:
            posted_at = parse_posted_at(r.get("ngaydang"))
            if not posted_at or posted_at < six_months_ago:
                skip_date += 1
                continue

            mapped = map_type(r.get("trade_type"), r.get("loaihinh"))
            if not mapped:
                skip_type += 1
                continue
            property_type, cat_id, type_id = mapped

            province_id = city_map.get(str(r["city_ext"])) if r.get("city_ext") is not None else None
            ward_id = ward_map.get(str(r["ward_ext"])) if r.get("ward_ext") is not None else None
            if not province_id or not ward_id:
                skip_region += 1
                continue

            project_id = None
            project_name = None
            if r.get("thuocduan"):
                pm = project_map.get(str(r["thuocduan"]))
                if pm:
                    project_id, project_name = pm

            payload = (
                (r.get("title") or "")[:255] or None,
                r.get("diachi"),
                posted_at.strftime("%Y-%m-%d %H:%M:%S"),
                first_img.get(r["id"]),
                parse_price(r.get("khoanggia")),
                parse_num(r.get("dientich")),
                r.get("mota"),
                property_type,
                r.get("trade_type"),
                parse_int(r.get("sophongvesinh")),
                parse_int(r.get("sophongngu")),
                parse_lat(r.get("lat")),
                parse_lng(r.get("lng")),
                r.get("tenmoigioi"),
                r.get("sodienthoai"),
                str(r.get("matin")),
                r["id"],
                province_id,
                ward_id,
                project_name,
                project_id,
                cat_id,
                type_id,
                ("thang" if r.get("trade_type") == "u" else "md"),
            )

            df_id = existing.get(str(r["matin"]))
            if df_id:
                to_update.append(payload + (df_id,))
                updated += 1
            else:
                to_insert.append(payload)
                inserted += 1
            converted_ids.append(r["id"])

        if args.insert and (to_insert or to_update):
            ok = False
            for attempt in range(1, 4):
                try:
                    with conn.cursor() as cur:
                        if to_insert:
                            cur.executemany(
                                """
                                INSERT INTO data_full
                                (title,address,posted_at,img,price,area,description,property_type,type,bathrooms,bedrooms,lat,`long`,
                                 broker_name,phone,source,time_converted_at,source_post_id,id_img,province_id,ward_id,project_name,
                                 project_id,cat_id,type_id,unit,images_status)
                                VALUES
                                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'homedy.com',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING')
                                """,
                                to_insert,
                            )
                        if to_update:
                            cur.executemany(
                                """
                                UPDATE data_full SET
                                    title=%s,address=%s,posted_at=%s,img=%s,price=%s,area=%s,description=%s,property_type=%s,type=%s,
                                    bathrooms=%s,bedrooms=%s,lat=%s,`long`=%s,broker_name=%s,phone=%s,source='homedy.com',
                                    time_converted_at=NOW(),source_post_id=%s,id_img=%s,province_id=%s,ward_id=%s,project_name=%s,
                                    project_id=%s,cat_id=%s,type_id=%s,unit=%s
                                WHERE id=%s
                                """,
                                to_update,
                            )
                        if converted_ids:
                            ph = ",".join(["%s"] * len(converted_ids))
                            cur.execute(f"UPDATE scraped_details_flat SET datafull_converted=1 WHERE id IN ({ph})", converted_ids)
                    conn.commit()
                    ok = True
                    break
                except pymysql.err.OperationalError as ex:
                    conn.rollback()
                    if ex.args and ex.args[0] in (1205, 1213):
                        time.sleep(1.5 * attempt)
                        continue
                    raise
            if not ok:
                raise RuntimeError("Batch failed after retries due to lock/deadlock")
        else:
            skip_exists += len([r for r in rows if str(r.get("matin")) in existing])

        print(
            f"[BATCH {batch_no}] last_id={last_id} seen={seen} insert={inserted} update={updated} "
            f"skip_date={skip_date} skip_type={skip_type} skip_region={skip_region}",
            flush=True,
        )

    print(
        f"[DONE] seen={seen} insert={inserted} update={updated} skip_date={skip_date} "
        f"skip_type={skip_type} skip_region={skip_region}",
        flush=True,
    )
    conn.close()


if __name__ == "__main__":
    main()
