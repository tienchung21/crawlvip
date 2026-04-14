#!/usr/bin/env python3
import argparse
import os
import re
import sys
from decimal import Decimal, InvalidOperation

import pymysql


SALE_MAP = {
    "nha_rieng": ("Bán nhà riêng", 1, 2),
    "nha_mat_pho": ("Bán nhà riêng", 1, 2),
    "biet_thu_lien_ke": ("Bán biệt thự", 1, 3),
    "can_ho_chung_cu": ("Bán căn hộ chung cư", 1, 5),
    "chung_cu_mini": ("Bán căn hộ Mini, Dịch vụ", 1, 56),
    "can_ho_van_phong_officetel": ("Bán căn hộ Mini, Dịch vụ", 1, 56),
    "can_ho_dich_vu_homestay": ("Bán căn hộ Mini, Dịch vụ", 1, 56),
    "can_ho_khach_san_condotel": ("Bán nhà hàng - Khách sạn", 1, 13),
    "khach_san_nha_nghi": ("Bán nhà hàng - Khách sạn", 1, 13),
    "shophouse_nha_pho_thuong_mai": ("Bán nhà phố dự án", 1, 1),
    "kho_nha_xuong": ("Bán kho, nhà xưởng", 1, 14),
    "nha_tap_the": ("Bán căn hộ Mini, Dịch vụ", 1, 56),
    "dat": ("Bán đất thổ cư", 2, 11),
    "dat_nen_du_an": ("Bán đất nền dự án", 2, 8),
}

RENT_MAP = {
    "nha_mat_pho": ("Nhà phố", 3, 1),
    "shophouse_nha_pho_thuong_mai": ("Nhà phố", 3, 1),
    "nha_rieng": ("Nhà riêng", 3, 2),
    "biet_thu_lien_ke": ("Biệt thự", 3, 3),
    "can_ho_chung_cu": ("Căn hộ chung cư", 3, 5),
    "can_ho_dich_vu_homestay": ("Căn hộ chung cư", 3, 5),
    "chung_cu_mini": ("Căn hộ chung cư", 3, 5),
    "van_phong": ("Văn phòng", 3, 6),
    "van_phong_coworking": ("Văn phòng", 3, 6),
    "mat_bang_cua_hang_ki_ot": ("Mặt bằng", 3, 12),
    "kho_nha_xuong": ("Nhà kho - Xưởng", 3, 14),
    "phong_tro": ("Phòng trọ", 3, 15),
    "nha_tro": ("Phòng trọ", 3, 15),
    "khach_san_nha_nghi": ("Nhà hàng - Khách sạn", 3, 13),
}


def parse_args():
    p = argparse.ArgumentParser(description="Convert meeymap/meeyland rows from scraped_details_flat to data_full")
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--max-batches", type=int, default=0, help="0 = no limit")
    p.add_argument("--start-id", type=int, default=0)
    p.add_argument("--insert", action="store_true")
    p.add_argument("--socket", type=str, default="/opt/lampp/var/mysql/mysql.sock")
    p.add_argument("--host", type=str, default="localhost")
    p.add_argument("--user", type=str, default="root")
    p.add_argument("--password", type=str, default="")
    p.add_argument("--database", type=str, default="craw_db")
    return p.parse_args()


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
        n = Decimal(m.group(0))
        if n == 0:
            return None
        return n
    except InvalidOperation:
        return None


def parse_int(v):
    n = parse_num(v)
    return int(n) if n is not None else None


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
    elif "nghin" in s or "ngàn" in s or "ngan" in s or "k" in s:
        mult = Decimal("1000")
    out = (num * mult).quantize(Decimal("0.01"))
    if out == 0:
        return None
    return out


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


def main():
    args = parse_args()
    conn = connect(args)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT code,cafeland_id FROM location_meeland WHERE level_type='city' AND code IS NOT NULL AND cafeland_id IS NOT NULL"
        )
        city_map = {str(r["code"]): int(r["cafeland_id"]) for r in cur.fetchall()}
        cur.execute(
            "SELECT code,cafeland_id FROM location_meeland WHERE level_type='ward' AND code IS NOT NULL AND cafeland_id IS NOT NULL"
        )
        ward_map = {str(r["code"]): int(r["cafeland_id"]) for r in cur.fetchall()}
        cur.execute("SELECT meeyland_project_id,duan_id,duan_ten FROM duan_meeyland_duan_merge")
        project_map = {
            str(r["meeyland_project_id"]): (r["duan_id"], r["duan_ten"])
            for r in cur.fetchall()
            if r.get("meeyland_project_id")
        }

    last_id = args.start_id
    batch_no = 0
    seen = inserted = skip_type = skip_region = skip_exists = 0

    while True:
        if args.max_batches and batch_no >= args.max_batches:
            break
        batch_no += 1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id,domain,trade_type,matin,title,mota,khoanggia,dientich,huongnha,sotang,sophongvesinh,sophongngu,duongvao,phaply,
                       lat,lng,tenmoigioi,sodienthoai,chieungang,chieudai,loaihinh,thuocduan,city_ext,ward_ext
                FROM scraped_details_flat
                WHERE id > %s
                  AND domain IN ('meeymap.com','meeyland.com')
                  AND trade_type IN ('s','u')
                  AND title IS NOT NULL
                  AND matin IS NOT NULL
                  AND COALESCE(created_at, NOW()) >= (NOW() - INTERVAL 6 MONTH)
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
        mats_by_source = {}
        for r in rows:
            mats_by_source.setdefault(r["domain"], []).append(r["matin"])

        first_img = {}
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(ids))
            cur.execute(
                f"""
                SELECT detail_id,SUBSTRING_INDEX(GROUP_CONCAT(image_url ORDER BY idx,id SEPARATOR '||'),'||',1) AS first_img
                FROM scraped_detail_images
                WHERE detail_id IN ({ph})
                GROUP BY detail_id
                """,
                ids,
            )
            for rr in cur.fetchall():
                first_img[int(rr["detail_id"])] = rr["first_img"]

        existing = set()
        with conn.cursor() as cur:
            for src, mats in mats_by_source.items():
                uniq = list(dict.fromkeys(mats))
                chunk = 500
                for i in range(0, len(uniq), chunk):
                    sub = uniq[i : i + chunk]
                    ph = ",".join(["%s"] * len(sub))
                    cur.execute(
                        f"SELECT source_post_id FROM data_full WHERE source=%s AND source_post_id IN ({ph})",
                        [src, *sub],
                    )
                    for rr in cur.fetchall():
                        existing.add((src, str(rr["source_post_id"])))

        to_insert = []
        converted_ids = []
        for r in rows:
            key = (r["domain"], str(r["matin"]))
            if key in existing:
                skip_exists += 1
                continue

            rule = SALE_MAP.get(r["loaihinh"]) if r["trade_type"] == "s" else RENT_MAP.get(r["loaihinh"])
            if not rule:
                skip_type += 1
                continue

            province_id = city_map.get(str(r["city_ext"])) if r["city_ext"] is not None else None
            ward_id = ward_map.get(str(r["ward_ext"])) if r["ward_ext"] is not None else None
            if not province_id or not ward_id:
                skip_region += 1
                continue

            property_type, cat_id, type_id = rule
            broker = (r["tenmoigioi"] or "").strip() if r.get("tenmoigioi") else None
            phone = r.get("sodienthoai")
            if broker == "Tài khoản Tin Crawl (system)":
                broker = "Hỗ trợ online"
                phone = "0942 825 711"

            project_id = project_name = None
            if r.get("thuocduan"):
                mapped = project_map.get(str(r["thuocduan"]))
                if mapped:
                    project_id, project_name = mapped
                else:
                    project_name = r["thuocduan"]

            to_insert.append(
                (
                    (r["title"] or "")[:255] or None,
                    first_img.get(r["id"]),
                    parse_price(r["khoanggia"]),
                    parse_num(r["dientich"]),
                    r.get("mota"),
                    property_type,
                    "Cần bán" if r["trade_type"] == "s" else "Cho thuê",
                    r.get("huongnha"),
                    parse_int(r.get("sotang")),
                    parse_int(r.get("sophongvesinh")),
                    parse_num(r.get("duongvao")),
                    parse_int(r.get("sophongngu")),
                    r.get("phaply"),
                    parse_num(r.get("lat")),
                    parse_num(r.get("lng")),
                    broker,
                    phone,
                    r["domain"],
                    r["matin"],
                    parse_num(r.get("chieungang")),
                    parse_num(r.get("chieudai")),
                    province_id,
                    ward_id,
                    r["id"],
                    project_name,
                    project_id,
                    cat_id,
                    type_id,
                    "thang" if cat_id == 3 else "md",
                )
            )
            converted_ids.append(r["id"])

        if args.insert and to_insert:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO data_full
                    (title,img,price,area,description,property_type,type,house_direction,floors,bathrooms,road_width,bedrooms,legal_status,
                     lat,`long`,broker_name,phone,source,time_converted_at,source_post_id,width,length,province_id,ward_id,id_img,project_name,project_id,cat_id,type_id,unit)
                    VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    to_insert,
                )
                ph = ",".join(["%s"] * len(converted_ids))
                cur.execute(f"UPDATE scraped_details_flat SET datafull_converted=1 WHERE id IN ({ph})", converted_ids)
            conn.commit()

        inserted += len(to_insert)
        print(
            f"[BATCH {batch_no}] last_id={last_id} seen={seen} inserted={inserted} "
            f"skip_exists={skip_exists} skip_type={skip_type} skip_region={skip_region}",
            flush=True,
        )

    print(
        f"[DONE] seen={seen} inserted={inserted} skip_exists={skip_exists} skip_type={skip_type} skip_region={skip_region}",
        flush=True,
    )
    conn.close()


if __name__ == "__main__":
    main()
