#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Thuviennhadat detail crawler
- Read PENDING links from collected_links (domain=thuviennhadat.vn)
- Fetch detail HTML with curl_cffi
- Parse key fields
- Save into scraped_details_flat + scraped_detail_images
- Update collected_links.status => DONE / ERROR
"""

from __future__ import annotations

import argparse
import html
import json
import os
import random
import re
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except Exception:
    from database import Database


DOMAIN = "thuviennhadat.vn"
BASE_URL = f"https://{DOMAIN}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Referer": BASE_URL + "/",
}


def _norm_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def _abs_url(href: str) -> str:
    return urljoin(BASE_URL, (href or "").strip())


def _first_phone_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    # ưu tiên số không bị mask ***
    # Bước 1: lấy các cụm có khả năng chứa phone
    chunks = re.findall(r"(?:\+?84|0)[\d\s\.\-]{7,20}", text)
    for ch in chunks:
        if "*" in ch:
            continue
        # Bước 2: tách các cụm số độc lập để tránh dính "0908...-0788..."
        for token in re.findall(r"(?:\+?84|0)\d{8,10}", ch.replace(" ", "")):
            c = re.sub(r"\D", "", token)
            if c.startswith("84") and len(c) in (10, 11, 12):
                return c
            if c.startswith("0") and len(c) in (9, 10, 11):
                return c
    return None


def _parse_ldjson_realestate(soup: BeautifulSoup) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for sc in soup.select('script[type="application/ld+json"]'):
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        objs = obj if isinstance(obj, list) else [obj]
        for it in objs:
            if not isinstance(it, dict):
                continue
            if it.get("@type") != "RealEstateListing":
                continue
            out["ldjson"] = it
            return out
    return out




def _to_number_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    t = str(v).strip().replace(' ', '')
    t = t.replace(',', '.')
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    x = m.group(0)
    try:
        f = float(x)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except Exception:
        return x


def _parse_price_to_vnd(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    s = str(raw).lower().strip()
    if not s:
        return None
    s = s.replace('đ', '').replace('vnđ', '').replace('vnd', '')
    s = s.replace(',', '.')
    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return None
    val = float(m.group(0))
    if 'tỷ' in s or 'ty' in s:
        return int(round(val * 1_000_000_000))
    if 'triệu' in s or 'trieu' in s or 'tr/' in s or 'tr' in s:
        return int(round(val * 1_000_000))
    if 'nghìn' in s or 'ngan' in s or 'k' in s:
        return int(round(val * 1_000))
    # fallback: already VND number
    if val >= 100000:
        return int(round(val))
    return int(round(val))


def _clean_mota(v: Any) -> Optional[str]:
    if v is None:
        return None
    t = str(v).replace('\r\n', '\n').replace('\r', '\n').strip()
    # giữ xuống dòng, chỉ dọn khoảng trắng quanh từng dòng
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split('\n')]
    # bỏ dòng rỗng thừa liên tiếp
    out = []
    prev_empty = False
    for ln in lines:
        empty = (ln == '')
        if empty and prev_empty:
            continue
        out.append(ln)
        prev_empty = empty
    t2 = '\n'.join(out).strip()
    return t2 or None


def _prettify_mota_lines(v: Optional[str]) -> Optional[str]:
    if not v:
        return v
    t = str(v)
    # If source has no line breaks, add light formatting for common bullet patterns
    if '\n' not in t:
        t = re.sub(r"\s+-\s+", "\n- ", t)
        t = re.sub(r"\s+(lh\s*:)", r"\n\1", t, flags=re.IGNORECASE)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip() or None


def _extract_meta(soup: BeautifulSoup, prop: str) -> Optional[str]:
    n = soup.select_one(f'meta[property="{prop}"]')
    if n:
        raw = n.get("content")
        return str(raw).strip() if raw is not None else None
    return None


def _extract_meta_raw_from_html(html_text: str, prop: str) -> Optional[str]:
    """
    Extract meta[property=...] content directly from raw HTML
    to preserve original newlines as much as possible.
    """
    if not html_text:
        return None
    pattern = re.compile(
        rf'<meta[^>]+property=["\']{re.escape(prop)}["\'][^>]+content=["\'](.*?)["\'][^>]*>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(html_text)
    if not m:
        return None
    raw = m.group(1)
    if raw is None:
        return None
    # unescape HTML entities but keep line breaks
    return html.unescape(raw).strip()


def _parse_info_boxes(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    rs = {
        "ngaydang": None,
        "ngayhethan": None,
        "loaitin": None,
        "matin": None,
    }
    for col in soup.select("div.four.wide.column"):
        text = " ".join(col.stripped_strings)
        t = text.lower()
        if "ngày đăng" in t or "ngay dang" in t:
            # Ngày đăng 22/04/2026 14:57
            m = re.search(r"(\d{2}/\d{2}/\d{4})(?:\s+(\d{1,2}:\d{2}))?", text)
            if m:
                rs["ngaydang"] = m.group(1) + (f" {m.group(2)}" if m.group(2) else "")
        elif "ngày hết hạn" in t or "ngay het han" in t:
            m = re.search(r"(\d{2}/\d{2}/\d{4})(?:\s+(\d{1,2}:\d{2}))?", text)
            if m:
                rs["ngayhethan"] = m.group(1) + (f" {m.group(2)}" if m.group(2) else "")
        elif "hạng tin" in t or "hang tin" in t:
            # remove label
            rs["loaitin"] = _norm_text(re.sub(r"(?i)hạng tin|hang tin", "", text).strip())
        elif "mã tin" in t or "ma tin" in t:
            m = re.search(r"(\d{3,})", text)
            rs["matin"] = m.group(1) if m else _norm_text(text)
    return rs


def _extract_images(soup: BeautifulSoup, html_text: str = "") -> List[str]:
    out: List[str] = []
    seen = set()

    def _accept(u: str) -> bool:
        lu = u.lower()
        return (
            "defaulttvnd" not in lu
            and (
                "/upload/images/post-image/" in lu
                or "/upload/tin-dang/hinh-anh/" in lu
            )
        )

    # Ưu tiên ảnh gallery (nhiều layout khác nhau)
    for img in soup.select("img.demo.cursor[src], img.demo[src], div.slick-track img[src], img[src]"):
        src = _norm_text(img.get("src"))
        if not src:
            continue
        u = _abs_url(src)
        if not _accept(u):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    # Fallback: quét regex trên HTML để bắt đủ ảnh bài đăng
    if html_text:
        for m in re.findall(
            r"https?://[^\"\'\s>]+/(?:Upload/images/post-image|upload/tin-dang/hinh-anh)/[^\"\'\s>]+",
            html_text,
            flags=re.IGNORECASE,
        ):
            u = _norm_text(m)
            if not u:
                continue
            if not _accept(u):
                continue
            if u in seen:
                continue
            seen.add(u)
            out.append(u)

    return out


def _extract_loaibds_and_diachi_from_header(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    rs = {"loaibds": None, "diachi": None, "thuocduan": None}
    p = soup.select_one("p.text-truncate-2")
    if not p:
        return rs

    text = _norm_text(p.get_text(" ", strip=True)) or ""
    # Ví dụ: "Bán Nhà riêng tại ..."
    m = re.search(r"^(Bán|Cho Thuê)\s+(.+?)\s+tại\s+(.+)$", text, flags=re.IGNORECASE)
    if m:
        rs["loaibds"] = _norm_text(m.group(2))
        rs["diachi"] = _norm_text(m.group(3))

    # Ví dụ: "... theo dự án Mega Village tại ..."
    m2 = re.search(r"dự án\s+(.+?)\s+tại", text, flags=re.IGNORECASE)
    if m2:
        rs["thuocduan"] = _norm_text(m2.group(1))

    # Fallback: block dự án riêng trong trang detail
    if not rs["thuocduan"]:
        n = soup.select_one(".KinhDoanhNhaDat-detail-grid-amenitites h1.text-primary")
        if n:
            rs["thuocduan"] = _norm_text(n.get_text(" ", strip=True))

    return rs


def _extract_lat_lng(html_text: str) -> Dict[str, Optional[str]]:
    rs = {"lat": None, "lng": None}
    # center=lat,lng
    m = re.search(r"center=([0-9\.\-]+),([0-9\.\-]+)", html_text)
    if not m:
        # origin=lat,lng
        m = re.search(r"origin=([0-9\.\-]+),([0-9\.\-]+)", html_text)
    if m:
        rs["lat"] = m.group(1)
        rs["lng"] = m.group(2)
    return rs


def parse_detail(url: str, html_text: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    ldwrap = _parse_ldjson_realestate(soup)
    ld = ldwrap.get("ldjson") if ldwrap else None

    og_title = _extract_meta(soup, "og:title")
    og_desc_raw = _extract_meta_raw_from_html(html_text, "og:description") or _extract_meta(soup, "og:description")
    og_desc = _prettify_mota_lines(_clean_mota(og_desc_raw))
    title = og_title or _norm_text(soup.title.get_text() if soup.title else None)

    boxes = _parse_info_boxes(soup)
    top = _extract_loaibds_and_diachi_from_header(soup)
    images = _extract_images(soup, html_text)

    ten_mg = _norm_text(soup.select_one("p.text-primary").get_text(" ", strip=True) if soup.select_one("p.text-primary") else None)
    gia_text = _norm_text(soup.select_one("span.unit-value-style").get_text(" ", strip=True) if soup.select_one("span.unit-value-style") else None)

    phone = _first_phone_from_text(og_desc or "")
    if not phone and ld and isinstance(ld.get("seller"), dict):
        phone = _first_phone_from_text(str(ld["seller"].get("telephone") or ""))

    dientich = None
    sophongngu = None
    sophongvesinh = None
    diachi = top.get("diachi")

    if ld and isinstance(ld, dict):
        try:
            floor = (((ld.get("itemOffered") or {}).get("floorSize") or {}).get("value"))
            if floor not in (None, ""):
                dientich = _to_number_text(floor)
        except Exception:
            pass
        try:
            sophongngu = _norm_text(((ld.get("itemOffered") or {}).get("numberOfBedrooms")))
        except Exception:
            pass
        try:
            sophongvesinh = _norm_text(((ld.get("itemOffered") or {}).get("numberOfBathroomsTotal")))
        except Exception:
            pass
        try:
            addr = (ld.get("itemOffered") or {}).get("address") or {}
            if not diachi and isinstance(addr, dict):
                parts = [addr.get("streetAddress"), addr.get("addressLocality"), addr.get("addressRegion")]
                diachi = _norm_text(", ".join([str(x).strip() for x in parts if x and str(x).strip()]))
        except Exception:
            pass

    if not dientich and og_desc:
        m_area = re.search(r"diện tích\s*([0-9\.,]+)\s*m[²2]?", og_desc, flags=re.IGNORECASE)
        if m_area:
            dientich = _to_number_text(m_area.group(1))

    latlng = _extract_lat_lng(html_text)

    gia_num = None
    if ld and isinstance(ld, dict):
        offers = ld.get("offers") or {}
        if isinstance(offers, dict):
            gia_num = _parse_price_to_vnd(offers.get("price"))
    if gia_num is None:
        gia_num = _parse_price_to_vnd(gia_text)

    data = {
        "url": url,
        "title": title,
        "mota": og_desc,
        "khoanggia": gia_num,
        "dientich": dientich,
        "sophongngu": sophongngu,
        "sophongvesinh": sophongvesinh,
        "loaibds": top.get("loaibds"),
        "thuocduan": top.get("thuocduan"),
        "diachi": diachi,
        "tenmoigioi": ten_mg,
        "sodienthoai": phone,
        "ngaydang": boxes.get("ngaydang"),
        "ngayhethan": boxes.get("ngayhethan"),
        "matin": boxes.get("matin"),
        "img": images,
        "lat": latlng.get("lat"),
        "lng": latlng.get("lng"),
        "map": None,
    }
    return data


def fetch_html(url: str) -> Optional[str]:
    try:
        r = cffi_requests.get(url, headers=HEADERS, impersonate="chrome124", timeout=45)
    except Exception as e:
        print(f"  [x] request failed: {e}")
        return None
    if r.status_code != 200:
        print(f"  [x] HTTP {r.status_code}")
        return None
    return r.text or ""


def process_one(db: Database, row: Dict[str, Any]) -> bool:
    link_id = row.get("id")
    url = row.get("url")
    loaihinh = row.get("loaihinh")
    trade_type = row.get("trade_type")

    print(f"[FETCH] id={link_id} url={url}")
    html = fetch_html(url)
    if not html:
        db.update_link_status(url, "ERROR")
        return False

    data = parse_detail(url, html)
    # Theo yêu cầu: loaibds lấy từ loaihinh ở collected_links
    if loaihinh:
        data["loaibds"] = loaihinh
    detail_id = db.add_scraped_detail_flat(
        url=url,
        data=data,
        domain=DOMAIN,
        link_id=link_id,
        loaihinh=loaihinh,
        trade_type=trade_type,
    )

    if detail_id:
        imgs = data.get("img") or []
        if imgs:
            db.add_detail_images(detail_id, imgs)
        db.update_link_status(url, "DONE")
        print(f"  -> Saved detail_id={detail_id}, images={len(imgs)}")
        return True

    db.update_link_status(url, "ERROR")
    print("  -> save failed")
    return False


def run_batch(limit: int, sleep_min: float, sleep_max: float):
    db = Database()
    rows = db.get_pending_links(limit=limit, domain=DOMAIN)
    if not rows:
        print("[DONE] no pending links")
        return

    print(f"[BATCH] pending_fetch={len(rows)}")
    for i, row in enumerate(rows, 1):
        print(f"[{i}/{len(rows)}] Crawling id={row.get('id')}")
        process_one(db, row)
        if i < len(rows):
            sl = random.uniform(sleep_min, sleep_max)
            print(f"  Sleeping {sl:.2f}s...")
            time.sleep(sl)


def run_test(url: str, out_json: Optional[str], test_loaihinh: Optional[str] = None):
    html = fetch_html(url)
    if not html:
        print("[TEST] fetch failed")
        return 1

    data = parse_detail(url, html)
    if test_loaihinh:
        data["loaibds"] = test_loaihinh
    print(json.dumps(data, ensure_ascii=False, indent=2))

    if out_json:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[TEST] wrote: {out_json}")
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Thuviennhadat detail crawler")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--sleep-min", type=float, default=0.8)
    ap.add_argument("--sleep-max", type=float, default=1.4)
    ap.add_argument("--test-url", default="")
    ap.add_argument("--test-loaihinh", default="")
    ap.add_argument("--out-json", default="")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    if args.test_url:
        return run_test(args.test_url, args.out_json or None, args.test_loaihinh or None)

    run_batch(limit=args.limit, sleep_min=args.sleep_min, sleep_max=args.sleep_max)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
