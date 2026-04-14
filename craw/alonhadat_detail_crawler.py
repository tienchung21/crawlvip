#!/usr/bin/env python3
"""
Alonhadat detail crawler using JSON template.

Flow matches dashboard detail flow:
- Extract fields by template (CSS/XPath + valueType)
- Save to scraped_details_flat via Database.add_scraped_detail_flat
- Save raw to scraped_details via Database.add_scraped_detail
- Save images to scraped_detail_images via Database.add_detail_images
"""

import argparse
import json
import os
import random
import re
import sys
from typing import Any, Dict, List, Optional
from pathlib import Path

from bs4 import BeautifulSoup
from lxml import html as lxml_html

from database import Database

try:
    from curl_cffi import requests as cffi_requests
except Exception:
    cffi_requests = None

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None

# Reuse the same captcha solver package used by alonhadat link crawler
_solver_dir = os.path.join(os.getcwd(), "alonhadat-captcha")
if _solver_dir not in sys.path:
    sys.path.append(_solver_dir)
try:
    from captcha_solver import solve as solve_captcha
except Exception:
    solve_captcha = None


DEFAULT_TEMPLATE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "template",
    "alonhadatdetail.json",
)
DEFAULT_COOKIE_PROFILE_DIR = str(Path(__file__).resolve().parent / "playwright_profile_alonhadat")


def refresh_cookie_from_playwright(
    profile_dir: str,
    headless: bool = True,
    timeout_ms: int = 30000,
    login_user: str = "",
    login_pass: str = "",
    force_login: bool = False,
) -> Optional[str]:
    if sync_playwright is None:
        print("[COOKIE] playwright not available; skip auto refresh.")
        return None
    profile_dir = str(profile_dir or "").strip()
    if not profile_dir:
        return None
    Path(profile_dir).mkdir(parents=True, exist_ok=True)
    try:
        captured_cookie_header: Optional[str] = None
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=headless,
                args=["--no-sandbox"],
            )
            page = context.new_page()
            def _on_request(req):
                nonlocal captured_cookie_header
                try:
                    if "handler/handler.ashx?command=19" in req.url.lower():
                        h = req.headers
                        c = h.get("cookie")
                        if c:
                            captured_cookie_header = c
                except Exception:
                    pass
            page.on("request", _on_request)
            target = "https://alonhadat.com.vn/forbidden.aspx" if force_login else "https://alonhadat.com.vn/quan-ly-ca-nhan.html"
            page.goto(target, wait_until="domcontentloaded", timeout=timeout_ms)
            if login_user and login_pass:
                try:
                    if page.locator("#account").count() > 0 and page.locator("#password").count() > 0:
                        page.locator("#account").first.fill(login_user)
                        page.locator("#password").first.fill(login_pass)
                        if page.locator("#remember").count() > 0:
                            try:
                                page.locator("#remember").first.check()
                            except Exception:
                                pass
                        try:
                            page.evaluate("if (typeof Login === 'function') { Login(); }")
                        except Exception:
                            if page.locator(".input-form .button .login").count() > 0:
                                page.locator(".input-form .button .login").first.click(force=True)
                        page.wait_for_timeout(2500)
                except Exception as e:
                    print(f"[COOKIE] auto-login error: {e}")
            try:
                page.goto("https://alonhadat.com.vn/quan-ly-ca-nhan.html", wait_until="domcontentloaded", timeout=timeout_ms)
            except Exception:
                pass
            # Trigger same-origin request seen in browser flow; capture its cookie header if any.
            try:
                page.evaluate(
                    """
                    () => fetch('/handler/Handler.ashx?command=19', {
                        method: 'POST',
                        credentials: 'include',
                        cache: 'no-store'
                    }).catch(() => null)
                    """
                )
                page.wait_for_timeout(1200)
            except Exception:
                pass
            page.wait_for_timeout(1500)
            cookies = context.cookies("https://alonhadat.com.vn")
            context.close()
        if not cookies:
            print("[COOKIE] no cookies from browser context.")
            return None
        kv = [f"{c.get('name')}={c.get('value')}" for c in cookies if c.get("name") and c.get("value") is not None]
        cookie_str = "; ".join(kv).strip()
        names = {c.get("name") for c in cookies}
        # Some sessions do not expose page_idcc; ASP.NET_SessionId + remember/loginname
        # is a more reliable indicator in current Alonhadat flow.
        has_session = ("ASP.NET_SessionId" in names) and (
            ("remember" in names) or ("loginname" in names) or ("password" in names)
        )
        print(
            f"[COOKIE] refreshed from profile={profile_dir} total={len(kv)} "
            f"has_session={has_session} names={sorted(list(names))}"
        )
        if captured_cookie_header:
            print(f"[COOKIE] using captured request cookie header len={len(captured_cookie_header)}")
            return captured_cookie_header
        return cookie_str or None
    except Exception as e:
        print(f"[COOKIE] refresh failed: {e}")
        return None


def _parse_exclude_words(field: Dict[str, Any]) -> List[str]:
    raw = field.get("excludeWords")
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(w).strip() for w in raw if str(w).strip()]
    if isinstance(raw, str):
        parts = re.split(r"[|,]", raw)
        return [p.strip() for p in parts if p.strip()]
    return []


def _apply_exclude_words(value: Any, field: Dict[str, Any]) -> Any:
    if value is None:
        return None
    words = _parse_exclude_words(field)
    if not words:
        return value
    if isinstance(value, list):
        out = []
        for item in value:
            cleaned = _apply_exclude_words(item, field)
            if cleaned:
                out.append(cleaned)
        return out
    if not isinstance(value, str):
        return value
    cleaned = value
    for w in words:
        cleaned = cleaned.replace(w, "")
    cleaned = cleaned.strip()
    return cleaned or None


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", s).strip()
    return t or None


def _norm_multiline_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    text = s.replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    lines = [line for line in lines if line]
    if not lines:
        return None
    return "\n".join(lines)


def _normalize_alonhadat_ngaydang(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    raw = value.strip()
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    m = re.search(r"(\d{2}/\d{2}/\d{4})", raw)
    if m:
        return m.group(1)
    return raw or None


def _is_xpath(selector: str) -> bool:
    sel = (selector or "").strip()
    return sel.startswith("/") or sel.startswith("(")


def _extract_style_url(style_text: str) -> List[str]:
    if not style_text:
        return []
    vals = re.findall(r"url\((['\"]?)(.*?)\1\)", style_text)
    out = []
    for _, u in vals:
        u = (u or "").strip()
        if u:
            out.append(u)
    return out


def _unique_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _extract_src_values(node: Any, is_xpath_node: bool) -> List[str]:
    attrs = [
        "src",
        "data-src",
        "data-original",
        "data-lazy-src",
        "data-lazy",
        "data-thumb",
        "data-image",
    ]
    values: List[str] = []

    def get_attr(obj: Any, key: str) -> Optional[str]:
        try:
            if is_xpath_node and hasattr(obj, "get"):
                return obj.get(key)
            if hasattr(obj, "attrs"):
                return obj.attrs.get(key)
            if hasattr(obj, "get"):
                return obj.get(key)
        except Exception:
            return None
        return None

    def collect_one(obj: Any) -> None:
        for k in attrs:
            v = get_attr(obj, k)
            if v and isinstance(v, str):
                values.append(v.strip())
        style_v = get_attr(obj, "style")
        if style_v and isinstance(style_v, str):
            values.extend(_extract_style_url(style_v))

    collect_one(node)
    try:
        if is_xpath_node:
            for it in node.xpath(".//*"):
                collect_one(it)
        else:
            for it in node.select("*"):
                collect_one(it)
    except Exception:
        pass

    values = [v for v in values if v]
    return _unique_keep_order(values)


def _to_abs_url(u: str) -> str:
    if not u:
        return u
    u = u.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://alonhadat.com.vn" + u
    return u


def _is_valid_listing_image_url(u: str) -> bool:
    s = (u or "").strip().lower()
    if not s:
        return False
    # Drop captcha/logo/ui assets
    bad_markers = [
        "captcha",
        "imagecaptcha",
        "/publish/img/",
        "logo",
        "gotop",
        "bo-cong-thuong",
    ]
    if any(m in s for m in bad_markers):
        return False
    # Keep property images only (avoid related post thumbnails/noise)
    return ("/files/properties/" in s) and ("/images/" in s) and s.endswith((".jpg", ".jpeg", ".png", ".webp"))


def _fallback_extract_images(soup: BeautifulSoup, tree: Any) -> List[str]:
    vals: List[str] = []

    # Priority 1: single image container variant seen on Alonhadat
    for sel in [".imageview #limage", ".imageview img", "#limage"]:
        for n in soup.select(sel):
            for k in ("src", "data-src", "data-original", "data-lazy-src"):
                v = n.get(k)
                if v:
                    vals.append(_to_abs_url(v))

    # Priority 2: images inside detail description block
    for n in soup.select('[itemprop="description"] img, .detail img, .text-content img'):
        for k in ("src", "data-src", "data-original", "data-lazy-src"):
            v = n.get(k)
            if v:
                vals.append(_to_abs_url(v))

    # Priority 3: xpath fallback for robustness
    try:
        for n in tree.xpath('//img[@id="limage" or ancestor::*[contains(@class,"imageview")] or ancestor::*[@itemprop="description"]]'):
            v = n.get("src") or n.get("data-src") or n.get("data-original") or n.get("data-lazy-src")
            if v:
                vals.append(_to_abs_url(v))
    except Exception:
        pass

    vals = [v for v in vals if _is_valid_listing_image_url(v)]
    return _unique_keep_order(vals)


def _extract_field_value(
    field: Dict[str, Any],
    soup: BeautifulSoup,
    tree: Any,
) -> Any:
    selector = (field.get("selector") or "").strip()
    value_type = (field.get("valueType") or field.get("type") or "text").strip().lower()
    if not selector:
        return None, 0

    nodes = []
    try:
        if _is_xpath(selector):
            nodes = tree.xpath(selector)
        else:
            nodes = soup.select(selector)
    except Exception:
        nodes = []

    if not nodes:
        return None, 0

    first = nodes[0]
    is_xpath_node = _is_xpath(selector)

    if value_type == "src":
        val = _extract_src_values(first, is_xpath_node)
    elif value_type == "href":
        if is_xpath_node:
            val = first.get("href") if hasattr(first, "get") else None
        else:
            val = first.get("href")
        val = _norm_text(val)
    elif value_type == "html":
        if is_xpath_node:
            val = lxml_html.tostring(first, encoding="unicode")
        else:
            val = str(first)
        val = val.strip() if isinstance(val, str) else val
    elif value_type in ("innertext", "multiline_text"):
        if is_xpath_node:
            if isinstance(first, str):
                val = _norm_multiline_text(first)
            else:
                val = _norm_multiline_text(first.text_content())
        else:
            val = _norm_multiline_text(first.get_text("\n", strip=True))
    else:
        # text
        if is_xpath_node:
            if isinstance(first, str):
                val = _norm_text(first)
            else:
                val = _norm_text(first.text_content())
        else:
            val = _norm_text(first.get_text(" ", strip=True))

    val = _apply_exclude_words(val, field)
    return val, len(nodes)


def extract_with_template(html_text: str, template: Dict[str, Any]) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    tree = lxml_html.fromstring(html_text)
    extracted: Dict[str, Any] = {}

    for field in template.get("fields", []):
        name = field.get("name")
        if not name:
            continue
        value, _ = _extract_field_value(field, soup, tree)
        extracted[name] = value if value else None

    # Image fallback: when template selector misses (.image-list not present)
    imgs = extracted.get("img")
    if not imgs:
        fb = _fallback_extract_images(soup, tree)
        if fb:
            extracted["img"] = fb
    elif isinstance(imgs, list):
        normalized = [_to_abs_url(x) for x in imgs if isinstance(x, str) and x.strip()]
        normalized = [x for x in normalized if _is_valid_listing_image_url(x)]
        if normalized:
            # Merge with fallback images, keep order unique
            fb = _fallback_extract_images(soup, tree)
            extracted["img"] = _unique_keep_order(normalized + fb)
        else:
            fb = _fallback_extract_images(soup, tree)
            extracted["img"] = fb or None

    return extracted


def get_link_meta(db: Database, url: str, link_id: Optional[int]) -> Dict[str, Any]:
    conn = db.get_connection(use_database=True)
    cur = conn.cursor()
    try:
        if link_id:
            cur.execute(
                """
                SELECT id, domain, loaihinh, trade_type
                FROM collected_links
                WHERE id = %s
                LIMIT 1
                """,
                (link_id,),
            )
        else:
            cur.execute(
                """
                SELECT id, domain, loaihinh, trade_type
                FROM collected_links
                WHERE url = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (url,),
            )
        row = cur.fetchone()
        if not row:
            return {
                "link_id": link_id,
                "domain": "alonhadat.com.vn",
                "loaihinh": None,
                "trade_type": None,
            }
        if isinstance(row, dict):
            return {
                "link_id": row.get("id"),
                "domain": row.get("domain") or "alonhadat.com.vn",
                "loaihinh": row.get("loaihinh"),
                "trade_type": row.get("trade_type"),
            }
        return {
            "link_id": row[0],
            "domain": row[1] or "alonhadat.com.vn",
            "loaihinh": row[2],
            "trade_type": row[3],
        }
    finally:
        cur.close()
        conn.close()


def get_links_by_status(
    db: Database,
    status: str,
    limit: int,
    domain: str = "alonhadat.com.vn",
) -> List[Dict[str, Any]]:
    conn = db.get_connection(use_database=True)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, url, domain, loaihinh, trade_type, status
            FROM collected_links
            WHERE status = %s
              AND (domain = %s OR %s IS NULL)
            ORDER BY id ASC
            LIMIT %s
            """,
            (status, domain, domain, limit),
        )
        rows = cur.fetchall() or []
        if rows and isinstance(rows[0], dict):
            return rows
        result: List[Dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "id": row[0],
                    "url": row[1],
                    "domain": row[2],
                    "loaihinh": row[3],
                    "trade_type": row[4],
                    "status": row[5],
                }
            )
        return result
    finally:
        cur.close()
        conn.close()


def _is_verification_page(final_url: str, html_text: str) -> bool:
    u = (final_url or "").lower()
    h = (html_text or "").lower()
    return (
        "xac-thuc-nguoi-dung" in u
        or "imagecaptcha.ashx" in h
        or "vui-long-thu-lai" in u
        or "mã xác thực" in h
        or "ma xac thuc" in h
    )


def _extract_form_data(html_text: str, captcha_answer: str) -> Dict[str, str]:
    soup = BeautifulSoup(html_text, "html.parser")
    form = soup.find("form")
    if not form:
        return {}
    data: Dict[str, str] = {}
    for input_tag in form.find_all("input"):
        name = input_tag.get("name")
        if not name:
            continue
        value = input_tag.get("value", "")
        lname = name.lower()
        if "captcha" in lname or "txtcaptcha" in lname or "code" in lname:
            data[name] = captcha_answer
        else:
            data[name] = value
    # Alonhadat needs this target for submit
    data["__EVENTTARGET"] = "verify"
    return data


def fetch_html_with_captcha(url: str, cookie: Optional[str], max_retry: int = 3):
    if cffi_requests is None:
        raise RuntimeError("curl_cffi is required for alonhadat detail crawler")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
        "Referer": "https://alonhadat.com.vn/",
    }
    if cookie:
        headers["Cookie"] = cookie

    session = cffi_requests.Session()
    resp = session.get(url, headers=headers, timeout=30, allow_redirects=True, impersonate="chrome120")
    html_text = resp.text
    final_url = resp.url

    if not _is_verification_page(final_url, html_text):
        return True, html_text, final_url, "ok"

    # Need captcha solving
    if solve_captcha is None:
        return False, html_text, final_url, "captcha_solver_missing"

    for _ in range(max_retry):
        # Get captcha image with same session
        img_url = "https://alonhadat.com.vn/ImageCaptcha.ashx?v=3"
        img_resp = session.get(img_url, headers=headers, timeout=20, impersonate="chrome120")
        if img_resp.status_code != 200:
            continue
        answer = solve_captcha(img_resp.content)
        if not answer:
            continue

        form_data = _extract_form_data(html_text, answer)
        if not form_data:
            continue

        # submit on verification page if redirected there
        post_url = final_url
        try:
            s = BeautifulSoup(html_text, "html.parser")
            form = s.find("form")
            if form and form.get("action"):
                action = form.get("action")
                if action.startswith("/"):
                    post_url = "https://alonhadat.com.vn" + action
        except Exception:
            pass

        post_resp = session.post(
            post_url,
            data=form_data,
            headers=headers,
            timeout=30,
            impersonate="chrome120",
            allow_redirects=True,
        )
        # Re-open target detail after verification submit
        resp2 = session.get(url, headers=headers, timeout=30, allow_redirects=True, impersonate="chrome120")
        html_text = resp2.text
        final_url = resp2.url
        if not _is_verification_page(final_url, html_text):
            return True, html_text, final_url, "captcha_passed"

    return False, html_text, final_url, "captcha_failed"


def run_one(
    url: str,
    template_path: str,
    link_id: Optional[int],
    cookie: Optional[str],
    force_domain: Optional[str],
) -> int:
    if not os.path.isfile(template_path):
        print(f"[ERROR] Template not found: {template_path}")
        return 2

    with open(template_path, "r", encoding="utf-8") as f:
        template = json.load(f)

    db = Database()
    meta = get_link_meta(db, url, link_id)
    if force_domain:
        meta["domain"] = force_domain

    ok, html_text, final_url, reason = fetch_html_with_captcha(url, cookie)
    print(f"[HTTP] final_url={final_url}")
    print(f"[HTTP] resolve_reason={reason}")
    if not ok:
        print("[ERROR] Could not pass verification page. Skip save.")
        return 3

    data = extract_with_template(html_text, template)
    data["ngaydang"] = _normalize_alonhadat_ngaydang(data.get("ngaydang"))

    detail_id = db.add_scraped_detail_flat(
        url=url,
        data=data,
        domain=meta.get("domain"),
        link_id=meta.get("link_id"),
        loaihinh=meta.get("loaihinh"),
        trade_type=meta.get("trade_type"),
    )
    db.add_scraped_detail(
        url=url,
        data=data,
        domain=meta.get("domain"),
        link_id=meta.get("link_id"),
        success=True,
    )
    imgs = data.get("img")
    if detail_id and imgs:
        if isinstance(imgs, list):
            db.add_detail_images(detail_id, imgs)
        elif isinstance(imgs, str):
            db.add_detail_images(detail_id, [imgs])

    try:
        db.update_link_status(url, "CRAWLED")
    except Exception:
        pass

    # Report
    non_empty = sum(1 for v in data.values() if v not in (None, "", [], {}))
    print(f"[SAVE] detail_id={detail_id}")
    print(f"[SAVE] fields_non_empty={non_empty}/{len(data)}")
    for k in ["title", "matin", "khoanggia", "dientich", "sodienthoai", "diachi", "loaibds"]:
        print(f"[DATA] {k}={data.get(k)}")
    print(f"[DATA] img_count={len(data.get('img') or []) if isinstance(data.get('img'), list) else (1 if data.get('img') else 0)}")

    return 0


def run_full(
    template_path: str,
    cookie: Optional[str],
    force_domain: Optional[str],
    delay_min_seconds: float,
    delay_max_seconds: float,
    batch_limit: int,
    max_consecutive_block: int,
    cycle_sleep_seconds: float,
    auto_refresh_cookie_on_block: bool,
    cookie_profile_dir: str,
    cookie_refresh_headless: bool,
    login_user: str,
    login_pass: str,
) -> int:
    if not os.path.isfile(template_path):
        print(f"[ERROR] Template not found: {template_path}")
        return 2

    with open(template_path, "r", encoding="utf-8") as f:
        template = json.load(f)

    db = Database()
    total_ok = 0
    total_fail = 0
    cycle = 0
    consecutive_block = 0
    runtime_cookie = cookie

    while True:
        cycle += 1
        batch_status = "PENDING"
        links = get_links_by_status(
            db,
            status=batch_status,
            limit=batch_limit,
            domain="alonhadat.com.vn",
        )
        if not links:
            batch_status = "ERROR"
            links = get_links_by_status(
                db,
                status=batch_status,
                limit=batch_limit,
                domain="alonhadat.com.vn",
            )
        if not links:
            break

        print(f"[BATCH] cycle={cycle} status={batch_status} fetch={len(links)}")
        for idx, link in enumerate(links, 1):
            url = (link.get("url") or "").strip()
            if not url:
                continue
            link_id = link.get("id")
            meta = {
                "link_id": link_id,
                "domain": link.get("domain") or "alonhadat.com.vn",
                "loaihinh": link.get("loaihinh"),
                "trade_type": link.get("trade_type"),
            }
            if force_domain:
                meta["domain"] = force_domain

            print(f"[{idx}/{len(links)}] Crawling id={link_id} url={url[:120]}")
            try:
                ok, html_text, final_url, reason = fetch_html_with_captcha(url, runtime_cookie)
                print(f"  -> final={final_url}")
                print(f"  -> resolve_reason={reason}")
                # If blocked and enabled, refresh cookie from Playwright profile and retry once.
                if not ok and auto_refresh_cookie_on_block:
                    new_cookie = refresh_cookie_from_playwright(
                        profile_dir=cookie_profile_dir,
                        headless=cookie_refresh_headless,
                        login_user=login_user,
                        login_pass=login_pass,
                        force_login=True,
                    )
                    if new_cookie:
                        runtime_cookie = new_cookie
                        print("  -> Retrying once after cookie refresh...")
                        ok2, html2, final2, reason2 = fetch_html_with_captcha(url, runtime_cookie)
                        print(f"  -> final(retry)={final2}")
                        print(f"  -> resolve_reason(retry)={reason2}")
                        if ok2:
                            ok, html_text, final_url, reason = ok2, html2, final2, reason2

                if not ok:
                    print("  -> Verification not passed, set ERROR")
                    try:
                        db.update_link_status(url, "ERROR")
                    except Exception:
                        pass
                    total_fail += 1
                    consecutive_block += 1
                    print(f"  -> consecutive_block={consecutive_block}")

                    # Progressive backoff on consecutive verification blocks:
                    # 2 -> 5m, 3 -> 10m, 4 -> 15m, ... up to 10 -> 45m.
                    if consecutive_block >= 2:
                        import time
                        backoff_minutes = min((consecutive_block - 1) * 5, 45)
                        print(
                            f"  -> blocked backoff: sleeping {backoff_minutes} minutes "
                            f"(consecutive_block={consecutive_block})"
                        )
                        time.sleep(backoff_minutes * 60)

                    if max_consecutive_block > 0 and consecutive_block >= max_consecutive_block:
                        print(
                            f"[STOP] Reached max_consecutive_block={max_consecutive_block}. "
                            "Stop full crawl to avoid hard block."
                        )
                        print("=== DONE FULL ALONHADAT DETAIL ===")
                        print(
                            f"saved_ok={total_ok} failed={total_fail} "
                            f"(stopped_by_block_threshold={max_consecutive_block})"
                        )
                        return 4
                else:
                    data = extract_with_template(html_text, template)
                    data["ngaydang"] = _normalize_alonhadat_ngaydang(data.get("ngaydang"))
                    detail_id = db.add_scraped_detail_flat(
                        url=url,
                        data=data,
                        domain=meta.get("domain"),
                        link_id=meta.get("link_id"),
                        loaihinh=meta.get("loaihinh"),
                        trade_type=meta.get("trade_type"),
                    )
                    db.add_scraped_detail(
                        url=url,
                        data=data,
                        domain=meta.get("domain"),
                        link_id=meta.get("link_id"),
                        success=True,
                    )
                    imgs = data.get("img")
                    if detail_id and imgs:
                        if isinstance(imgs, list):
                            db.add_detail_images(detail_id, imgs)
                        elif isinstance(imgs, str):
                            db.add_detail_images(detail_id, [imgs])
                    db.update_link_status(url, "CRAWLED")
                    total_ok += 1
                    consecutive_block = 0
                    print(f"  -> Saved detail_id={detail_id}")
            except Exception as e:
                print(f"  -> ERROR: {e}")
                try:
                    db.update_link_status(url, "ERROR")
                except Exception:
                    pass
                total_fail += 1

            if delay_max_seconds > 0:
                import time

                delay_s = random.uniform(delay_min_seconds, delay_max_seconds)
                time.sleep(delay_s)

        if cycle_sleep_seconds > 0:
            import time

            print(f"[BATCH] cycle={cycle} done, sleeping {cycle_sleep_seconds}s before next batch...")
            time.sleep(cycle_sleep_seconds)

    print("=== DONE FULL ALONHADAT DETAIL ===")
    print(f"saved_ok={total_ok} failed={total_fail}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Alonhadat detail crawler (template based)")
    parser.add_argument("--url", help="Detail URL (for one-shot mode)")
    parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        help="Template JSON path (default: craw/template/alonhadatdetail.json)",
    )
    parser.add_argument("--link-id", type=int, default=None, help="collected_links.id if known")
    parser.add_argument(
        "--cookie",
        default=os.getenv("ALONHADAT_COOKIE", ""),
        help="Cookie string",
    )
    parser.add_argument("--force-domain", default="alonhadat.com.vn", help="Domain to save")
    parser.add_argument("--full", action="store_true", help="Run full pending links in collected_links")
    parser.add_argument("--delay-min-seconds", type=float, default=13.0, help="Min delay between links in full mode")
    parser.add_argument("--delay-max-seconds", type=float, default=17.0, help="Max delay between links in full mode")
    parser.add_argument("--batch-limit", type=int, default=100, help="Fetch pending links per cycle in full mode")
    parser.add_argument(
        "--max-consecutive-block",
        type=int,
        default=10,
        help="Stop full crawl after N consecutive verification blocks (default 10, 0=disable).",
    )
    parser.add_argument(
        "--cycle-sleep-seconds",
        type=float,
        default=3600.0,
        help="Sleep between full batches (default 3600s = 1 hour).",
    )
    parser.add_argument(
        "--auto-refresh-cookie-on-block",
        action="store_true",
        help="When captcha_failed, refresh cookie from Playwright profile and retry once",
    )
    parser.add_argument(
        "--cookie-profile-dir",
        default=os.getenv("ALONHADAT_COOKIE_PROFILE_DIR", DEFAULT_COOKIE_PROFILE_DIR),
        help="Playwright persistent profile dir for auto cookie refresh",
    )
    parser.add_argument(
        "--cookie-refresh-headed",
        action="store_true",
        help="Use headed browser for cookie refresh (default headless)",
    )
    parser.add_argument("--login-user", default=os.getenv("ALONHADAT_LOGIN_USER", ""), help="Auto login username for cookie refresh")
    parser.add_argument("--login-pass", default=os.getenv("ALONHADAT_LOGIN_PASS", ""), help="Auto login password for cookie refresh")
    args = parser.parse_args()

    if args.full:
        return run_full(
            template_path=args.template,
            cookie=(args.cookie or "").strip() or None,
            force_domain=(args.force_domain or "").strip() or None,
            delay_min_seconds=max(0.0, float(args.delay_min_seconds)),
            delay_max_seconds=max(0.0, float(args.delay_max_seconds)),
            batch_limit=max(1, int(args.batch_limit)),
            max_consecutive_block=max(0, int(args.max_consecutive_block)),
            cycle_sleep_seconds=max(0.0, float(args.cycle_sleep_seconds)),
            auto_refresh_cookie_on_block=bool(args.auto_refresh_cookie_on_block),
            cookie_profile_dir=str(args.cookie_profile_dir or "").strip(),
            cookie_refresh_headless=not bool(args.cookie_refresh_headed),
            login_user=str(args.login_user or "").strip(),
            login_pass=str(args.login_pass or "").strip(),
        )

    if not args.url:
        print("[ERROR] --url is required when not using --full")
        return 2

    return run_one(
        url=args.url.strip(),
        template_path=args.template,
        link_id=args.link_id,
        cookie=(args.cookie or "").strip() or None,
        force_domain=(args.force_domain or "").strip() or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
