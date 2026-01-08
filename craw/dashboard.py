"""

Streamlit Dashboard for Bulk Web Scraping

Manages bulk scraping tasks using templates exported from the Extension

"""



import streamlit as st
import asyncio
import random
import json
import pandas as pd
import sys
import os
import time
import uuid
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
import io
from lxml import html as lxml_html
import re
import requests
# Fix asyncio for Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Helper: parse lat,lng from URL (center=lat,lng hoặc q=lat,lng)
def parse_latlng_from_url(url: str) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    try:
        decoded = url
        m = re.search(r'(?:center|q)=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)', decoded, re.IGNORECASE)
        if m:
            lat = float(m.group(1))
            lng = float(m.group(2))
            return f"{lat},{lng}"
    except Exception:
        return None
    return None


def _is_target_phone_domain(url: str) -> bool:
    if not url:
        return False
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return (
        ("batdongsan.com.vn" in host)
        or ("nhatot.com" in host)
        or ("mogi.vn" in host)
    )


def _get_phone_selector(template: Dict[str, Any]) -> Optional[str]:
    for field in template.get('fields', []):
        name = (field.get('name') or '').strip().lower()
        if name == 'sodienthoai':
            selector = field.get('selector') or field.get('cssSelector') or field.get('xpath') or ''
            selector = selector.strip()
            if selector:
                return selector
    return None


def _is_xpath_selector(selector: str) -> bool:
    return selector.startswith('/') or selector.startswith('(')


def _extract_text_from_tree(tree, selector: str) -> Optional[str]:
    if not tree or not selector:
        return None
    try:
        if selector.startswith('/') or selector.startswith('('):
            elements = tree.xpath(selector)
        else:
            elements = tree.cssselect(selector)
    except Exception:
        return None
    for el in elements:
        if isinstance(el, str):
            text = el.strip()
        else:
            text = el.text_content().strip()
        if text:
            return text
    return None


def _clean_phone_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    match = re.search(r'(\+?\d[\d\s\.\-]{7,}\d)', text)
    if match:
        return match.group(1).strip()
    return text.strip()


def _extract_phone_from_ng_bind(attr_text: Optional[str]) -> Optional[str]:
    if not attr_text:
        return None
    match = re.search(r"PhoneFormat\(['\"]([^'\"]+)['\"]\)", attr_text)
    if match:
        return match.group(1).strip()
    match = re.search(r'(\+?\d[\d\s\.\-]{7,}\d)', attr_text)
    if match:
        return match.group(1).strip()
    return None


def _is_masked_phone(text: Optional[str]) -> bool:
    if not text:
        return True
    if '*' in text:
        return True
    digits = re.sub(r'\D', '', text)
    return len(digits) < 9


async def _get_phone_text_from_page(page, selector: str) -> Optional[str]:
    if not page or not selector:
        return None
    is_xpath = _is_xpath_selector(selector)
    try:
        return await page.evaluate(
            """([sel, useXpath]) => {
                const getText = (el) => (el && (el.innerText || el.textContent) || '').trim();
                const findEl = () => {
                    if (useXpath) {
                        const res = document.evaluate(sel, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                        return res.singleNodeValue;
                    }
                    return document.querySelector(sel);
                };
                const isMasked = (t) => {
                    if (!t) return true;
                    if (t.includes('*')) return true;
                    const digits = t.replace(/\\D/g, '');
                    return digits.length < 9;
                };
                const readPhone = (node) => {
                    if (!node) return '';
                    const attrs = ['mobile','data-mobile','data-phone','data-phone-number','data-phonenumber','data-full-phone','data-contact','data-call'];
                    for (const attr of attrs) {
                        const v = node.getAttribute && node.getAttribute(attr);
                        if (v) return v;
                    }
                    return getText(node);
                };
                const findNhatotPhone = () => {
                    const btns = Array.from(document.querySelectorAll('button.b1b6q6wa, button[data-clad="Button"]'));
                    for (const btn of btns) {
                        const t1 = getText(btn);
                        if (t1 && !isMasked(t1)) return t1;
                        const inner = btn.querySelector('div');
                        const t2 = getText(inner);
                        if (t2 && !isMasked(t2)) return t2;
                    }
                    return '';
                };
                const extractFromNgBind = (node) => {
                    if (!node || !node.getAttribute) return '';
                    const raw = node.getAttribute('ng-bind') || '';
                    if (!raw) return '';
                    const m = raw.match(/PhoneFormat\\(['"]([^'"]+)['"]\\)/);
                    if (m && m[1]) return m[1];
                    const digits = raw.match(/(\\+?\\d[\\d\\s\\.-]{7,}\\d)/);
                    return digits ? digits[1] : '';
                };
                const findBdsPhone = () => {
                    const bdsSel = '.js__phone-event[mobile], .js__phone[mobile], .phoneEvent[mobile]';
                    const bdsNode = document.querySelector(bdsSel);
                    return readPhone(bdsNode);
                };
                const findMogiPhone = () => {
                    const mogiNode = document.querySelector('.agent-contact .ng-binding');
                    return extractFromNgBind(mogiNode) || getText(mogiNode);
                };
                const findTel = (root) => {
                    if (!root) return '';
                    const tel = root.querySelector('a[href^="tel:"]');
                    if (!tel) return '';
                    const href = tel.getAttribute('href') || '';
                    const telNum = href.replace(/^tel:/i, '').trim();
                    return telNum || getText(tel);
                };
                const nhatot = findNhatotPhone();
                if (nhatot) return nhatot;
                const bds = findBdsPhone();
                if (bds && !isMasked(bds)) return bds;
                const mogi = findMogiPhone();
                if (mogi && !isMasked(mogi)) return mogi;
                const el = findEl();
                if (el) {
                    let cur = el;
                    for (let i = 0; i < 4 && cur; i += 1) {
                        const fromNg = extractFromNgBind(cur);
                        if (fromNg) return fromNg;
                        cur = cur.parentElement;
                    }
                }
                let text = getText(el);
                if (text && !isMasked(text)) return text;
                const scope = el ? (el.closest('section,div,li,article') || el.parentElement) : null;
                const tel = findTel(scope) || findTel(document);
                if (tel) return tel;
                const attrs = ['data-phone','data-phone-number','data-phonenumber','data-full-phone','data-contact','data-call'];
                let cur = el;
                for (let i = 0; i < 4 && cur; i += 1) {
                    for (const attr of attrs) {
                        const v = cur.getAttribute && cur.getAttribute(attr);
                        if (v) return v;
                    }
                    cur = cur.parentElement;
                }
                return text || '';
            }""",
            [selector, is_xpath],
        )
    except Exception:
        return None


async def _reveal_phone_before_extract(page, url: str, template: Dict[str, Any]) -> bool:
    if not page or not _is_target_phone_domain(url):
        return False
    selector = _get_phone_selector(template)
    if not selector:
        return False
    try:
        await page.wait_for_timeout(2000)
    except Exception:
        return False
    is_xpath = _is_xpath_selector(selector)
    try:
        locator_selector = f"xpath={selector}" if is_xpath else selector
        locator = page.locator(locator_selector)
        if await locator.count() > 0:
            try:
                await locator.first.click(timeout=2000)
                try:
                    await page.wait_for_function(
                        """([sel, useXpath]) => {
                            let el = null;
                            if (useXpath) {
                                const res = document.evaluate(sel, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                                el = res.singleNodeValue;
                            } else {
                                el = document.querySelector(sel);
                            }
                            const mogiNode = document.querySelector('.agent-contact .ng-binding');
                            if (mogiNode && !isMasked((mogiNode.innerText || mogiNode.textContent || '').trim())) return true;
                            const mogiNode = document.querySelector('.agent-contact .ng-binding');
                            if (mogiNode && !isMasked((mogiNode.innerText || mogiNode.textContent || '').trim())) return true;
                            const mogiNode = document.querySelector('.agent-contact .ng-binding');
                            if (mogiNode) {
                                const raw = mogiNode.getAttribute && mogiNode.getAttribute('ng-bind');
                                const m = raw ? raw.match(/PhoneFormat\(['"]([^'"]+)['"]\)/) : null;
                                if (m && m[1]) return true;
                            }
                            if (!el) return false;
                            const text = (el.innerText || el.textContent || '').trim();
                            return /\\d{3,}/.test(text) || text.includes('*');
                        }""",
                        [selector, is_xpath],
                        timeout=3000,
                    )
                except Exception:
                    await page.wait_for_timeout(3000)
                return True
            except Exception:
                pass
    except Exception:
        pass
    try:
        clicked = await page.evaluate(
            """([sel, useXpath]) => {
                let el = null;
                if (useXpath) {
                    const res = document.evaluate(sel, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                    el = res.singleNodeValue;
                } else {
                    el = document.querySelector(sel);
                }
                if (!el) return false;
                const target = el.closest('button, a') || el;
                target.click();
                return true;
            }""",
            [selector, is_xpath],
        )
        if clicked:
            try:
                await page.wait_for_function(
                    """([sel, useXpath]) => {
                        let el = null;
                        if (useXpath) {
                            const res = document.evaluate(sel, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                            el = res.singleNodeValue;
                        } else {
                            el = document.querySelector(sel);
                        }
                        if (!el) return false;
                        const text = (el.innerText || el.textContent || '').trim();
                        return /\\d{3,}/.test(text) || text.includes('*');
                    }""",
                    [selector, is_xpath],
                    timeout=3000,
                )
            except Exception:
                await page.wait_for_timeout(3000)
            return True
    except Exception:
        pass
    return False


def _parse_exclude_words(field: Dict[str, Any]) -> list:
    raw = field.get('excludeWords')
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(w).strip() for w in raw if str(w).strip()]
    if isinstance(raw, str):
        parts = re.split(r'[|,]', raw)
        return [p.strip() for p in parts if p.strip()]
    return []


def _apply_exclude_words(value: Any, field: Dict[str, Any]) -> Any:
    if value is None:
        return None
    words = _parse_exclude_words(field)
    if not words:
        return value
    if isinstance(value, list):
        cleaned = []
        for v in value:
            cleaned_val = _apply_exclude_words(v, field)
            if cleaned_val:
                cleaned.append(cleaned_val)
        return cleaned
    if not isinstance(value, str):
        return value
    cleaned_value = value
    for word in words:
        cleaned_value = cleaned_value.replace(word, "")
    return cleaned_value.strip()


def _get_inner_html(el: Any) -> Optional[str]:
    if el is None:
        return None
    if isinstance(el, str):
        return el.strip()
    try:
        parts = []
        if getattr(el, 'text', None):
            parts.append(el.text)
        for child in el:
            parts.append(lxml_html.tostring(child, encoding='unicode'))
        html_value = ''.join(parts).strip()
        return html_value if html_value else None
    except Exception:
        return None


def _save_image_bytes(image_bytes: bytes, file_path: str, max_width: int = 1100):
    try:
        from io import BytesIO
        from PIL import Image
    except Exception:
        with open(file_path, 'wb') as f:
            f.write(image_bytes)
        return

    try:
        img = Image.open(BytesIO(image_bytes))
        width, height = img.size
        if width > max_width:
            new_height = max(int(height * max_width / width), 1)
            img = img.resize((max_width, new_height), Image.LANCZOS)
        ext = os.path.splitext(file_path)[1].lower()
        fmt = img.format
        if not fmt:
            fmt = 'PNG' if ext == '.png' else 'JPEG'
        if fmt.upper() == 'JPEG' and img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        img.save(file_path, format=fmt)
    except Exception:
        with open(file_path, 'wb') as f:
            f.write(image_bytes)


def _apply_watermark(base_img, logo_img, position: str, scale_pct: int, opacity: float, margin: int):
    from PIL import Image
    # Keep original aspect ratio and place logo at chosen corner.
    base = base_img.convert("RGBA")
    logo = logo_img.convert("RGBA")
    bw, bh = base.size
    if bw <= 0 or bh <= 0:
        return base
    target_w = max(int(bw * (scale_pct / 100.0)), 1)
    lw, lh = logo.size
    if lw > 0:
        target_h = max(int(lh * (target_w / lw)), 1)
    else:
        target_h = max(int(bh * (scale_pct / 100.0)), 1)
    logo = logo.resize((target_w, target_h), Image.LANCZOS)
    if opacity < 1.0:
        alpha = logo.split()[-1]
        alpha = alpha.point(lambda p: int(p * opacity))
        logo.putalpha(alpha)

    x = margin
    y = margin
    if position == "top-right":
        x = max(bw - target_w - margin, margin)
        y = margin
    elif position == "bottom-left":
        x = margin
        y = max(bh - target_h - margin, margin)
    elif position == "bottom-right":
        x = max(bw - target_w - margin, margin)
        y = max(bh - target_h - margin, margin)
    elif position == "center":
        x = max(int((bw - target_w) / 2), margin)
        y = max(int((bh - target_h) / 2), margin)

    base.paste(logo, (x, y), logo)
    return base


def _has_watermark_marker(img) -> bool:
    info = getattr(img, "info", {}) or {}
    marker = None
    if isinstance(info, dict):
        marker = info.get("watermarked") or info.get("Watermarked") or info.get("comment")
    if isinstance(marker, bytes):
        try:
            marker = marker.decode("utf-8", errors="ignore")
        except Exception:
            marker = None
    if marker and "WATERMARKED=1" in str(marker):
        return True
    return False


def _add_watermark_marker(fmt: str, save_kwargs: dict):
    marker_text = "WATERMARKED=1"
    if fmt.upper() == "PNG":
        try:
            from PIL import PngImagePlugin
            pnginfo = PngImagePlugin.PngInfo()
            pnginfo.add_text("watermarked", marker_text)
            save_kwargs["pnginfo"] = pnginfo
        except Exception:
            pass
    else:
        save_kwargs["comment"] = marker_text.encode("utf-8")

def _unlock_playwright_profile(profile_dir: str):
    lock_files = [
        "SingletonLock",
        "SingletonCookie",
        "SingletonSocket",
        ".in_use.lock",
        "DevToolsActivePort",
    ]
    for fname in lock_files:
        fpath = os.path.join(profile_dir, fname)
        if os.path.exists(fpath):
            try:
                os.remove(fpath)
            except Exception:
                pass


def _find_playwright_chrome_exe():
    base = os.getenv("LOCALAPPDATA", "")
    if not base:
        return None
    root = os.path.join(base, "ms-playwright")
    if not os.path.isdir(root):
        return None
    candidates = []
    try:
        for name in os.listdir(root):
            if not name.startswith("chromium-"):
                continue
            # Check both chrome-win and chrome-win64
            for win_folder in ["chrome-win64", "chrome-win"]:
                exe = os.path.join(root, name, win_folder, "chrome.exe")
                if os.path.isfile(exe):
                    candidates.append(exe)
                    break
    except Exception:
        return None
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1]


def _open_chrome_profile(profile_dir: str, url: str):
    import subprocess
    exe = _find_playwright_chrome_exe()
    if not exe:
        raise RuntimeError("Cannot find Playwright Chromium executable")
    
    # Đảm bảo profile_dir là đường dẫn tuyệt đối
    profile_dir = os.path.abspath(profile_dir)
    
    args = [
        exe,
        f"--user-data-dir={profile_dir}",
        # Ẩn các dấu hiệu automation / bot detection
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--disable-automation",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-popup-blocking",
        # Thêm flag để tránh crash
        "--no-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        url or "about:blank",
    ]
    print(f"[Profile Manager] Opening Chrome with profile: {profile_dir}")
    print(f"[Profile Manager] Chrome exe: {exe}")
    print(f"[Profile Manager] Args: {args}")
    
    # Không ẩn output để debug được nếu có lỗi
    proc = subprocess.Popen(args)
    print(f"[Profile Manager] Chrome started with PID: {proc.pid}")


def _clear_profile_cache(profile_dir: str):
    import shutil
    cache_dirs = [
        "Cache",
        "Code Cache",
        "GPUCache",
        "ShaderCache",
        "Service Worker",
        "Crashpad",
        "BrowserMetrics",
        "Component Store",
    ]
    for name in cache_dirs:
        path = os.path.join(profile_dir, name)
        if os.path.isdir(path):
            try:
                shutil.rmtree(path)
            except Exception:
                pass
    try:
        for name in os.listdir(profile_dir):
            if name.endswith(".CHROME_DELETE"):
                path = os.path.join(profile_dir, name)
                if os.path.isdir(path):
                    try:
                        shutil.rmtree(path)
                    except Exception:
                        pass
    except Exception:
        pass
    lock_files = ["SingletonLock", "SingletonCookie", "SingletonSocket", ".in_use.lock"]
    for fname in lock_files:
        fpath = os.path.join(profile_dir, fname)
        if os.path.exists(fpath):
            try:
                os.remove(fpath)
            except Exception:
                pass


def _reset_profile_dir(profile_dir: str):
    import shutil
    if os.path.isdir(profile_dir):
        shutil.rmtree(profile_dir, ignore_errors=True)


async def _open_playwright_profile(profile_dir: str, url: str, wait_seconds: int, keep_open: bool):
    from playwright.async_api import async_playwright
    p = await async_playwright().start()
    try:
        ctx = await p.chromium.launch_persistent_context(
            profile_dir,
            headless=False,
            viewport={"width": 1366, "height": 768},
        )
        await asyncio.sleep(1)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded")
        
        if keep_open:
            # Wait until browser is closed manually by user
            try:
                while True:
                    await asyncio.sleep(2)
                    # Check if context is still connected
                    try:
                        _ = ctx.pages
                    except Exception:
                        break
            except Exception:
                pass
        else:
            await page.wait_for_timeout(max(wait_seconds, 5) * 1000)
            await ctx.close()
    finally:
        if not keep_open:
            await p.stop()


async def _open_nodriver_profile(profile_dir: str, url: str, wait_seconds: int, keep_open: bool):
    import nodriver as uc
    
    print(f"[Profile Manager] Opening nodriver profile: {profile_dir}")
    print(f"[Profile Manager] URL: {url}")
    
    browser = await uc.start(
        headless=False,
        user_data_dir=profile_dir
    )
    try:
        await browser.get(url)
        print(f"[Profile Manager] Browser started, waiting...")
        
        if keep_open:
            # Đợi cho đến khi user đóng browser thủ công
            # Kiểm tra mỗi 5 giây xem browser còn sống không
            try:
                while True:
                    await asyncio.sleep(5)
                    # Check if browser is still running bằng cách kiểm tra connection
                    try:
                        if browser and browser.connection:
                            # Chỉ kiểm tra connection còn sống, không navigate
                            pass
                        else:
                            print("[Profile Manager] Browser connection lost")
                            break
                    except Exception:
                        print("[Profile Manager] Browser closed by user")
                        break
            except Exception:
                pass
        else:
            await asyncio.sleep(max(wait_seconds, 5))
            try:
                await browser.stop()
            except Exception:
                pass
    except Exception as e:
        print(f"[Profile Manager] Error: {e}")
        if not keep_open:
            try:
                await browser.stop()
            except Exception:
                pass
        raise e

# City lookup helpers (transaction_city_new)
def _fetch_cities(db):
    def _safe_text(val):
        if val is None:
            return ""
        if isinstance(val, bytes):
            try:
                return val.decode("utf-8", errors="replace")
            except Exception:
                return str(val)
        return str(val)
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT old_city_id, old_city_name, new_city_id, new_city_name
            FROM transaction_city_merge
            WHERE old_city_parent_id = 0
            ORDER BY old_city_name
            """
        )
        rows = cursor.fetchall()
        return [
            {
                "old_city_id": row[0],
                "old_city_name": _safe_text(row[1]),
                "new_city_id": row[2],
                "new_city_name": _safe_text(row[3]),
            }
            for row in rows
            if row and row[0] is not None
        ]
    except Exception:
        return []
    finally:
        cursor.close()
        conn.close()


def _fetch_city_children(db, parent_id):
    if not parent_id:
        return []
    def _safe_text(val):
        if val is None:
            return ""
        if isinstance(val, bytes):
            try:
                return val.decode("utf-8", errors="replace")
            except Exception:
                return str(val)
        return str(val)
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT old_city_id, old_city_name, new_city_id, new_city_name
            FROM transaction_city_merge
            WHERE old_city_parent_id = %s
            ORDER BY old_city_name
            """,
            (parent_id,),
        )
        rows = cursor.fetchall()
        return [
            {
                "old_city_id": row[0],
                "old_city_name": _safe_text(row[1]),
                "new_city_id": row[2],
                "new_city_name": _safe_text(row[3]),
            }
            for row in rows
            if row and row[0] is not None
        ]
    except Exception:
        return []
    finally:
        cursor.close()
        conn.close()

# Add parent directory to path

sys.path.insert(0, str(Path(__file__).parent))

from web_scraper import WebScraper

from database import Database

from listing_crawler import crawl_listing
from listing_simple_core import crawl_listing_simple

import nodriver as uc



# Cấu hình tiết kiệm cho nodriver (chặn ảnh, tắt audio để giảm lag và tiết kiệm bandwidth)

BROWSER_CONFIG_TIET_KIEM = [

    "--blink-settings=imagesEnabled=false", 

    "--disable-images",

    "--mute-audio",

]



# Page config

st.set_page_config(

    page_title="Scraping Dashboard",

    page_icon="🕷️",

    layout="wide",

    initial_sidebar_state="expanded"

)



# Initialize session state

if 'results' not in st.session_state:

    st.session_state.results = []

if 'is_scraping' not in st.session_state:

    st.session_state.is_scraping = False

if 'current_progress' not in st.session_state:

    st.session_state.current_progress = 0

if 'total_urls' not in st.session_state:

    st.session_state.total_urls = 0

if 'scraping_status' not in st.session_state:

    st.session_state.scraping_status = ""

if 'template_data' not in st.session_state:

    st.session_state.template_data = None

if 'template_schema' not in st.session_state:

    st.session_state.template_schema = None





# KHÔNG chuyển đổi XPath sang CSS vì:

# - Nhiều div có cùng class structure

# - XPath filter theo text để tìm đúng div (ví dụ: "Khoảng giá")

# - CSS không thể filter theo text của sibling

# => Giữ nguyên XPath và test xem JsonCssExtractionStrategy có hỗ trợ XPath không





def convert_template_to_schema(template: Dict) -> Dict:

    """

    Convert extension template format to Crawl4AI schema format

    Similar to extension_api_server.py logic

    """

    base_selector = template.get('baseSelector', 'body')

    schema = {

        'name': template.get('name', 'ExtractedData'),

        'baseSelector': base_selector,

        'fields': []

    }

    

    for field in template.get('fields', []):

        # Ưu tiên CSS selector hơn XPath (vì CSS selector thường ổn định hơn)

        # JsonCssExtractionStrategy có thể không hỗ trợ XPath đầy đủ

        css_selector = field.get('cssSelector')

        selector = field.get('selector')  # Có thể là CSS hoặc XPath

        xpath_selector = field.get('xpath')

        

        # Kiểm tra xem selector có phải XPath không (bắt đầu với //)

        selector_is_xpath = selector and selector.strip().startswith('//')

        

        # Ưu tiên: cssSelector > selector (nếu là CSS) > xpath > selector (nếu là XPath, chuyển sang CSS)

        if css_selector:

            raw_selector = css_selector

            print(f"[convert_template] Field '{field.get('name')}': Dùng cssSelector: '{css_selector}'")

        elif selector and not selector_is_xpath:

            # selector là CSS selector

            raw_selector = selector

            print(f"[convert_template] Field '{field.get('name')}': Dùng selector (CSS): '{selector}'")

        elif xpath_selector:

            # Dùng xpath field riêng - GIỮ NGUYÊN XPath (không chuyển sang CSS)

            raw_selector = xpath_selector

            print(f"[convert_template] Field '{field.get('name')}': Dùng xpath (GIỮ NGUYÊN XPath): '{xpath_selector[:80]}...'")

            print(f"[convert_template] Lưu ý: Không chuyển XPath sang CSS vì cần filter theo text để tìm đúng element")

        elif selector and selector_is_xpath:

            # selector là XPath - GIỮ NGUYÊN XPath (không chuyển sang CSS)

            raw_selector = selector

            print(f"[convert_template] Field '{field.get('name')}': Dùng selector (XPath, GIỮ NGUYÊN): '{selector[:80]}...'")

            print(f"[convert_template] Lưu ý: Không chuyển XPath sang CSS vì cần filter theo text để tìm đúng element")

        else:

            print(f"[WARNING] Field '{field.get('name')}': Không có selector nào. Bỏ qua field này.")

            continue

        

        if not raw_selector:

            continue

        

        value_type = field.get('valueType') or field.get('type', 'text')

        field_name = field.get('name', '')

        

        field_config = {

            'name': field_name,

            'selector': raw_selector,

            'type': 'text'

        }

        

        # Handle different value types

        if value_type in ['src', 'href', 'alt', 'title', 'data-id', 'data-phone']:

            if value_type == 'src':

                # Handle images with lazy loading support

                selector_lower = raw_selector.lower()

                img_selector = raw_selector

                if 'img' not in selector_lower and not raw_selector.startswith('//'):

                    img_selector = f"{raw_selector} img"

                elif raw_selector.startswith('//'):

                    img_selector = f"{raw_selector}//img"

                

                field_config = {

                    'name': field_name,

                    'selector': img_selector,

                    'type': 'list',

                    'fields': [{

                        'name': 'data_src',

                        'type': 'attribute',

                        'attribute': 'data-src'

                    }, {

                        'name': 'src',

                        'type': 'attribute',

                        'attribute': 'src'

                    }, {

                        'name': 'data_lazy_src',

                        'type': 'attribute',

                        'attribute': 'data-lazy-src'

                    }]

                }

            else:

                # Other attributes

                field_config['type'] = 'attribute'

                field_config['attribute'] = value_type

        elif value_type == 'html':

            field_config['type'] = 'html'

        elif value_type == 'all' or value_type == 'container':

            # Container extraction with itemprop

            if raw_selector.startswith('//'):

                container_selector = raw_selector

                if 'strong' not in raw_selector.lower():

                    container_selector = f"{raw_selector}//strong[@itemprop]"

                else:

                    if not raw_selector.endswith(']') or '@itemprop' not in raw_selector:

                        container_selector = f"{raw_selector}[@itemprop]"

                

                field_config = {

                    'name': field_name,

                    'selector': container_selector,

                    'type': 'list',

                    'fields': [{

                        'name': 'value',

                        'type': 'text'

                    }, {

                        'name': 'itemprop',

                        'type': 'attribute',

                        'attribute': 'itemprop'

                    }]

                }

            else:

                container_selector = raw_selector

                if 'strong' not in raw_selector.lower():

                    container_selector = f"{raw_selector} strong[itemprop]"

                

                field_config = {

                    'name': field_name,

                    'selector': container_selector,

                    'type': 'list',

                    'fields': [{

                        'name': 'value',

                        'type': 'text'

                    }, {

                        'name': 'itemprop',

                        'type': 'attribute',

                        'attribute': 'itemprop'

                    }]

                }

        

        schema['fields'].append(field_config)

    

    return schema





def format_extracted_data(extracted_data: Any, template: Dict, markdown: str = None) -> Dict:

    """

    Format extracted data similar to extension_api_server.py

    Handle images, containers, textContent fallback, etc.

    """

    if not isinstance(extracted_data, dict):

        return {}

    

    formatted_data = {}

    

    for field in template.get('fields', []):

        field_name = field.get('name', '')

        text_content = field.get('textContent', '')

        value_type = field.get('valueType') or field.get('type', 'text')

        

        # Check if textContent is binary data

        text_content_is_binary = False

        if text_content:

            text_lower = text_content.lower()

            if ('jfif' in text_lower[:500] or 

                'png' in text_lower[:500] or 

                '\xff\xd8' in text_content[:20] or 

                len(text_content) > 5000):

                text_content_is_binary = True

                text_content = None

        

        if field_name in extracted_data:

            value = extracted_data[field_name]

            

            # Handle container/all type

            if value_type == 'all' or value_type == 'container':

                if isinstance(value, list):

                    container_dict = {}

                    for item in value:

                        if isinstance(item, dict):

                            itemprop = item.get('itemprop', '')

                            text_value = item.get('value', '')

                            if itemprop and text_value:

                                container_dict[itemprop] = text_value.strip()

                    if container_dict:

                        formatted_data[field_name] = container_dict

                        continue

                # Fallback to textContent if not list

                if text_content and not text_content_is_binary and text_content.strip():

                    formatted_data[field_name] = text_content.strip()

                    continue

                formatted_data[field_name] = value if value else None

                continue

            

            # Handle src and href (images and links)

            if value_type in ['src', 'href']:

                is_binary = False

                

                # Check if value is bytes

                if isinstance(value, bytes):

                    is_binary = True

                    value = None

                elif isinstance(value, str):

                    value_preview = value[:500].lower() if len(value) > 500 else value.lower()

                    value_start = value[:20] if len(value) > 20 else value

                    

                    # Check for binary markers

                    if ('jfif' in value_preview or 

                        'png' in value_preview or

                        '\xff\xd8' in value_start or

                        value_start.startswith('\x89PNG') or

                        '\x10JFIF' in value_start or

                        len(value) > 5000 or

                        (not value.startswith('http') and not value.startswith('//') and not value.startswith('/') and len(value) > 200)):

                        is_binary = True

                        value = None

                

                # If value is list (for images with lazy loading)

                if isinstance(value, list):

                    filtered_values = []

                    for v in value:

                        original_v = v

                        if isinstance(v, dict):

                            # Priority: data-src > data-lazy-src > src > url > href

                            v = (v.get('data_src') or 

                                 v.get('data-src') or

                                 v.get('data_lazy_src') or 

                                 v.get('data-lazy-src') or

                                 v.get('src') or 

                                 v.get('url') or

                                 v.get('href') or 

                                 v.get(field_name))

                            # If still None, try first non-None value in dict

                            if v is None:

                                for key, val in original_v.items():

                                    if val is not None and isinstance(val, str):

                                        v = val

                                        break

                        

                        if v is None:

                            continue

                        

                        if isinstance(v, bytes):

                            continue

                        elif isinstance(v, str):

                            v_preview = v[:500].lower() if len(v) > 500 else v.lower()

                            v_start = v[:20] if len(v) > 20 else v

                            

                            # Skip binary data

                            if ('jfif' in v_preview or 

                                'png' in v_preview or

                                '\xff\xd8' in v_start or

                                v_start.startswith('\x89PNG') or

                                '\x10JFIF' in v_start or

                                len(v) > 5000):

                                continue

                            

                            # Skip SVG files

                            v_lower = v.lower()

                            if ('.svg' in v_lower or 

                                v.startswith('data:image/svg+xml') or

                                v.endswith('.svg')):

                                continue

                            

                            # Skip placeholder/empty images

                            if ('img_empty' in v_lower or 

                                '/user/assets/img/img_empty' in v_lower or

                                'placeholder' in v_lower or

                                'empty.jpg' in v_lower or

                                'empty.png' in v_lower or

                                'no-image' in v_lower or

                                'noimage' in v_lower or

                                'default-image' in v_lower):

                                continue

                            

                            # Only keep URLs

                            if v.startswith('http') or v.startswith('//') or v.startswith('/'):

                                filtered_values.append(v)

                    

                    if filtered_values:

                        filtered_values = _apply_exclude_words(filtered_values, field)
                        formatted_data[field_name] = filtered_values

                        continue

                    else:

                        # No valid URLs found, try markdown fallback

                        value = None

                        is_binary = True

                

                # If binary or no valid URL, try to extract from markdown

                if is_binary or not value or (isinstance(value, str) and not value.startswith('http') and not value.startswith('//') and not value.startswith('/')):

                    if markdown:

                        import re

                        img_pattern = r'!\[.*?\]\((https?://[^\s\)]+)'

                        all_image_urls = re.findall(img_pattern, markdown)

                        unique_images = list(dict.fromkeys(all_image_urls))

                        # Filter out SVG files

                        unique_images = [url for url in unique_images 

                                        if '.svg' not in url.lower() 

                                        and not url.lower().endswith('.svg')]

                        if unique_images:

                            unique_images = _apply_exclude_words(unique_images, field)
                            formatted_data[field_name] = unique_images

                            continue

                

                # Skip if still binary or invalid

                if is_binary or not value:

                    continue

                

                # Compare textContent with value

                if text_content and text_content.strip():

                    text_normalized = text_content.strip().lower()

                    value_normalized = str(value).strip().lower() if value else ''

                    

                    if text_normalized != value_normalized:

                        import re

                        is_markdown = bool(re.search(r'\[([^\]]+)\]\([^\)]+\)', str(value))) or any(p in str(value) for p in ['](http', '- Ảnh', '!['])

                        text_len = len(text_content.strip())

                        value_len = len(str(value).strip()) if value else 0

                        

                        if is_markdown or (text_len > value_len * 2 and value_len < 100) or (text_len > 200 and value_len < 50):

                            formatted_data[field_name] = text_content.strip()

                            continue

                

                if not value or (isinstance(value, str) and not value.strip()):

                    # Value rỗng → để None (không dùng textContent fallback)

                    print(f"[WARNING] Field '{field_name}' có value rỗng. Để None.")

                    formatted_data[field_name] = None

                    continue

                

                # Final check: Don't add binary data

                if isinstance(value, str):

                    value_lower = value.lower()

                    if 'jfif' in value_lower[:1000] or 'png' in value_lower[:1000] or len(value) > 5000:

                        # Skip binary, use textContent or markdown fallback

                        if text_content and text_content.strip():

                            formatted_data[field_name] = text_content.strip()

                        elif markdown:

                            import re

                            img_pattern = r'!\[.*?\]\((https?://[^\s\)]+)'

                            all_image_urls = re.findall(img_pattern, markdown)

                            unique_images = list(dict.fromkeys(all_image_urls))

                            unique_images = [url for url in unique_images 

                                            if '.svg' not in url.lower() 

                                            and not url.lower().endswith('.svg')]

                            if unique_images:

                                unique_images = _apply_exclude_words(unique_images, field)
                                formatted_data[field_name] = unique_images

                        continue

                

                value = _apply_exclude_words(value, field)
                formatted_data[field_name] = value

            else:

                # Handle other value types (text, html, alt, title, data-id, etc.)

                # Compare textContent with value

                if text_content and text_content.strip():

                    text_normalized = text_content.strip().lower()

                    value_normalized = str(value).strip().lower() if value else ''

                    

                    if text_normalized != value_normalized:

                        import re

                        is_markdown = bool(re.search(r'\[([^\]]+)\]\([^\)]+\)', str(value))) or any(p in str(value) for p in ['](http', '- Ảnh', '!['])

                        text_len = len(text_content.strip())

                        value_len = len(str(value).strip()) if value else 0

                        

                        if is_markdown or (text_len > value_len * 2 and value_len < 100) or (text_len > 200 and value_len < 50):

                            formatted_data[field_name] = text_content.strip()

                            continue

                

                if not value or (isinstance(value, str) and not value.strip()):

                    # Value rỗng → để None (không dùng textContent fallback)

                    print(f"[WARNING] Field '{field_name}' có value rỗng. Để None.")

                    formatted_data[field_name] = None

                    continue

                

                value = _apply_exclude_words(value, field)
                formatted_data[field_name] = value

        else:

            # Field not found in extracted_data - để None (không dùng textContent fallback)

            print(f"[WARNING] Field '{field_name}' không tìm thấy trong extracted_data. Để None.")

            formatted_data[field_name] = None

    

    return formatted_data





async def scrape_url(

    url: str,

    schema: Dict,

    template: Dict,

    scraper: Optional[WebScraper] = None,

    wait_load_min: float = 0.0,

    wait_load_max: float = 0.0,

    show_browser: bool = True,

) -> Dict[str, Any]:

    """

    Hàm cào dữ liệu sử dụng lxml trực tiếp để hỗ trợ XPath tối đa.

    Đã tối ưu: Lấy TẤT CẢ ảnh trong khung (container) thay vì chỉ 1 cái.

    """

    try:

        import random

        # 1. Chờ ngẫu nhiên trước khi tải HTML (detail)

        if wait_load_max and wait_load_max > 0:

            wait_time = random.uniform(wait_load_min or 0.0, wait_load_max)

            if wait_time > 0:

                await asyncio.sleep(wait_time)




        # 2. Ch?% t?oi HTML v? (KhA'ng nh? Crawl4AI extract schema n?_a)
        tree_before = None
        phone_selector = None
        phone_override = None
        if scraper:
            # Neu co display_page va dang hien browser, dung tab do de goto + lay HTML
            if show_browser and hasattr(scraper, 'display_page') and scraper.display_page:
                page = scraper.display_page
                page_ready_url = getattr(scraper, "_page_ready_url", None)
                reuse_page = bool(page_ready_url and page_ready_url == url)
                try:
                    if reuse_page:
                        try:
                            if hasattr(page, "is_closed") and page.is_closed():
                                reuse_page = False
                        except Exception:
                            reuse_page = False
                    if reuse_page:
                        if _is_target_phone_domain(url):
                            phone_selector = _get_phone_selector(template)
                            if phone_selector:
                                try:
                                    html_before_click = await page.content()
                                    if html_before_click:
                                        tree_before = lxml_html.fromstring(html_before_click)
                                except Exception:
                                    tree_before = None
                        try:
                            await _reveal_phone_before_extract(page, url, template)
                        except Exception:
                            pass
                        if phone_selector:
                            try:
                                phone_override = await _get_phone_text_from_page(page, phone_selector)
                                phone_override = _clean_phone_text(phone_override)
                                if _is_masked_phone(phone_override):
                                    phone_override = None
                            except Exception:
                                phone_override = None
                        html_content = await page.content()
                        result = {'success': True, 'html': html_content}
                    else:
                        await page.goto(url)
                        await asyncio.sleep(1)
                        if _is_target_phone_domain(url):
                            phone_selector = _get_phone_selector(template)
                            if phone_selector:
                                try:
                                    html_before_click = await page.content()
                                    if html_before_click:
                                        tree_before = lxml_html.fromstring(html_before_click)
                                except Exception:
                                    tree_before = None
                        try:
                            await _reveal_phone_before_extract(page, url, template)
                        except Exception:
                            pass
                        if phone_selector:
                            try:
                                phone_override = await _get_phone_text_from_page(page, phone_selector)
                                phone_override = _clean_phone_text(phone_override)
                                if _is_masked_phone(phone_override):
                                    phone_override = None
                            except Exception:
                                phone_override = None
                        html_content = await page.content()
                        result = {'success': True, 'html': html_content}
                except Exception as e:
                    print(f"[detail scrape_url] display_page goto fail: {e}")
                    result = await scraper.scrape_simple(url, bypass_cache=True)
                finally:
                    if page_ready_url and page_ready_url == url:
                        try:
                            scraper._page_ready_url = None
                        except Exception:
                            pass
            else:
                if show_browser:
                    print("[detail scrape_url] display_page is None, fallback to scrape_simple")
                result = await scraper.scrape_simple(url, bypass_cache=True)
        else:
            async with WebScraper(headless=not show_browser, verbose=False) as new_scraper:
                result = await new_scraper.scrape_simple(url, bypass_cache=True)

        

        if not result.get('success'):

            return {'success': False, 'url': url, 'error': result.get('error'), 'timestamp': datetime.now().isoformat()}



        # 2. Dùng lxml để xử lý XPath/CSS "thủ công" nhưng chính xác

        html_content = result.get('html', '')

        if not html_content:

            return {'success': False, 'url': url, 'error': 'Empty HTML', 'timestamp': datetime.now().isoformat()}



        tree = lxml_html.fromstring(html_content)

        extracted_data = {}



        for field in template.get('fields', []):

            field_name = field.get('name')

            selector = field.get('selector', '').strip()

            value_type = field.get('valueType', 'text')

            

            if not selector:

                extracted_data[field_name] = None

                continue



            try:

                elements = []

                # Tự động nhận diện XPath (bắt đầu bằng / hoặc () )

                if selector.startswith('/') or selector.startswith('('):

                    elements = tree.xpath(selector)

                else:

                    # Nếu là CSS, chuyển sang dùng cssselect

                    elements = tree.cssselect(selector)



                # --- ĐOẠN CODE TỐI ƯU MỚI: QUÉT SẠCH ẢNH/LINK ---

                values = []

                def _add_value(val):
                    if val and val not in values:
                        values.append(val)

                for el in elements:

                    # 1. Xử lý lấy ẢNH (src)

                    if value_type == 'src':
                        # Support background-image in style attribute
                        if hasattr(el, 'get'):
                            style_val = el.get('style') or ''
                            if 'background-image' in style_val:
                                m = re.search(r'url\((["\']?)(.*?)\1\)', style_val, re.IGNORECASE)
                                if m and m.group(2):
                                    _add_value(m.group(2))

                        # Nếu bản thân nó là thẻ img -> Lấy luôn

                        if hasattr(el, 'tag') and el.tag in ('img', 'video', 'source', 'iframe'):

                            val = el.get('data-src') or el.get('data-lazy-src') or el.get('src')

                            _add_value(val)
                            # If selector targets img, try sibling video/iframe in the same parent.
                            if el.tag == 'img' and hasattr(el, 'getparent'):
                                parent = el.getparent()
                                if parent is not None:
                                    for media in parent.xpath('.//video|.//source|.//iframe'):
                                        val = media.get('data-src') or media.get('data-lazy-src') or media.get('src')
                                        _add_value(val)
                                    # Also try to grab media from nearby carousel/gallery container.
                                    ancestor = parent
                                    for _ in range(5):
                                        if ancestor is None:
                                            break
                                        class_attr = ancestor.get('class')
                                        if isinstance(class_attr, (list, tuple)):
                                            class_str = ' '.join(class_attr)
                                        else:
                                            class_str = class_attr or ''
                                        id_str = ancestor.get('id') or ''
                                        if re.search(r'(carousel|gallery|photos)', f"{class_str} {id_str}", re.I):
                                            if re.search(r'carousel-item', class_str, re.I):
                                                ancestor = ancestor.getparent()
                                                continue
                                            for media in ancestor.xpath('.//video|.//source|.//iframe'):
                                                val = media.get('data-src') or media.get('data-lazy-src') or media.get('src')
                                                _add_value(val)
                                            break
                                        ancestor = ancestor.getparent()

                        

                        # Nếu nó là thẻ Div/Span/Khung -> Chui vào tìm TẤT CẢ thẻ img con

                        elif hasattr(el, 'xpath'):

                            child_media = el.xpath('.//img|.//video|.//source|.//iframe')

                            for media in child_media:

                                val = media.get('data-src') or media.get('data-lazy-src') or media.get('src')

                                _add_value(val)
                            # Also check any element with inline background-image
                            for node in el.xpath('.//*[@style]'):
                                style_val = node.get('style') or ''
                                if 'background-image' in style_val:
                                    m = re.search(r'url\((["\']?)(.*?)\1\)', style_val, re.IGNORECASE)
                                    if m and m.group(2):
                                        _add_value(m.group(2))



                    # 2. Xử lý lấy LINK (href)

                    elif value_type == 'href':

                        if hasattr(el, 'tag') and el.tag == 'a':

                            val = el.get('href')

                            _add_value(val)

                        elif hasattr(el, 'xpath'):

                            child_as = el.xpath('.//a')

                            for a in child_as:

                                val = a.get('href')

                                _add_value(val)



                    # 3. Xử lý TEXT (Mặc định) + iframe map lat,lng
                    # 2b. Xu ly attribute (data-*)
                    elif value_type in ['data-id', 'data-phone']:
                        if hasattr(el, 'get'):
                            val = el.get(value_type)
                            _add_value(val)
                        elif hasattr(el, 'xpath'):
                            # Fallback: tim attribute trong con
                            attr_name = value_type.replace('data-', '')
                            child_nodes = el.xpath(f'.//*[@data-{attr_name}]')
                            for child in child_nodes:
                                val = child.get(value_type)
                                _add_value(val)

                    elif value_type == 'html':
                        val = _get_inner_html(el)
                        _add_value(val)
                    else:
                        val = None
                        if field_name and field_name.strip().lower() == 'sodienthoai' and hasattr(el, 'get'):
                            ng_bind = el.get('ng-bind')
                            ng_val = _extract_phone_from_ng_bind(ng_bind)
                            if ng_val:
                                val = ng_val
                        # Nếu là iframe, thử lấy lat,lng từ src/data-src/data-lat/data-lng
                        if hasattr(el, 'tag') and el.tag == 'iframe':
                            src = el.get('src') or el.get('data-src')
                            coord = parse_latlng_from_url(src)
                            if coord:
                                val = coord
                            else:
                                dlat = el.get('data-lat')
                                dlng = el.get('data-lng')
                                if dlat and dlng:
                                    val = f"{dlat},{dlng}"
                                elif src:
                                    val = src
                        # Fallback text
                        if not val:
                            val = el if isinstance(el, str) else el.text_content().strip()
                        if val:
                            values.append(val)
                # ------------------------------------------------



                # Gán kết quả: Ảnh/Link lấy cả list, Text lấy cái đầu tiên

                if not values:
                    if field_name and field_name.strip().lower() == 'sodienthoai':
                        if phone_override:
                            extracted_data[field_name] = phone_override
                        elif tree_before is not None and phone_selector:
                            fallback_val = _extract_text_from_tree(tree_before, phone_selector)
                            fallback_val = _clean_phone_text(fallback_val)
                            extracted_data[field_name] = fallback_val if fallback_val else None
                        else:
                            extracted_data[field_name] = None
                    else:
                        extracted_data[field_name] = None

                elif value_type in ['src', 'href']:

                     extracted_data[field_name] = values # <--- Lấy trọn bộ list ảnh

                else:
                    if field_name and field_name.strip().lower() == 'sodienthoai':
                        cleaned_values = []
                        for v in values:
                            cleaned = _clean_phone_text(v)
                            if cleaned:
                                cleaned_values.append(cleaned)
                        chosen = next((v for v in cleaned_values if not _is_masked_phone(v)), None)
                        if phone_override:
                            extracted_data[field_name] = phone_override
                        elif chosen:
                            extracted_data[field_name] = chosen
                        elif cleaned_values:
                            extracted_data[field_name] = cleaned_values[0]
                        else:
                            extracted_data[field_name] = values[0]
                    else:
                        extracted_data[field_name] = values[0] # <--- Text chỉ lấy 1 cái đầu



            except Exception as e:

                print(f"❌ Lỗi extract field '{field_name}': {e}")

                extracted_data[field_name] = None



        # 3. Format dữ liệu

        formatted_data = format_extracted_data_fixed(extracted_data, template)



        return {

            'success': True,

            'url': url,

            'data': formatted_data,

            'timestamp': datetime.now().isoformat()

        }



    except Exception as e:

        return {'success': False, 'url': url, 'error': str(e), 'timestamp': datetime.now().isoformat()}



        # 3. Format dữ liệu (Bỏ logic tự điền textContent)

        formatted_data = format_extracted_data_fixed(extracted_data, template)



        return {

            'success': True,

            'url': url,

            'data': formatted_data,

            'timestamp': datetime.now().isoformat()

        }



    except Exception as e:

        return {'success': False, 'url': url, 'error': str(e), 'timestamp': datetime.now().isoformat()}



def format_extracted_data_fixed(extracted_data: Any, template: Dict) -> Dict:

    """

    Format dữ liệu: Trung thực, có sao nói vậy, không tự bịa số liệu.

    """

    if not isinstance(extracted_data, dict): return {}

    formatted = {}

    

    for field in template.get('fields', []):

        name = field.get('name')

        val = extracted_data.get(name)

        

        # Nếu có dữ liệu thì lấy, không có thì để None (Tuyệt đối không lấy textContent)

        value_type = field.get('valueType') or field.get('type', 'text')
        val = _apply_exclude_words(val, field)
        formatted[name] = val if val else None

        

    return formatted

async def scrape_bulk(urls: List[str], schema: Dict, template: Dict, progress_callback=None):

    """

    Scrape multiple URLs sequentially

    """

    results = []

    total = len(urls)

    

    # Giữ browser mở sau khi hoàn thành để người dùng quan sát

    async with WebScraper(headless=not show_browser, verbose=False, keep_open=True) as scraper:

        for i, url in enumerate(urls, 1):

            if progress_callback:

                progress_callback(i, total, f"Scraping {i}/{total}: {url[:50]}...")

            

            result = await scrape_url(url.strip(), schema, template, scraper)

            results.append(result)

            

            # Small delay between requests

            await asyncio.sleep(0.5)

    

    return results





# UI Components

st.title("🕷️ Web Scraping Dashboard")

st.markdown("---")



# Create tabs
tab3, tab4, tab5, tab6, tab7 = st.tabs(
    ["?? Crawl Listing", "?? Download Images", "Auto Schedule", "??? Watermark", "Profile Manager"]
)


# ============================================

with tab3:

    st.header("🔗 Crawl Listing (Simple)")

    st.markdown("Crawl listing pages using nodriver with simple selectors.")

    

    # Initialize separate database for this tab (MySQL - cùng database)

    if 'db_crawl_listing' not in st.session_state:

        st.session_state.db_crawl_listing = Database(

            host="localhost",

            user="root",

            password="",

            database="craw_db"

        )

    

    # Default values from testvip.py

    DEFAULT_URL = "https://batdongsan.com.vn/nha-dat-ban"

    DEFAULT_ITEM_SELECTOR = ".js__product-link-for-product-id"

    DEFAULT_NEXT_SELECTOR = ".re__pagination-icon > .re__icon-chevron-right--sm"

    

    # Input form

    st.subheader("📝 Configuration")

    

    # Template uploader (Listing Template)

    listing_template_file = st.file_uploader(

        "📄 Upload Listing Template (JSON) - Optional",

        type=['json'],

        help="Upload listing template JSON file từ Extension (Listing Mode). Nếu có template, selectors sẽ được tự động lấy từ template.",

        key="listing_template_uploader_tab3"

    )

    

    # Initialize session state for template selectors

    if 'template_item_selector' not in st.session_state:

        st.session_state.template_item_selector = None

    if 'template_next_selector' not in st.session_state:

        st.session_state.template_next_selector = None

    

    listing_template_data = None

    item_selector_from_template = None

    next_selector_from_template = None

    

    if listing_template_file is not None:

        try:

            listing_template_data = json.load(listing_template_file)

            

            # Validate template

            if listing_template_data.get('type') != 'listing':

                st.warning("⚠️ This doesn't look like a Listing Template. Make sure it has 'type': 'listing'")

            else:

                st.success("✅ Listing Template loaded!")

                

                # Lấy selectors từ template và lưu vào session state

                item_selector_from_template = listing_template_data.get('itemSelector', '')

                next_selector_from_template = listing_template_data.get('nextPageSelector', '')

                

                # Lưu vào session state để dùng cho input fields

                st.session_state.template_item_selector = item_selector_from_template

                st.session_state.template_next_selector = next_selector_from_template

                

                # Set trực tiếp vào session state của input fields để force update

                if item_selector_from_template:

                    st.session_state.crawl_listing_item_selector = item_selector_from_template

                if next_selector_from_template:

                    st.session_state.crawl_listing_next_selector = next_selector_from_template

                

                # Hiển thị thông tin template

                st.info("💡 Selectors đã được tự động điền vào các ô bên dưới. Bạn có thể chỉnh sửa nếu cần.")

                col1, col2 = st.columns(2)

                with col1:

                    st.write(f"**Item Selector:** `{item_selector_from_template or 'N/A'}`")

                with col2:

                    st.write(f"**Next Page Selector:** `{next_selector_from_template or 'N/A'}`")

        except json.JSONDecodeError:

            st.error("❌ Invalid JSON file!")

        except Exception as e:

            st.error(f"❌ Error loading template: {str(e)}")

    

    # Sử dụng session state nếu có, nếu không dùng giá trị từ template hoặc default

    item_selector_from_template = st.session_state.template_item_selector

    next_selector_from_template = st.session_state.template_next_selector

    

    st.markdown("---")

    

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:

        target_url = st.text_input(

            "🌐 Target URL",

            value=DEFAULT_URL,

            help="URL của trang listing cần crawl",

            key="crawl_listing_url"

        )

    

    with col2:

        max_pages = st.number_input(

            "📄 Max Pages",

            min_value=1,

            max_value=100,

            value=3,

            help="Số trang tối đa để crawl",

            key="crawl_listing_max_pages"

        )

    

    with col3:

        domain = st.text_input(

            "🏷️ Domain label (vd: batdongsan, nhatot)",

            value="batdongsan",

            help="Lưu kèm nhãn domain cho links thu thập",

            key="crawl_listing_domain"

        )

    loaihinh_cho_thue = {
        'Nhà phố',
        'Nhà riêng',
        'Biệt thự',
        'Căn hộ chung cư',
        'Văn phòng',
        'Mặt bằng',
        'Nhà hàng - Khách sạn',
        'Nhà Kho - Xưởng',
        'Phòng trọ',
        'Đất khu công nghiệp',
    }
    loaihinh_options = [
        'Ban nha rieng',
        'Ban nha pho du an',
        'Ban biet thu',
        'Ban can ho chung cu',
        'Ban can ho Mini, Dich vu',
        'Ban dat nen du an',
        'Ban dat tho cu',
        'Ban dat nong, lam nghiep',
        'Ban nha hang - Khach san',
        'Ban kho, nha xuong',
        'Dự án',
        'Nhà phố',
        'Nhà riêng',
        'Biệt thự',
        'Căn hộ chung cư',
        'Văn phòng',
        'Mặt bằng',
        'Nhà hàng - Khách sạn',
        'Nhà Kho - Xưởng',
        'Phòng trọ',
        'Đất khu công nghiệp',
    ]
    loaihinh = st.selectbox(
        'Loai hinh',
        options=loaihinh_options,
        index=3,
        help='Chon loai hinh khi luu links',
        key='crawl_listing_loaihinh',
        format_func=lambda x: f"{x} (cho thuê)" if x in loaihinh_cho_thue else x,
    )

    # Province/ward selection
    city_options = _fetch_cities(st.session_state.db_crawl_listing)
    city_choices = [(None, "(Tat ca)", None, None)] + [
        (c["old_city_id"], c["old_city_name"], c["new_city_id"], c["new_city_name"])
        for c in city_options
    ]
    selected_city = st.selectbox(
        "Tinh/TP",
        options=city_choices,
        format_func=lambda x: x[1] if isinstance(x, (list, tuple)) else x,
        key="crawl_listing_city",
    )
    city_id = selected_city[0] if isinstance(selected_city, (list, tuple)) and selected_city[0] else None
    city_name = selected_city[1] if isinstance(selected_city, (list, tuple)) and selected_city[0] else None
    new_city_id = selected_city[2] if isinstance(selected_city, (list, tuple)) and selected_city[0] else None
    new_city_name = selected_city[3] if isinstance(selected_city, (list, tuple)) and selected_city[0] else None

    ward_options = _fetch_city_children(st.session_state.db_crawl_listing, city_id)
    ward_choices = [(None, "(Tat ca)", None, None)] + [
        (c["old_city_id"], c["old_city_name"], c["new_city_id"], c["new_city_name"])
        for c in ward_options
    ]
    selected_ward = st.selectbox(
        "Huyen/Xa",
        options=ward_choices,
        format_func=lambda x: x[1] if isinstance(x, (list, tuple)) else x,
        key="crawl_listing_ward",
    )
    ward_id = selected_ward[0] if isinstance(selected_ward, (list, tuple)) and selected_ward[0] else None
    ward_name = selected_ward[1] if isinstance(selected_ward, (list, tuple)) and selected_ward[0] else None
    new_ward_id = selected_ward[2] if isinstance(selected_ward, (list, tuple)) and selected_ward[0] else None
    new_ward_name = selected_ward[3] if isinstance(selected_ward, (list, tuple)) and selected_ward[0] else None


    

    st.markdown("---")

    st.subheader("🎯 Selectors")

    st.markdown("💡 Nếu đã upload template, selectors sẽ được tự động điền. Bạn có thể chỉnh sửa nếu cần.")

    

    col1, col2 = st.columns(2)

    with col1:

        # Kiểm tra session state của input field, nếu không có thì dùng từ template hoặc default

        if 'crawl_listing_item_selector' not in st.session_state:

            if st.session_state.template_item_selector:

                st.session_state.crawl_listing_item_selector = st.session_state.template_item_selector

            else:

                st.session_state.crawl_listing_item_selector = DEFAULT_ITEM_SELECTOR

        

        item_selector = st.text_input(

            "🔗 Item Link Selector",

            value=st.session_state.crawl_listing_item_selector,

            help="CSS selector để tìm các link items (ví dụ: .js__product-link-for-product-id)",

            key="crawl_listing_item_selector"

        )

    

    with col2:

        # Kiểm tra session state của input field, nếu không có thì dùng từ template hoặc default

        if 'crawl_listing_next_selector' not in st.session_state:

            if st.session_state.template_next_selector:

                st.session_state.crawl_listing_next_selector = st.session_state.template_next_selector

            else:

                st.session_state.crawl_listing_next_selector = DEFAULT_NEXT_SELECTOR

        

        next_selector = st.text_input(

            "➡️ Next Page Selector",

            value=st.session_state.crawl_listing_next_selector,

            help="CSS selector để tìm nút Next (ví dụ: .re__pagination-icon > .re__icon-chevron-right--sm)",

            key="crawl_listing_next_selector"

        )

    

    # Advanced options

    st.markdown("Cai dat nang cao")

    col_adv1, col_adv2, col_adv3 = st.columns(3)

    with col_adv1:

        show_browser = st.checkbox("Hien browser", value=True, help="An/hien trinh duyet khi crawl")

        enable_fake_scroll = st.checkbox("Cuon gia", value=True, help="Cuon trang gia truoc khi click next")

    with col_adv2:

        enable_fake_hover = st.checkbox("Re chuot gia", value=False, help="Re chuot ngau nhien tren trang")

        wait_load_min = st.number_input("Cho load toi thieu (s)", value=20, min_value=0, max_value=120)

        wait_load_max = st.number_input("Cho load toi da (s)", value=30, min_value=0, max_value=180)

    with col_adv3:

        wait_next_min = st.number_input("Cho truoc Next toi thieu (s)", value=10, min_value=0, max_value=120)

        wait_next_max = st.number_input("Cho truoc Next toi da (s)", value=20, min_value=0, max_value=180)



    # Preview

    with st.expander("Preview Configuration", expanded=False):

        st.json({

            "URL": target_url,

            "Item Selector": item_selector,

            "Next Selector": next_selector,

            "Max Pages": max_pages

        })

    

    st.markdown("---")

    

    # Start button

    start_crawl_btn = st.button(

        "🚀 Start Crawling",

        type="primary",

        use_container_width=True,

        key="start_crawl_listing_btn"

    )

    

    # Execute crawling

    if start_crawl_btn and target_url and item_selector:

        if not next_selector and max_pages > 1:

            st.warning("⚠️ Next selector is empty but max_pages > 1. Will only crawl first page.")

        

        st.session_state.crawl_listing_status = 'running'

        

        with st.status("🔄 Crawling listing pages...", expanded=True) as status:

            try:

                def update_progress(page_num, total_pages, status_text, links_count):

                    # Removed status.update() to prevent removeChild errors from Streamlit internal JS

                    # Progress is shown via session state only

                    pass

                

                # Run async crawl

                result = asyncio.run(

                    crawl_listing_simple(

                        target_url,

                        item_selector,

                        next_selector,

                        max_pages,

                        st.session_state.db_crawl_listing,

                        update_progress,

                        domain,
                        loaihinh,
                        city_id,
                        city_name,
                        ward_id,
                        ward_name,
                        new_city_id,
                        new_city_name,
                        new_ward_id,
                        new_ward_name,

                        show_browser,

                        enable_fake_scroll,

                        enable_fake_hover,

                        wait_load_min,

                        wait_load_max,

                        wait_next_min,

                        wait_next_max

                    )

                )

                

                # Store result

                st.session_state.last_crawl_listing_result = result

                

                if result.get('success'):

                    st.session_state.crawl_listing_status = 'completed'

                    try:

                        if status:

                            status.update(

                                label=f"✅ Crawling completed! Found {result.get('total_links', 0)} links",

                                state="complete"

                            )

                    except Exception:

                        pass

                else:

                    st.session_state.crawl_listing_status = 'error'

                    try:

                        if status:

                            status.update(

                                label=f"❌ Error: {result.get('error', 'Unknown error')}",

                                state="error"

                            )

                    except Exception:

                        pass

            

            except Exception as e:

                st.session_state.crawl_listing_status = 'error'

                try:

                    if status:

                        status.update(label=f"❌ Error: {str(e)}", state="error")

                except Exception:

                    pass

                import traceback

                st.code(traceback.format_exc())

        

        # Show results if completed

        if st.session_state.crawl_listing_status == 'completed':

            result = st.session_state.get('last_crawl_listing_result', {})

            if result:

                st.markdown("---")

                st.subheader("📊 Crawling Summary")

                col1, col2, col3 = st.columns(3)

                with col1:

                    st.metric("Pages Crawled", result.get('pages_crawled', 0))

                with col2:

                    st.metric("Total Links Found", result.get('total_links', 0))

                with col3:

                    st.metric("New Links Added", result.get('new_links_added', 0))

    

    # Display all links

    st.markdown("---")

    st.subheader("📋 Tất Cả Liên Kết Trong Database")

    

    # Loc theo domain de tranh cong don nham giua cac nguon

    domain_options = ["(Tat ca)"]

    try:

        conn = st.session_state.db_crawl_listing.get_connection()

        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT domain FROM collected_links ORDER BY domain")

        rows = cursor.fetchall()

        for row in rows:

            dom = row[0]

            if dom:

                domain_options.append(dom)

        cursor.close()

        conn.close()

    except Exception as e:

        st.warning(f"Khong lay duoc danh sach domain: {e}")



    selected_domain = st.selectbox(

        "Loc theo domain",

        domain_options,

        index=0,

        help="Chon domain de xem link theo nguon, tranh cong don tu cac lan crawl khac."

    )

    domain_filter = None if selected_domain == "(Tat ca)" else selected_domain



    # Lay links theo domain da chon (mac dinh: tat ca)

    # Loai hinh filter
    loaihinh_filter_options = ["(Tat ca)"] + loaihinh_options if 'loaihinh_options' in locals() else ["(Tat ca)"]
    loaihinh_filter = st.selectbox(
        "Loc theo loai hinh",
        loaihinh_filter_options,
        index=0,
        help="Chon loai hinh de loc links",
        format_func=lambda x: x if x == "(Tat ca)" else (f"{x} (cho thuê)" if x in loaihinh_cho_thue else x),
    )
    loaihinh_filter_val = None if loaihinh_filter == "(Tat ca)" else loaihinh_filter

    all_links = st.session_state.db_crawl_listing.get_recent_links(limit=100000, domain=domain_filter, loaihinh=loaihinh_filter_val)

    

    # Thống kê tổng quan

    if all_links:

        total_in_db = len(all_links)

        unique_urls = len(set(link['url'] for link in all_links))

        

        col1, col2, col3 = st.columns(3)

        with col1:

            st.metric("Total links (filter)", total_in_db)

        with col2:

            st.metric("Unique URLs", unique_urls)

        with col3:

            if total_in_db > unique_urls:

                st.metric("Duplicates", total_in_db - unique_urls)

            else:

                st.metric("No Duplicates", 0)





        # Nút xóa dữ liệu cũ và reset ID (optional)

        with st.expander("🔧 Quản lý Database", expanded=False):

            col1, col2 = st.columns(2)

            

            with col1:

                st.warning("⚠️ Xóa TẤT CẢ links trong database!")

                if st.button("🗑️ Xóa TẤT CẢ links", type="secondary", use_container_width=True):

                    try:

                        conn = st.session_state.db_crawl_listing.get_connection()

                        cursor = conn.cursor()

                        cursor.execute("DELETE FROM collected_links")

                        conn.commit()

                        conn.close()

                        st.success("✅ Đã xóa tất cả links!")

                        st.rerun()

                    except Exception as e:

                        st.error(f"❌ Lỗi khi xóa: {e}")

            

            with col2:

                st.info("🔄 Reset ID sequence để làm ID liên tục từ 1")

                if st.button("🔄 Reset ID Sequence", type="secondary", use_container_width=True):

                    try:

                        # Kiểm tra xem hàm có tồn tại không

                        if hasattr(st.session_state.db_crawl_listing, 'reset_id_sequence'):

                            st.session_state.db_crawl_listing.reset_id_sequence()

                            st.success("✅ Đã reset ID sequence! ID giờ sẽ liên tục từ 1.")

                            st.rerun()

                        else:

                            # Fallback: Reset thủ công

                            conn = st.session_state.db_crawl_listing.get_connection()

                            cursor = conn.cursor()

                            

                            # Tạo bảng tạm

                            cursor.execute('''

                                CREATE TABLE collected_links_new (

                                    id INTEGER PRIMARY KEY AUTOINCREMENT,

                                    url TEXT NOT NULL UNIQUE,

                                    status TEXT NOT NULL DEFAULT 'PENDING',

                                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

                                )

                            ''')

                            

                            # Copy dữ liệu

                            cursor.execute('''

                                INSERT INTO collected_links_new (url, status, created_at)

                                SELECT url, status, created_at

                                FROM collected_links

                                ORDER BY id

                            ''')

                            

                            # Xóa bảng cũ và đổi tên

                            cursor.execute('DROP TABLE collected_links')

                            cursor.execute('ALTER TABLE collected_links_new RENAME TO collected_links')

                            

                            # Tạo lại indexes

                            cursor.execute('''

                                CREATE INDEX IF NOT EXISTS idx_collected_links_url 

                                ON collected_links(url)

                            ''')

                            cursor.execute('''

                                CREATE INDEX IF NOT EXISTS idx_collected_links_status 

                                ON collected_links(status)

                            ''')

                            

                            conn.commit()

                            conn.close()

                            st.success("✅ Đã reset ID sequence! ID giờ sẽ liên tục từ 1.")

                            st.rerun()

                    except Exception as e:

                        st.error(f"❌ Lỗi khi reset ID: {e}")

                        import traceback

                        st.code(traceback.format_exc())

    

    # Hiển thị TẤT CẢ links (không giới hạn)

    if all_links:

        df_links = pd.DataFrame(all_links)

        if 'loaihinh' not in df_links.columns:
            df_links['loaihinh'] = None

        df_links['created_at'] = pd.to_datetime(df_links['created_at'])

        df_links = df_links.sort_values('id', ascending=False)  # Sort theo ID DESC

        

        # Bộ lọc domain

        domain_options_all = ['All'] + sorted([d for d in df_links['domain'].dropna().unique()])

        domain_filter_all = st.selectbox(

            "Lọc theo Domain",

            domain_options_all,

            key="link_domain_filter_all"

        )

        if domain_filter_all != 'All':

            df_links = df_links[df_links['domain'] == domain_filter_all]
        if loaihinh_filter_val:
            df_links = df_links[df_links['loaihinh'] == loaihinh_filter_val]

        

        st.dataframe(

            df_links,  # Hiển thị TẤT CẢ links, không giới hạn

            use_container_width=True,

            hide_index=True,

            column_config={

                'url': st.column_config.LinkColumn('URL', display_text='View'),

                'status': st.column_config.TextColumn('Status'),

                'domain': st.column_config.TextColumn('Domain'),
                'loaihinh': st.column_config.TextColumn('Loai hinh'),

                'created_at': st.column_config.DatetimeColumn('Created At')

            }

        )

    else:

        st.info("📭 No links collected yet. Start crawling to see links here.")

    

    # ============================================

    # Section 2: Scrape Detail Pages

    # ============================================

    st.markdown("---")

    st.subheader("🔍 Scrape Detail Pages")

    st.markdown("Cào dữ liệu từ các trang chi tiết đã thu thập bằng crawl4ai với template.")

    

    # Get all links for ID selection

    all_links = st.session_state.db_crawl_listing.get_recent_links(limit=10000)

    

    if all_links:

        # ID range selection

        col1, col2, col3 = st.columns(3)

        with col1:

            min_id = st.number_input(

                "🔢 ID Bắt Đầu",

                min_value=1,

                max_value=1000000,

                value=1,

                help="ID nhỏ nhất để cào",

                key="detail_min_id"

            )

        with col2:

            max_id = st.number_input(

                "🔢 ID Kết Thúc",

                min_value=1,

                max_value=1000000,

                value=100,

                help="ID lớn nhất để cào",

                key="detail_max_id"

            )

        with col3:

            delay_seconds = st.number_input(

                "⏱️ Delay (giây)",

                min_value=0.0,

                max_value=60.0,

                value=2.0,

                step=0.5,

                help="Thời gian chờ giữa mỗi link (để tránh bị check)",

                key="detail_delay"

            )



        # Cài đặt nâng cao (Detail) - tách riêng với Listing

        st.markdown("Cài đặt nâng cao (Detail)")

        dcol1, dcol2 = st.columns(2)

        with dcol1:

            detail_show_browser = st.checkbox("Trình duyệt hiện (Detail)", value=True, help="Ẩn/hiện trình duyệt khi scrape detail")

            detail_fake_hover = st.checkbox("Re chuot gia (Detail)", value=True, help="Re chuot ngau nhien truoc khi scrape detail")

            detail_fake_scroll = st.checkbox("Cuon gia (Detail)", value=True, help="Cuon trang detail truoc khi scrape")

            detail_wait_load_min = st.number_input("Chờ load tối thiểu (s)", value=2.0, min_value=0.0, max_value=120.0, step=0.5, key="detail_wait_load_min")

            detail_wait_load_max = st.number_input("Chờ load tối đa (s)", value=5.0, min_value=0.0, max_value=180.0, step=0.5, key="detail_wait_load_max")

        with dcol2:

            detail_delay_min = st.number_input("Delay giữa link tối thiểu (s)", value=2.0, min_value=0.0, max_value=120.0, step=0.5, key="detail_delay_min")

            detail_delay_max = st.number_input("Delay giữa link tối đa (s)", value=3.0, min_value=0.0, max_value=180.0, step=0.5, key="detail_delay_max")

        

        # Filter links by ID range

        filtered_links = [link for link in all_links if min_id <= link['id'] <= max_id]

        

        if filtered_links:

            st.info(f"📊 Tìm thấy {len(filtered_links)} links trong khoảng ID {min_id} - {max_id}")

            

            # Template uploader

            st.markdown("---")

            detail_template_file = st.file_uploader(

                "📄 Upload Detail Template (JSON)",

                type=['json'],

                help="Upload template JSON file từ Extension (Detail Mode)",

                key="detail_template_uploader"

            )

            

            detail_template_data = None

            detail_schema = None

            

            if detail_template_file is not None:

                try:

                    detail_template_data = json.load(detail_template_file)

                    detail_schema = convert_template_to_schema(detail_template_data)

                    

                    st.success("✅ Template loaded successfully!")

                    col1, col2 = st.columns(2)

                    with col1:

                        st.write(f"**Name:** {detail_template_data.get('name', 'N/A')}")

                        st.write(f"**Fields:** {len(detail_template_data.get('fields', []))}")

                    with col2:

                        st.write(f"**Original URL:** {detail_template_data.get('url', 'N/A')[:50]}...")

                        st.write(f"**Created:** {detail_template_data.get('createdAt', 'N/A')}")

                except json.JSONDecodeError:

                    st.error("❌ Invalid JSON file!")

                except Exception as e:

                    st.error(f"❌ Error loading template: {str(e)}")

            

            # Start scraping button

            start_detail_scrape_btn = st.button(

                "🚀 Start Scraping Detail Pages",

                type="primary",

                use_container_width=True,

                key="start_detail_scrape_btn",

                disabled=not detail_schema

            )

            

            # Scraping function

            async def scrape_detail_pages(

                links: List[dict],

                schema: Dict,

                template: Dict,

                delay: float,

                delay_min: float,

                delay_max: float,

                wait_load_min: float,

                wait_load_max: float,

                show_browser: bool,

                db: Database,

                progress_callback=None

            ): 

                """

                Scrape detail pages from links with delay and update status

                """

                results = []

                total = len(links)

                

                # 1. Xác định đường dẫn tuyệt đối cho Profile

                import os

                # Lưu profile ngay cạnh file dashboard.py với tên 'playwright_profile_tab3_detail'

                profile_dir_tab3 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "playwright_profile_tab3_detail")

                

                # Tạo thư mục nếu chưa có

                if not os.path.exists(profile_dir_tab3):

                    os.makedirs(profile_dir_tab3)



                print(f"📂 Đang dùng Profile tại: {profile_dir_tab3}") #

                # Giữ browser mở sau khi hoàn thành để quan sát, kèm profile riêng

                async with WebScraper(

                    headless=not show_browser,

                    verbose=False,

                    keep_open=True,

                    user_data_dir=profile_dir_tab3

                    

                ) as scraper:
                    # Warm up crawler/browser so display_page can be created
                    if detail_show_browser:
                        try:
                            await scraper.scrape_simple('https://example.com', bypass_cache=True)
                        except Exception as e:
                            print(f"[detail observe] warmup fail: {e}")
                        try:
                            try:
                                bm = getattr(scraper.crawler.crawler_strategy, "browser_manager", None)
                                mb = getattr(bm, "managed_browser", None) if bm else None
                                bp = getattr(mb, "browser_process", None) if mb else None
                                pid = getattr(bp, "pid", None)
                                if pid:
                                    print(f"[detail observe] Browser PID: {pid}")
                            except Exception:
                                pass
                            context = None
                            if hasattr(scraper, "crawler") and hasattr(scraper.crawler, "crawler_strategy"):
                                bm = getattr(scraper.crawler.crawler_strategy, "browser_manager", None)
                                context = getattr(bm, "default_context", None) if bm else None
                            if context:
                                pages = []
                                try:
                                    pages = list(context.pages)
                                except Exception:
                                    pages = []
                                try:
                                    print(f"[detail observe] Context pages: {len(pages)}")
                                    for i, p in enumerate(pages[:5], 1):
                                        try:
                                            print(f"[detail observe] Page {i}: {p.url}")
                                        except Exception:
                                            pass
                                except Exception:
                                    pass
                                if pages:
                                    scraper.display_page = pages[0]
                                else:
                                    try:
                                        scraper.display_page = await context.new_page()
                                    except Exception as e:
                                        scraper.display_page = None
                                        print(f"[detail observe] cannot create display_page: {e}")
                            else:
                                scraper.display_page = None
                                print('[detail observe] crawler context not ready after warmup')
                        except Exception as e:
                            print(f"[detail observe] cannot create display_page: {e}")


                    for i, link_data in enumerate(links, 1):

                        url = link_data['url']

                        link_id = link_data['id']

                        

                        if progress_callback:

                            progress_callback(i, total, f"Scraping ID {link_id} ({i}/{total}): {url[:50]}...", 0)

                        

                        try:

                            import random

                            # Delay ngẫu nhiên giữa các link (detail)

                            if detail_delay_min is not None and detail_delay_max is not None and detail_delay_max >= detail_delay_min and (detail_delay_min > 0 or detail_delay_max > 0):

                                wait_time = random.uniform(detail_delay_min, detail_delay_max)

                            else:

                                wait_time = delay_seconds

                            if wait_time and wait_time > 0:

                                await asyncio.sleep(wait_time)



                            # Fake scroll/hover truoc khi scrape (chong bot)
                            if detail_show_browser:
                                try:
                                    # Khoi tao / reset tab quan sat
                                    if (not hasattr(scraper, "display_page") or scraper.display_page is None
                                        or (hasattr(scraper.display_page, "is_closed") and scraper.display_page.is_closed())):
                                        context = None
                                        if hasattr(scraper, "crawler") and hasattr(scraper.crawler, "crawler_strategy"):
                                            bm = getattr(scraper.crawler.crawler_strategy, "browser_manager", None)
                                            context = getattr(bm, "default_context", None) if bm else None
                                        if context:
                                            pages = []
                                            try:
                                                pages = list(context.pages)
                                            except Exception:
                                                pages = []
                                            if pages:
                                                scraper.display_page = pages[0]
                                            else:
                                                try:
                                                    scraper.display_page = await context.new_page()
                                                except Exception as e:
                                                    scraper.display_page = None
                                                    print(f"[detail observe] cannot create display_page: {e}")
                                        else:
                                            scraper.display_page = None
                                            print("[detail observe] Khong tao duoc display_page (context chua san).")
                                    if scraper.display_page:
                                        try:
                                            await scraper.display_page.goto(url, wait_until="domcontentloaded")
                                            try:
                                                await scraper.display_page.bring_to_front()
                                            except Exception:
                                                pass
                                            try:
                                                await scraper.display_page.wait_for_load_state("networkidle", timeout=15000)
                                            except Exception:
                                                pass
                                            try:
                                                await scraper.display_page.evaluate(
                                                    "(t) => { document.title = t; }",
                                                    f"[C4AI] {url[:80]}"
                                                )
                                                await scraper.display_page.evaluate(
                                                    "() => {"
                                                    "  let b = document.getElementById('c4ai-banner');"
                                                    "  if (!b) {"
                                                    "    b = document.createElement('div');"
                                                    "    b.id = 'c4ai-banner';"
                                                    "    b.style.position = 'fixed';"
                                                    "    b.style.top = '0';"
                                                    "    b.style.left = '0';"
                                                    "    b.style.right = '0';"
                                                    "    b.style.zIndex = '2147483647';"
                                                    "    b.style.background = 'rgba(255,0,0,0.85)';"
                                                    "    b.style.color = '#fff';"
                                                    "    b.style.fontSize = '16px';"
                                                    "    b.style.fontFamily = 'Arial, sans-serif';"
                                                    "    b.style.padding = '6px 10px';"
                                                    "    b.textContent = '[C4AI ACTIVE]';"
                                                    "    document.body.appendChild(b);"
                                                    "  }"
                                                    "}"
                                                )
                                            except Exception:
                                                pass
                                            await asyncio.sleep(1)
                                        except Exception as e:
                                            print(f"[detail observe] goto fail: {e}")
                                        # Hover gia: di chuyen chuot 3 lan, moi lan cach 200ms
                                        if detail_fake_hover:
                                            width = await scraper.display_page.evaluate("() => window.innerWidth || 1200")
                                            height = await scraper.display_page.evaluate("() => window.innerHeight || 800")
                                            for _ in range(3):
                                                x = random.randint(0, max(int(width) - 1, 1))
                                                y = random.randint(0, max(int(height) - 1, 1))
                                                await scraper.display_page.mouse.move(x, y)
                                                await asyncio.sleep(0.2)
                                        # Cuon gia: focus body + cuon 10 buoc bang JS, moi buoc 200ms
                                        if detail_fake_scroll:
                                            await scraper.display_page.evaluate("""
                                                () => {
                                                    const doc = document.scrollingElement || document.documentElement || document.body;
                                                    if (doc && doc.focus) { doc.focus(); }
                                                    const height = doc?.scrollHeight || document.body.scrollHeight || 2000;
                                                    const step = Math.max(Math.floor(height / 10), 200);
                                                    let count = 0;
                                                    function doScroll() {
                                                        window.scrollBy(0, step);
                                                        count++;
                                                        if (count < 10) {
                                                            setTimeout(doScroll, 200);
                                                        }
                                                    }
                                                    doScroll();
                                                }
                                            """)
                                            await asyncio.sleep(3)
                                        if scraper.display_page:
                                            scraper._page_ready_url = url
                                except Exception as e:
                                    print(f"[detail observe] {e}")

                            result = await scrape_url(

                                url,

                                schema,

                                template,

                                scraper,

                                wait_load_min=detail_wait_load_min,

                                wait_load_max=detail_wait_load_max,

                                show_browser=detail_show_browser

                            )

                            result['link_id'] = link_id

                            result['url'] = url

                            results.append(result)

                            # Luu vao bang scraped_details
                            detail_id = None
                            try:
                                detail_id = db.add_scraped_detail_flat(
                                    url=url,
                                    data=result.get('data'),
                                    domain=link_data.get('domain'),
                                    link_id=link_id
                                )
                            except Exception as e:
                                print(f"[scraped_details] cannot save {url}: {e}")
                            try:
                                imgs = result.get('data', {}).get('img') if isinstance(result.get('data'), dict) else None
                                if detail_id and imgs:
                                    if isinstance(imgs, list):
                                        db.add_detail_images(detail_id, imgs)
                                    elif isinstance(imgs, str):
                                        db.add_detail_images(detail_id, [imgs])
                            except Exception as e:
                                print(f"[scraped_details] cannot save images for {url}: {e}")

                            #
                            try:
                                db.add_scraped_detail(
                                    url=url,
                                    data=result.get('data'),
                                    domain=link_data.get('domain'),
                                    link_id=link_id,
                                    success=result.get('success', False)
                                )
                            except Exception as e:
                                print(f"[scraped_details] cannot save {url}: {e}")

                            

                            # Cập nhật status thành CRAWLED nếu thành công

                            if result.get('success'):

                                db.update_link_status(url, 'CRAWLED')

                            else:

                                db.update_link_status(url, 'ERROR')

                        except Exception as e:

                            results.append({

                                'success': False,

                                'error': str(e),

                                'link_id': link_id,

                                'url': url

                            })

                            # Cập nhật status thành ERROR nếu có lỗi

                            db.update_link_status(url, 'ERROR')

                        

                        # Delay between requests

                        if i < total:

                            await asyncio.sleep(delay)

                

                return results

            

            # Execute scraping

            if start_detail_scrape_btn and detail_schema and filtered_links:

                st.session_state.detail_scraping_status = 'running'

                

                with st.status("🔄 Scraping detail pages...", expanded=True) as status:

                    try:

                        def update_detail_progress(current, total, status_text, _):

                            # Removed status.update() to prevent removeChild errors from Streamlit internal JS

                            # Progress is shown via session state only

                            pass

                        

                        # Run async scrape

                        detail_results = asyncio.run(

                            scrape_detail_pages(

                                filtered_links,

                                detail_schema,

                                detail_template_data,

                                delay_seconds,

                                detail_delay_min,

                                detail_delay_max,

                                detail_wait_load_min,

                                detail_wait_load_max,

                                detail_show_browser,

                                st.session_state.db_crawl_listing,

                                update_detail_progress

                            )

                        )

                        

                        # Store results

                        st.session_state.detail_scraping_results = detail_results

                        st.session_state.detail_scraping_status = 'completed'

                        

                        # Count success/failed

                        success_count = sum(1 for r in detail_results if r.get('success'))

                        failed_count = len(detail_results) - success_count

                        

                        try:

                            if status:

                                status.update(

                                    label=f"✅ Scraping completed! Success: {success_count}, Failed: {failed_count}",

                                    state="complete"

                                )

                        except Exception:

                            pass

                    

                    except Exception as e:

                        st.session_state.detail_scraping_status = 'error'

                        status.update(label=f"❌ Error: {str(e)}", state="error")

                        import traceback

                        st.code(traceback.format_exc())

                

                # Show results

                if st.session_state.detail_scraping_status == 'completed':

                    results = st.session_state.get('detail_scraping_results', [])

                    

                    if results:

                        st.markdown("---")

                        st.subheader("📊 Scraping Results")

                        

                        # Summary

                        success_results = [r for r in results if r.get('success')]

                        failed_results = [r for r in results if not r.get('success')]

                        

                        col1, col2, col3 = st.columns(3)

                        with col1:

                            st.metric("Total", len(results))

                        with col2:

                            st.metric("Success", len(success_results))

                        with col3:

                            st.metric("Failed", len(failed_results))

                        

                        # Display results

                        if success_results:

                            st.markdown("### ✅ Successful Results")

                            

                            # Convert to DataFrame for summary table

                            def _safe_data(rec):
                                data = rec.get('data') if isinstance(rec, dict) else None
                                return data if isinstance(data, dict) else {}

                            df_results = pd.DataFrame([
                                {
                                    'ID': r.get('link_id') if isinstance(r, dict) else None,
                                    'URL': ((r.get('url', '') if isinstance(r, dict) else '')[:50] + '...'),
                                    'Title': str(_safe_data(r).get('title') or 'N/A')[:50],
                                    '????<a ch??%': str(_safe_data(r).get('diachi') or 'N/A')[:50],
                                    'GiA?': _safe_data(r).get('gia', 'N/A'),
                                    'Status': '?o.'
                                }
                                for r in success_results
                                if isinstance(r, dict)
                            ])

                            

                            st.dataframe(df_results, use_container_width=True, hide_index=True)

                            

                            # Detailed results with expander

                            st.markdown("---")

                            st.markdown("### 📋 Chi Tiết Kết Quả Đã Cào")

                            

                            for idx, r in enumerate(success_results, 1):

                                link_id = r.get('link_id')

                                url = r.get('url', 'N/A')

                                data = r.get('data', {})

                                

                                with st.expander(f"🔍 ID {link_id}: {url[:60]}...", expanded=False):

                                    st.write(f"**URL:** {url}")

                                    st.write(f"**Timestamp:** {r.get('timestamp', 'N/A')}")

                                    

                                    if isinstance(data, dict) and data:

                                        st.markdown("**Dữ liệu đã cào:**")

                                        for key, value in data.items():

                                            if value:

                                                if key == 'img' and isinstance(value, list):

                                                    st.write(f"**{key}:** {len(value)} ảnh")

                                                    # Show first few images

                                                    for img_idx, img_url in enumerate(value[:3], 1):

                                                        st.write(f"  - Ảnh {img_idx}: {img_url[:80]}...")

                                                elif isinstance(value, list):
                                                    if len(value) == 1:
                                                        st.write(f"**{key}:** {value[0]}")
                                                    else:
                                                        st.write(f"**{key}:** {len(value)} item(s)")
                                                        for item_idx, item in enumerate(value[:5], 1):
                                                            st.write(f"  - {item_idx}: {item}")
                                                elif isinstance(value, str) and len(value) > 200:

                                                    st.write(f"**{key}:** {value[:200]}...")

                                                else:

                                                    st.write(f"**{key}:** {value}")

                                    else:

                                        st.write("**Dữ liệu:** Không có dữ liệu")

                            

                            # Download button

                            st.markdown("---")

                            results_json = json.dumps(success_results, ensure_ascii=False, indent=2)

                            st.download_button(

                                "📥 Download Results (JSON)",

                                results_json,

                                file_name=f"detail_scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",

                                mime="application/json"

                            )

                        

                        if failed_results:

                            with st.expander("❌ Failed URLs", expanded=False):

                                for r in failed_results:

                                    st.error(f"**ID {r.get('link_id')}**: {r.get('url', 'N/A')} - {r.get('error', 'Unknown error')}")

        else:

            st.warning(f"⚠️ Không tìm thấy links nào trong khoảng ID {min_id} - {max_id}")

    else:

        st.info("📭 Chưa có links nào. Hãy crawl listing trước.")







# ============================================
# TAB 4: Download Images
# ============================================
with tab4:
    st.header("Download Images")
    st.markdown("Tai anh tu danh sach URL, luu vao thu muc chi dinh va ghi log vao database.")

    # Init DB handler
    if 'db_images' not in st.session_state:
        st.session_state.db_images = Database(
            host="localhost",
            user="root",
            password="",
            database="craw_db"
        )

    col_in1, col_in2 = st.columns([2, 1])
    with col_in1:
        image_urls_raw = st.text_area(
            "Danh sach URL anh (moi dong mot URL)",
            height=200,
            placeholder="https://example.com/image1.jpg\nhttps://example.com/image2.png"
        )
    with col_in2:
        output_dir = st.text_input(
            "Thu muc luu anh",
            value=os.path.join(os.getcwd(), "output", "images")
        )
        images_per_minute = st.number_input(
            "So anh toi da moi phut",
            min_value=1,
            max_value=600,
            value=30,
            help="He thong se cho ~60/gia tri nay giay giua cac lan tai."
        )
        domain_label = st.text_input("Domain (tuy chon)", value="")

    start_dl = st.button("?? Bat dau tai anh", type="primary", use_container_width=True)

    if start_dl:
        urls = [u.strip() for u in image_urls_raw.splitlines() if u.strip()]
        if not urls:
            st.warning("Chua co URL anh nao.")
        else:
            os.makedirs(output_dir, exist_ok=True)
            interval = 60.0 / max(images_per_minute, 1)
            total = len(urls)
            success = 0
            failed = 0
            log_lines = []

            for idx, url in enumerate(urls, 1):
                log_lines.append(f"{idx}/{total} - Dang tai: {url[:80]}...")
                file_path = None
                try:
                    resp = requests.get(url, timeout=30)
                    resp.raise_for_status()
                    parsed = os.path.splitext(url.split('?')[0])
                    ext = parsed[1] if parsed[1] else '.jpg'
                    # Ten file theo hash de trung thi ghi de
                    filename = f"{hashlib.md5(url.encode()).hexdigest()}{ext}"
                    file_path = os.path.join(output_dir, filename)
                    _save_image_bytes(resp.content, file_path, max_width=1100)
                    success += 1
                    log_lines.append(f"✅ {url}")
                    st.session_state.db_images.add_downloaded_image(
                        image_url=url,
                        file_path=file_path,
                        status='SUCCESS',
                        domain=domain_label or None,
                        error=None
                    )
                except Exception as e:
                    failed += 1
                    log_lines.append(f"❌ {url} - {e}")
                    st.session_state.db_images.add_downloaded_image(
                        image_url=url,
                        file_path=file_path,
                        status='FAILED',
                        domain=domain_label or None,
                        error=str(e)
                    )
                time.sleep(interval)

            st.success(f"Xong. Tong: {total}, thanh cong: {success}, loi: {failed}")
            if log_lines:
                st.code("\n".join(log_lines), language="text")

    st.markdown('---')
    st.subheader('Lich su tai anh gan day')
    try:
        history = st.session_state.db_images.get_recent_images(limit=200, domain=None)
        if history:
            df_imgs = pd.DataFrame(history)
            st.dataframe(df_imgs, use_container_width=True, hide_index=True)
        else:
            st.info('Chua co ban ghi tai anh nao.')
    except Exception as e:
        st.error(f'Loi khi tai lich su anh: {e}')

    st.markdown("---")
    st.subheader("Anh trong scraped_detail_images (20/ trang)")
    if 'tab4_page' not in st.session_state:
        st.session_state.tab4_page = 1
    if 'tab4_page_input' not in st.session_state:
        st.session_state.tab4_page_input = st.session_state.tab4_page
    if 'tab4_domain_filter' not in st.session_state:
        st.session_state.tab4_domain_filter = "(Tat ca)"
    if 'tab4_status_filter' not in st.session_state:
        st.session_state.tab4_status_filter = "(Tat ca)"
    prev_domain_filter = st.session_state.tab4_domain_filter
    prev_status_filter = st.session_state.tab4_status_filter
    filter_cols = st.columns(2)
    with filter_cols[0]:
        domain_options_table = ["(Tat ca)"] + st.session_state.db_images.get_detail_image_domains()
        selected_domain_table = st.selectbox(
            "Domain (table)",
            domain_options_table,
            index=domain_options_table.index(st.session_state.tab4_domain_filter)
            if st.session_state.tab4_domain_filter in domain_options_table else 0,
            key="tab4_domain_filter",
        )
    with filter_cols[1]:
        status_options_table = ["(Tat ca)", "PENDING", "DOWNLOADED", "FAILED"]
        selected_status_table = st.selectbox(
            "Status (table)",
            status_options_table,
            index=status_options_table.index(st.session_state.tab4_status_filter)
            if st.session_state.tab4_status_filter in status_options_table else 0,
            key="tab4_status_filter",
        )
    domain_filter_table = None if selected_domain_table == "(Tat ca)" else selected_domain_table
    status_filter_table = None if selected_status_table == "(Tat ca)" else selected_status_table

    if prev_domain_filter != selected_domain_table or prev_status_filter != selected_status_table:
        st.session_state.tab4_page = 1
        st.session_state.tab4_page_input = 1
    if domain_filter_table or status_filter_table:
        total_img = st.session_state.db_images.count_detail_images_filtered(domain=domain_filter_table, status=status_filter_table)
    else:
        total_img = st.session_state.db_images.count_detail_images()
    colp1, colp2, colp3 = st.columns([1,1,1])
    with colp1:
        st.metric("Tong anh", total_img)
    with colp2:
        page_num = st.number_input(
            "Trang",
            min_value=1,
            value=st.session_state.tab4_page,
            step=1,
            key="tab4_page_input",
        )
        st.session_state.tab4_page = page_num
    with colp3:
        st.write("20 anh/trang")
    offset = (st.session_state.tab4_page - 1) * 20
    try:
        if domain_filter_table or status_filter_table:
            img_rows = st.session_state.db_images.get_detail_images_paginated_filtered(
                limit=20,
                offset=offset,
                domain=domain_filter_table,
                status=status_filter_table,
            )
        else:
            img_rows = st.session_state.db_images.get_detail_images_paginated(limit=20, offset=offset)
        if img_rows:
            df_img = pd.DataFrame(img_rows)
            st.dataframe(df_img, use_container_width=True, hide_index=True)
        else:
            st.info("Khong co anh o trang nay.")
    except Exception as e:
        st.error(f"Loi khi doc scraped_detail_images: {e}")

    st.markdown("---")
    st.subheader("Download anh theo ID range")
    colf1, colf2 = st.columns(2)
    with colf1:
        domain_options_range = ["(Tat ca)"] + st.session_state.db_images.get_detail_image_domains()
        selected_domain_range = st.selectbox("Domain (range)", domain_options_range, index=0, key="range_domain")
    with colf2:
        status_options_range = ["(Tat ca)", "PENDING", "DOWNLOADED", "FAILED"]
        selected_status_range = st.selectbox("Status (range)", status_options_range, index=0, key="range_status")
    domain_filter_range = None if selected_domain_range == "(Tat ca)" else selected_domain_range
    status_filter_range = None if selected_status_range == "(Tat ca)" else selected_status_range

    colr1, colr2, colr3 = st.columns(3)
    with colr1:
        range_start = st.number_input("Start ID (0 = all)", min_value=0, value=0, step=1)
    with colr2:
        range_end = st.number_input("End ID (0 = all)", min_value=0, value=0, step=1)
    with colr3:
        dl_images_per_minute = st.number_input("So anh/phut (range)", min_value=1, max_value=600, value=30)
        download_range_btn = st.button("Download theo ID range", use_container_width=True)
    if download_range_btn:
        if (range_start == 0 and range_end > 0) or (range_end == 0 and range_start > 0):
            st.warning("Vui long nhap ca Start ID va End ID (hoac de 0 de lay tat ca).")
        elif range_start > 0 and range_end > 0 and range_end < range_start:
            st.warning("End ID phai >= Start ID")
        else:
            interval = 60.0 / max(dl_images_per_minute, 1)
            stats = {"ok": 0, "fail": 0}
            log_lines = []
            progress = st.progress(0)
            progress_text = st.empty()
            os.makedirs(output_dir, exist_ok=True)

            def _download_rows(batch_rows, start_idx, total_count):
                for idx, row in enumerate(batch_rows, start_idx):
                    url = row['image_url']
                    image_id = row.get('id') if isinstance(row, dict) else None
                    log_lines.append(f"{idx}/{total_count} - {url[:80]}...")
                    file_path = None
                    try:
                        resp = requests.get(url, timeout=30)
                        resp.raise_for_status()
                        parsed = os.path.splitext(url.split('?')[0])
                        ext = parsed[1] if parsed[1] else '.jpg'
                        filename = f"{hashlib.md5(url.encode()).hexdigest()}{ext}"
                        file_path = os.path.join(output_dir, filename)
                        _save_image_bytes(resp.content, file_path, max_width=1100)
                        stats["ok"] += 1
                        log_lines.append(f"OK: {url}")
                        st.session_state.db_images.add_downloaded_image(
                            image_url=url,
                            file_path=file_path,
                            status='SUCCESS',
                            domain=domain_label or None,
                            error=None
                        )
                        if image_id:
                            st.session_state.db_images.update_detail_image_status(image_id, "DOWNLOADED")
                    except Exception as e:
                        stats["fail"] += 1
                        print(f"Download error {url}: {e}")
                        log_lines.append(f"FAIL: {url} - {e}")
                        st.session_state.db_images.add_downloaded_image(
                            image_url=url,
                            file_path=file_path,
                            status='FAILED',
                            domain=domain_label or None,
                            error=str(e)
                        )
                        if image_id:
                            st.session_state.db_images.update_detail_image_status(image_id, "FAILED")
                    progress_value = int((idx / total_count) * 100)
                    progress.progress(progress_value)
                    progress_text.write(f"Da tai: {idx}/{total_count} | Thanh cong: {stats['ok']} | Loi: {stats['fail']}")
                    time.sleep(interval)

            if range_start == 0 and range_end == 0:
                total = (
                    st.session_state.db_images.count_detail_images_filtered(domain=domain_filter_range, status=status_filter_range)
                    if domain_filter_range or status_filter_range
                    else st.session_state.db_images.count_detail_images()
                )
                if total == 0:
                    st.info("Khong co ban ghi nao theo bo loc hien tai.")
                else:
                    st.success(f"Tim thay {total} anh. Bat dau tai ...")
                    batch_size = 200
                    processed = 0
                    while processed < total:
                        if domain_filter_range or status_filter_range:
                            batch_rows = st.session_state.db_images.get_detail_images_paginated_filtered(
                                limit=batch_size,
                                offset=processed,
                                domain=domain_filter_range,
                                status=status_filter_range,
                            )
                        else:
                            batch_rows = st.session_state.db_images.get_detail_images_paginated(
                                limit=batch_size,
                                offset=processed,
                            )
                        if not batch_rows:
                            break
                        _download_rows(batch_rows, processed + 1, total)
                        processed += len(batch_rows)
                    progress.progress(100)
            else:
                rows = st.session_state.db_images.get_detail_images_by_id_range(
                    range_start,
                    range_end,
                    domain=domain_filter_range,
                    status=status_filter_range,
                )
                if not rows:
                    st.info("Khong co ban ghi nao trong khoang ID nay.")
                else:
                    total = len(rows)
                    st.success(f"Tim thay {total} anh. Bat dau tai ...")
                    _download_rows(rows, 1, total)
                    progress.progress(100)
                progress_text.write(f"Hoan thanh: {total} anh | Thanh cong: {ok} | Loi: {fail}")
                st.success(f"Xong range. Thanh cong: {ok}, Loi: {fail}")
                if log_lines:
                    st.code("\\n".join(log_lines), language="text")
# ============================================
# TAB 5: Auto Schedule
# ============================================
with tab5:
    st.header("Auto Schedule")
    st.markdown("Manage background tasks (listing -> detail -> image).")

    if 'db_scheduler' not in st.session_state:
        st.session_state.db_scheduler = Database(
            host="localhost",
            user="root",
            password="",
            database="craw_db"
        )

    # Add task form
    st.subheader("Add Task")
    template_dir = os.path.join(os.getcwd(), "template")
    template_files = []
    try:
        if os.path.isdir(template_dir):
            template_files = [f for f in os.listdir(template_dir) if f.lower().endswith('.json')]
    except Exception:
        template_files = []
    template_choices = [""] + sorted(template_files)

    loaihinh_cho_thue_local = {
        'Nhà phố',
        'Nhà riêng',
        'Biệt thự',
        'Căn hộ chung cư',
        'Văn phòng',
        'Mặt bằng',
        'Nhà hàng - Khách sạn',
        'Nhà Kho - Xưởng',
        'Phòng trọ',
        'Đất khu công nghiệp',
    }
    loaihinh_options_local = [
        'Ban nha rieng',
        'Ban nha pho du an',
        'Ban biet thu',
        'Ban can ho chung cu',
        'Ban can ho Mini, Dich vu',
        'Ban dat nen du an',
        'Ban dat tho cu',
        'Ban dat nong, lam nghiep',
        'Ban nha hang - Khach san',
        'Ban kho, nha xuong',
        'Dự án',
        'Nhà phố',
        'Nhà riêng',
        'Biệt thự',
        'Căn hộ chung cư',
        'Văn phòng',
        'Mặt bằng',
        'Nhà hàng - Khách sạn',
        'Nhà Kho - Xưởng',
        'Phòng trọ',
        'Đất khu công nghiệp',
    ]

    st.markdown("Chon cac chuc nang can chay")
    enable_cols = st.columns(3)
    with enable_cols[0]:
        enable_listing = st.checkbox("Bat tinh nang liet ke", value=True, key="task_enable_listing")
    with enable_cols[1]:
        enable_detail = st.checkbox("Bat chi tiet", value=True, key="task_enable_detail")
    with enable_cols[2]:
        enable_image = st.checkbox("Bat hinh anh", value=False, key="task_enable_image")

    st.markdown("Chon tinh/xa (luu kem vao collected_links)")
    city_options_task = _fetch_cities(st.session_state.db_scheduler)
    city_choices_task = [(None, "(Tat ca)", None, None)] + [
        (c["old_city_id"], c["old_city_name"], c["new_city_id"], c["new_city_name"])
        for c in city_options_task
    ]
    selected_city_task = st.selectbox(
        "Tinh/TP (task)",
        options=city_choices_task,
        format_func=lambda x: x[1] if isinstance(x, (list, tuple)) else x,
        key="task_city",
    )
    task_city_id = selected_city_task[0] if isinstance(selected_city_task, (list, tuple)) and selected_city_task[0] else None
    task_city_name = selected_city_task[1] if isinstance(selected_city_task, (list, tuple)) and selected_city_task[0] else None
    task_new_city_id = selected_city_task[2] if isinstance(selected_city_task, (list, tuple)) and selected_city_task[0] else None
    task_new_city_name = selected_city_task[3] if isinstance(selected_city_task, (list, tuple)) and selected_city_task[0] else None

    ward_options_task = _fetch_city_children(st.session_state.db_scheduler, task_city_id)
    ward_choices_task = [(None, "(Tat ca)", None, None)] + [
        (c["old_city_id"], c["old_city_name"], c["new_city_id"], c["new_city_name"])
        for c in ward_options_task
    ]
    selected_ward_task = st.selectbox(
        "Huyen/Xa (task)",
        options=ward_choices_task,
        format_func=lambda x: x[1] if isinstance(x, (list, tuple)) else x,
        key="task_ward",
    )
    task_ward_id = selected_ward_task[0] if isinstance(selected_ward_task, (list, tuple)) and selected_ward_task[0] else None
    task_ward_name = selected_ward_task[1] if isinstance(selected_ward_task, (list, tuple)) and selected_ward_task[0] else None
    task_new_ward_id = selected_ward_task[2] if isinstance(selected_ward_task, (list, tuple)) and selected_ward_task[0] else None
    task_new_ward_name = selected_ward_task[3] if isinstance(selected_ward_task, (list, tuple)) and selected_ward_task[0] else None

    with st.form("add_task_form"):
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.markdown("General")
            task_name = st.text_input("Task name", value="Crawl Task")
            schedule_type = st.selectbox("Schedule type", ["interval", "daily"])
            interval_minutes = st.number_input("Interval minutes", min_value=1, max_value=1440, value=30)
            run_times = st.text_input("Daily run times (HH:MM,HH:MM)", value="08:00,20:00")
            listing_template = st.selectbox("Listing template", options=template_choices)
            detail_template = st.selectbox("Detail template", options=template_choices)
            start_url = st.text_input("Start URL", value="https://batdongsan.com.vn/nha-dat-ban")
            max_pages = st.number_input("Max pages", min_value=1, max_value=100, value=2)
            domain = st.text_input("Domain label", value="batdongsan")
            loaihinh = st.selectbox(
                "Loai hinh",
                options=loaihinh_options_local,
                index=3,
                format_func=lambda x: f"{x} (cho thuê)" if x in loaihinh_cho_thue_local else x,
            )
        with col_b:
            if enable_listing:
                st.markdown("Listing settings")
                listing_show_browser = st.checkbox("Listing show browser", value=True)
                listing_fake_scroll = st.checkbox("Listing fake scroll", value=True)
                listing_fake_hover = st.checkbox("Listing fake hover", value=False)
                listing_wait_load_min = st.number_input("Listing wait load min (s)", min_value=0.0, max_value=120.0, value=20.0, step=0.5)
                listing_wait_load_max = st.number_input("Listing wait load max (s)", min_value=0.0, max_value=180.0, value=30.0, step=0.5)
                listing_wait_next_min = st.number_input("Listing wait next min (s)", min_value=0.0, max_value=120.0, value=10.0, step=0.5)
                listing_wait_next_max = st.number_input("Listing wait next max (s)", min_value=0.0, max_value=180.0, value=20.0, step=0.5)
            else:
                listing_show_browser = False
                listing_fake_scroll = False
                listing_fake_hover = False
                listing_wait_load_min = 0.0
                listing_wait_load_max = 0.0
                listing_wait_next_min = 0.0
                listing_wait_next_max = 0.0
        with col_c:
            if enable_detail or enable_image:
                st.markdown("Detail + Image")
            if enable_detail:
                detail_show_browser = st.checkbox("Detail show browser", value=False)
                detail_fake_scroll = st.checkbox("Detail fake scroll", value=True)
                detail_fake_hover = st.checkbox("Detail fake hover", value=True)
                detail_wait_load_min = st.number_input("Detail wait load min (s)", min_value=0.0, max_value=120.0, value=2.0, step=0.5)
                detail_wait_load_max = st.number_input("Detail wait load max (s)", min_value=0.0, max_value=180.0, value=5.0, step=0.5)
                detail_delay_min = st.number_input("Detail delay min (s)", min_value=0.0, max_value=120.0, value=2.0, step=0.5)
                detail_delay_max = st.number_input("Detail delay max (s)", min_value=0.0, max_value=180.0, value=3.0, step=0.5)
            else:
                detail_show_browser = False
                detail_fake_scroll = False
                detail_fake_hover = False
                detail_wait_load_min = 0.0
                detail_wait_load_max = 0.0
                detail_delay_min = 0.0
                detail_delay_max = 0.0
            if enable_image:
                image_dir = st.text_input("Image dir", value=os.path.join(os.getcwd(), "output", "images"))
                images_per_minute = st.number_input("Images per minute", min_value=1, max_value=600, value=30)
                image_domain_options = ["(Tat ca)"] + st.session_state.db_scheduler.get_detail_image_domains()
                image_domain_selected = st.selectbox("Image domain", image_domain_options, index=0)
                image_domain = None if image_domain_selected == "(Tat ca)" else image_domain_selected
                image_status_options = ["(Tat ca)", "PENDING", "FAILED", "DOWNLOADED"]
                image_status_selected = st.selectbox("Image status", image_status_options, index=0)
                image_status = None if image_status_selected == "(Tat ca)" else image_status_selected
            else:
                image_dir = None
                images_per_minute = 0
                image_domain = None
                image_status = None

        submit_task = st.form_submit_button("Add task")

    def _compute_next_run_local(now_dt, s_type, interval_min, times_str):
        if s_type == 'daily':
            times = [t.strip() for t in (times_str or '').split(',') if t.strip()]
            if not times:
                times = ["08:00", "20:00"]
            candidates = []
            for t in times:
                try:
                    h, m = t.split(':', 1)
                    candidates.append(now_dt.replace(hour=int(h), minute=int(m), second=0, microsecond=0))
                except Exception:
                    pass
            candidates = sorted(candidates)
            for c in candidates:
                if c > now_dt:
                    return c
            return candidates[0] + timedelta(days=1) if candidates else now_dt + timedelta(days=1)
        mins = max(int(interval_min or 30), 1)
        return now_dt + timedelta(minutes=mins)

    if submit_task:
        enable_listing = bool(st.session_state.get("task_enable_listing", enable_listing))
        enable_detail = bool(st.session_state.get("task_enable_detail", enable_detail))
        enable_image = bool(st.session_state.get("task_enable_image", enable_image))
        if image_dir:
            enable_image = True
        listing_path = os.path.join(template_dir, listing_template) if listing_template else None
        detail_path = os.path.join(template_dir, detail_template) if detail_template else None
        now_dt = datetime.now()
        next_run = _compute_next_run_local(now_dt, schedule_type, interval_minutes, run_times)
        if not enable_image:
            image_dir = None
            images_per_minute = 0
        if enable_image and not image_dir:
            image_dir = os.path.join(os.getcwd(), "output", "images")
        task_id = st.session_state.db_scheduler.add_scheduler_task({
            'name': task_name,
            'active': True,
            'enable_listing': enable_listing,
            'enable_detail': enable_detail,
            'enable_image': enable_image,
            'schedule_type': schedule_type,
            'interval_minutes': interval_minutes if schedule_type == 'interval' else None,
            'run_times': run_times if schedule_type == 'daily' else None,
            'listing_template_path': listing_path,
            'detail_template_path': detail_path,
            'start_url': start_url,
            'max_pages': max_pages,
            'domain': domain,
            'loaihinh': loaihinh,
            'city_id': task_city_id,
            'city_name': task_city_name,
            'ward_id': task_ward_id,
            'ward_name': task_ward_name,
            'new_city_id': task_new_city_id,
            'new_city_name': task_new_city_name,
            'new_ward_id': task_new_ward_id,
            'new_ward_name': task_new_ward_name,
            'listing_show_browser': 1 if listing_show_browser else 0,
            'listing_fake_scroll': 1 if listing_fake_scroll else 0,
            'listing_fake_hover': 1 if listing_fake_hover else 0,
            'listing_wait_load_min': listing_wait_load_min,
            'listing_wait_load_max': listing_wait_load_max,
            'listing_wait_next_min': listing_wait_next_min,
            'listing_wait_next_max': listing_wait_next_max,
            'detail_show_browser': 1 if detail_show_browser else 0,
            'detail_fake_scroll': 1 if detail_fake_scroll else 0,
            'detail_fake_hover': 1 if detail_fake_hover else 0,
            'detail_wait_load_min': detail_wait_load_min,
            'detail_wait_load_max': detail_wait_load_max,
            'detail_delay_min': detail_delay_min,
            'detail_delay_max': detail_delay_max,
            'image_dir': image_dir,
            'images_per_minute': images_per_minute,
            'image_domain': image_domain,
            'image_status': image_status,
            'last_run_at': None,
            'next_run_at': next_run
        })
        st.success(f"Task added: {task_id}")

    st.markdown("---")
    st.subheader("Running tasks")
    tasks_running = st.session_state.db_scheduler.list_scheduler_tasks(active_only=False)
    running_only = [t for t in tasks_running if t.get('is_running')]
    if running_only:
        df_running = pd.DataFrame(running_only)
        st.dataframe(df_running, use_container_width=True, hide_index=True)
    else:
        st.info("No running tasks.")

    st.subheader("Tasks")
    tasks = st.session_state.db_scheduler.list_scheduler_tasks(active_only=False)
    if tasks:
        df_tasks = pd.DataFrame(tasks)
        st.dataframe(df_tasks, use_container_width=True, hide_index=True)
    else:
        st.info("No tasks yet")

    st.subheader("Task Actions")
    task_options = [f"{t['id']} - {t['name']}" for t in tasks] if tasks else []
    selected_task = st.selectbox("Select task", options=[""] + task_options)
    if selected_task:
        task_id = int(selected_task.split(' - ')[0])
        cur = next((t for t in tasks if t['id'] == task_id), None)
        if cur:
            status_cols = st.columns(4)
            with status_cols[0]:
                st.metric("Active", "ON" if cur.get('active') else "OFF")
            with status_cols[1]:
                st.metric("Running", "YES" if cur.get('is_running') else "NO")
            with status_cols[2]:
                st.metric("Next run", str(cur.get('next_run_at') or "N/A"))
            with status_cols[3]:
                st.metric("Last run", str(cur.get('last_run_at') or "N/A"))
            status_cols2 = st.columns(2)
            with status_cols2[0]:
                st.metric("Cancel flag", "ON" if cur.get('cancel_requested') else "OFF")
            with status_cols2[1]:
                st.metric("Run now", "YES" if cur.get('run_now') else "NO")

            if not cur.get('active'):
                st.info("Trang thai: PAUSED (bat task de chay).")
            elif cur.get('is_running'):
                st.success("Trang thai: RUNNING (dang chay).")
            else:
                st.info("Trang thai: SCHEDULED (cho den lich).")

            st.caption("Luu y: Scheduler service phai dang chay bang lenh `python scheduler_service.py`.")

        col_a, col_b, col_c, col_d, col_e = st.columns(5)
        with col_a:
            if st.button("Toggle active"):
                if cur:
                    st.session_state.db_scheduler.set_task_active(task_id, not bool(cur.get('active')))
                    st.success("Updated active state")
                    st.rerun()
        with col_b:
            if st.button("Delete task"):
                st.session_state.db_scheduler.delete_scheduler_task(task_id)
                st.success("Deleted task")
                st.rerun()
        with col_c:
            if st.button("Refresh logs"):
                st.rerun()
        with col_d:
            if st.button("Run now"):
                if cur:
                    # Quan trọng: clear cancel_requested để scheduler có thể pick task
                    st.session_state.db_scheduler.update_scheduler_task(
                        task_id,
                        {'run_now': 1, 'next_run_at': datetime.now(), 'active': 1, 'cancel_requested': 0}
                    )
                    st.session_state.db_scheduler.add_scheduler_log(
                        task_id, "task", "RUN_NOW", "Run now requested"
                    )
                    st.success("Run now queued")
                    st.rerun()
        with col_e:
            if st.button("Cancel (stop)", type="secondary"):
                st.session_state.db_scheduler.request_task_cancel(task_id)
                st.session_state.db_scheduler.add_scheduler_log(
                    task_id, "task", "CANCEL", "Cancel requested by user - Task deactivated"
                )
                st.success("Cancel requested - Task đã được TẮT để không tự chạy lại. Bật lại bằng Toggle active.")
                st.rerun()

        logs = st.session_state.db_scheduler.get_scheduler_logs(task_id, limit=200)
        if logs:
            df_logs = pd.DataFrame(logs)
            st.dataframe(
                df_logs,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "message": st.column_config.TextColumn("message", width="large"),
                },
            )
            with st.expander("Full logs (no truncation)", expanded=False):
                lines = []
                for row in logs:
                    created_at = row.get("created_at", "")
                    stage = row.get("stage", "")
                    status = row.get("status", "")
                    message = row.get("message", "")
                    lines.append(f"{created_at} | {stage} | {status} | {message}")
                st.text_area(
                    "Log text",
                    value="\n".join(lines),
                    height=300,
                )
        else:
            st.info("No logs for this task")

# ============================================
# TAB 6: Watermark / Logo
# ============================================
with tab6:
    st.header("Watermark / Logo")
    st.markdown("Chon thu muc anh, upload logo, va chay dong dau hang loat.")

    col_w1, col_w2 = st.columns(2)
    with col_w1:
        image_dir = st.text_input(
            "Thu muc anh",
            value=os.path.join(os.getcwd(), "output", "images")
        )
        overwrite = st.checkbox("Ghi de anh goc", value=False)
    with col_w2:
        output_dir = st.text_input(
            "Thu muc luu anh sau khi dong dau",
            value=os.path.join(os.getcwd(), "output", "images_watermarked")
        )
        margin = st.number_input("Le (px)", min_value=0, max_value=200, value=20, step=1)

    logo_file = st.file_uploader("Logo", type=["png", "jpg", "jpeg", "webp"])
    position = st.selectbox(
        "Vi tri logo",
        ["bottom-right", "bottom-left", "top-right", "top-left", "center"],
        index=0
    )
    scale_pct = st.slider("Ti le logo theo chieu ngang anh (%)", min_value=5, max_value=50, value=15)
    opacity = st.slider("Do mo logo", min_value=0.2, max_value=1.0, value=0.7, step=0.05)

    run_watermark = st.button("Bat dau dong dau", type="primary", use_container_width=True)
    if run_watermark:
        if not image_dir or not os.path.isdir(image_dir):
            st.error("Thu muc anh khong hop le.")
        elif not logo_file:
            st.error("Vui long upload logo.")
        else:
            try:
                from io import BytesIO
                from PIL import Image
            except Exception as e:
                st.error(f"Chua cai Pillow: {e}")
                st.stop()

            if overwrite:
                output_dir = image_dir
            os.makedirs(output_dir, exist_ok=True)

            logo_img = Image.open(BytesIO(logo_file.read()))

            exts = {".jpg", ".jpeg", ".png", ".webp"}
            paths = []
            for root, _, files in os.walk(image_dir):
                for fname in files:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext in exts:
                        paths.append(os.path.join(root, fname))

            if not paths:
                st.warning("Khong tim thay anh trong thu muc.")
            else:
                total = len(paths)
                ok = 0
                fail = 0
                skipped = 0
                progress = st.progress(0)
                progress_text = st.empty()
                log_lines = []

                for idx, src_path in enumerate(paths, 1):
                    rel = os.path.relpath(src_path, image_dir)
                    dst_path = src_path if overwrite else os.path.join(output_dir, rel)
                    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                    try:
                        base_img = Image.open(src_path)
                        if _has_watermark_marker(base_img):
                            skipped += 1
                            continue
                        marked = _apply_watermark(base_img, logo_img, position, scale_pct, opacity, margin)
                        ext = os.path.splitext(dst_path)[1].lower()
                        fmt = marked.format
                        if not fmt:
                            fmt = "PNG" if ext == ".png" else "JPEG"
                        if fmt.upper() == "JPEG" and marked.mode in ("RGBA", "LA", "P"):
                            marked = marked.convert("RGB")
                        save_kwargs = {"quality": 100}
                        _add_watermark_marker(fmt, save_kwargs)
                        marked.save(dst_path, format=fmt, **save_kwargs)
                        ok += 1
                    except Exception as e:
                        fail += 1
                        log_lines.append(f"FAIL: {src_path} - {e}")

                    progress_value = int((idx / total) * 100)
                    progress.progress(progress_value)
                    progress_text.write(f"Da xu ly: {idx}/{total} | OK: {ok} | SKIP: {skipped} | FAIL: {fail}")

                progress.progress(100)
                st.success(f"Hoan thanh. OK: {ok}, SKIP: {skipped}, FAIL: {fail}")
                if log_lines:
                    st.code("\n".join(log_lines), language="text")

# ============================================
# TAB 7: Profile Manager
# ============================================
with tab7:
    st.header("Profile Manager")
    st.markdown("Create and open browser profiles for login or captcha.")

    col_p1, col_p2 = st.columns(2)
    with col_p1:
        profile_type = st.selectbox(
            "Profile type",
            ["detail (playwright)", "listing (nodriver)"],
            index=0,
            key="profile_type_select"
        )
        task_id_input = st.text_input("Task ID (optional)", value="", key="profile_task_id")
    with col_p2:
        open_url = st.text_input(
            "Open URL",
            value="about:blank",
            key="profile_open_url"
        )
        wait_seconds = st.number_input(
            "Wait seconds",
            min_value=30,
            max_value=600,
            value=180,
            step=10,
            key="profile_wait_seconds"
        )
        force_unlock = st.checkbox(
            "Force unlock before open",
            value=True,
            key="profile_force_unlock"
        )
        keep_open = st.checkbox(
            "Keep browser open (manual close)",
            value=True,
            key="profile_keep_open"
        )
        open_mode = st.selectbox(
            "Open mode",
            [
                "Direct Chromium (recommended for login)",
                "Playwright (persistent context)",
                "Playwright then fallback to direct",
            ],
            index=0,
            key="profile_open_mode"
        )

    suffix = f"_{task_id_input.strip()}" if task_id_input.strip() else ""
    if profile_type.startswith("detail"):
        profile_dir = os.path.join(os.getcwd(), f"playwright_profile_tab3_detail{suffix}")
    else:
        profile_dir = os.path.join(os.getcwd(), f"nodriver_profile_listing{suffix}")

    st.caption(f"Profile path: {profile_dir}")

    col_b1, col_b2, col_b3 = st.columns(3)
    with col_b1:
        if st.button("Create profile folder", use_container_width=True):
            try:
                os.makedirs(profile_dir, exist_ok=True)
                st.success("Profile folder created.")
            except Exception as e:
                st.error(f"Create profile failed: {e}")
    with col_b2:
        if st.button("Clear cache/locks", use_container_width=True):
            try:
                os.makedirs(profile_dir, exist_ok=True)
                _clear_profile_cache(profile_dir)
                st.success("Cache/locks cleared.")
            except Exception as e:
                st.error(f"Clear cache failed: {e}")
    with col_b3:
        reset_confirm = st.checkbox(
            "I understand this deletes the profile data",
            value=False,
            key="profile_reset_confirm"
        )
        if st.button("Reset profile (delete)", use_container_width=True, disabled=not reset_confirm):
            try:
                _reset_profile_dir(profile_dir)
                os.makedirs(profile_dir, exist_ok=True)
                st.success("Profile reset.")
            except Exception as e:
                st.error(f"Reset profile failed: {e}")

    if keep_open and open_mode != "Direct Chromium (recommended for login)":
        st.info("💡 Tip: Với Keep open, nên dùng 'Direct Chromium' để browser chạy độc lập, không bị đóng.")

    col_open = st.columns(1)
    with col_open[0]:
        if st.button("Open profile", type="primary", use_container_width=True):
            try:
                os.makedirs(profile_dir, exist_ok=True)
                if force_unlock and profile_type.startswith("detail"):
                    _unlock_playwright_profile(profile_dir)
                target_url = open_url.strip() or "about:blank"
                
                # Khi keep_open=True, luôn dùng subprocess để browser không bị đóng
                if keep_open:
                    _open_chrome_profile(profile_dir, target_url)
                    st.success("✅ Profile opened (subprocess). Close the browser manually when done.")
                else:
                    # Không keep_open - dùng playwright/nodriver với timeout
                    if profile_type.startswith("detail"):
                        if open_mode == "Direct Chromium (recommended for login)":
                            _open_chrome_profile(profile_dir, target_url)
                        else:
                            try:
                                asyncio.run(_open_playwright_profile(profile_dir, target_url, int(wait_seconds), keep_open))
                            except Exception as e:
                                if open_mode == "Playwright then fallback to direct":
                                    _open_chrome_profile(profile_dir, target_url)
                                    st.warning(f"Playwright failed, opened direct Chromium instead: {e}")
                                else:
                                    raise
                    else:
                        asyncio.run(_open_nodriver_profile(profile_dir, target_url, int(wait_seconds), keep_open))
                    st.success("Profile closed after timeout.")
            except Exception as e:
                st.error(f"Open profile failed: {e}")
