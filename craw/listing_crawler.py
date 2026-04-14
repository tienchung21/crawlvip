"""
Listing Crawler - Collects item links from listing pages
Uses nodriver (undetected-chromedriver) to navigate and avoid bot detection
NOTE: Crawl4AI đã bị loại bỏ vì links được lấy trực tiếp bằng nodriver
"""

import asyncio
import os
import random
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional
import resource

# Tang stack size cho Linux (Fix CBOR stack limit exceeded)
try:
    max_stack = 256 * 1024 * 1024  # 256MB
    resource.setrlimit(resource.RLIMIT_STACK, (max_stack, max_stack))
    print(f"[INFO] Stack limit set to {max_stack}")
except Exception as e:
    print(f"[WARN] Could not set stack limit: {e}")

# Tang CBOR stack (Fix ProtocolException)
try:
    import cbor2
    cbor2._CBOR_MAX_STACK_LEVEL = 8192  # Tang len muc cao
except:
    pass

from urllib.parse import urljoin, urlparse

import nodriver as uc
# IMPORTANT: Bypass proxy for localhost to prevent ERR_TUNNEL_CONNECTION_FAILED (websocket)
# But ALLOW proxy for external sites (if system has http_proxy set)
os.environ['no_proxy'] = '127.0.0.1,localhost'

from nodriver import cdp  # CDP commands for bring_to_front
# NOTE: Crawl4AI imports đã bị loại bỏ - không còn sử dụng trong file này
# Việc import và khởi tạo Crawl4AI gây mở thêm browser không cần thiết

from database import Database

# Force UTF-8 stdout/stderr to avoid Windows charmap errors
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="ignore")
        except Exception:
            pass

# Cấu hình tiết kiệm cho nodriver (chặn ảnh, tắt audio để giảm lag và tiết kiệm bandwidth)
# Cấu hình tiết kiệm + Anti-detect + SANDBOX ENABLED
BROWSER_CONFIG_TIET_KIEM = [
    # "--mute-audio", # Enable audio just in case
    # Anti-detection flags - ẩn webdriver để bypass Cloudflare
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",  # Quan trong cho VPS
    "--disable-gpu",            # Tat GPU
]


def make_absolute_url(base_url: str, href: str) -> str:
    if not href:
        return ''
    if href.startswith('http://') or href.startswith('https://'):
        return href
    if href.startswith('//'):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}:{href}"
    return urljoin(base_url, href)


def _parse_cookie_string(cookie_str: str) -> Dict[str, str]:
    cookies = {}
    if not cookie_str:
        return cookies
    for part in cookie_str.split(';'):
        part = part.strip()
        if not part:
            continue
        if '=' in part:
            key, value = part.split('=', 1)
            cookies[key.strip()] = value.strip()
        else:
            cookies[part] = ''
    return cookies


def _find_ab_cookie(cookies: Dict[str, str]) -> Optional[str]:
    for key in cookies.keys():
        key_l = key.lower()
        if "ab" in key_l and any(tag in key_l for tag in ("id", "test", "variant")):
            return key
    for key in cookies.keys():
        if key.lower() in ("abid", "ab_id"):
            return key
    return None


async def crawl_listing(
    start_url: str,
    template_json: Dict,
    max_pages: int,
    db: Database,
    progress_callback=None,
    log_callback=None,
    domain: Optional[str] = None,
    loaihinh: Optional[str] = None,
    trade_type: Optional[str] = None,
    city_id: Optional[int] = None,
    city_name: Optional[str] = None,
    ward_id: Optional[int] = None,
    ward_name: Optional[str] = None,
    new_city_id: Optional[int] = None,
    new_city_name: Optional[str] = None,
    new_ward_id: Optional[int] = None,
    new_ward_name: Optional[str] = None,
    show_browser: bool = True,
    enable_fake_scroll: bool = True,
    enable_fake_hover: bool = False,
    wait_load_min: float = 20,
    wait_load_max: float = 30,
    wait_next_min: float = 10,
    wait_next_max: float = 20,
    profile_suffix: Optional[str] = None,
    cancel_callback=None,
) -> Dict[str, any]:
    raw_links = []
    item_selector = template_json.get('itemSelector', '')
    next_page_selector = template_json.get('nextPageSelector', '')

    # Normalize selectors (avoid XPath)
    if item_selector.strip().startswith('//'):
        item_selector = ''
    if next_page_selector.strip().startswith('//'):
        next_page_selector = ''

    # Safe defaults for batdongsan (only when template is empty)
    if 'batdongsan.com.vn' in start_url:
        if not item_selector:
            item_selector = '.js__product-link-for-product-id'
        if not next_page_selector:
            next_page_selector = (
                '.re__pagination a[aria-label*="Sau"], '
                '.re__pagination a[aria-label*="sau"], '
                '.re__pagination a[rel="next"], '
                'a.re__pagination-icon[href][aria-label*="sau"], '
                'a.re__pagination-icon[href][aria-label*="next"]'
            )

    print(f"[*] Listing item selector in use: {item_selector}")
    print(f"[*] Listing next selector in use: {next_page_selector}")
    if log_callback:
        log_callback(f"[Listing] itemSelector: {item_selector}")
        log_callback(f"[Listing] nextPageSelector: {next_page_selector}")
        try:
            import re as _re
            m = _re.search(r"\\.([a-zA-Z0-9_-]+)", next_page_selector or "")
            if m:
                log_callback(f"[Listing] Next button class: {m.group(1)}")
        except Exception:
            pass

    if not item_selector:
        return {
            'success': False,
            'error': 'itemSelector is required in template',
            'total_links': 0,
            'pages_crawled': 0,
            'new_links_added': 0
        }

    current_url = start_url
    total_links_collected: List[str] = []
    pages_crawled = 0
    cookie_logged = False
    canceled = False
    # Consecutive duplicate tracking - dừng khi có quá nhiều link trùng liên tiếp
    consecutive_duplicates = 0
    MAX_CONSECUTIVE_DUPLICATES = 350
    stopped_due_to_duplicates = False

    # Khởi động nodriver browser để navigate (tránh bot detection)
    browser = None
    # NOTE: crawler = None đã bị loại bỏ - Crawl4AI không còn được sử dụng
    try:
        # browser = await uc.start(headless=not show_browser, browser_args=BROWSER_CONFIG_TIET_KIEM)
        # 1. Định nghĩa đường dẫn lưu Profile "nhà riêng"
        import os
        # Tạo folder tên "nodriver_profile_listing" ngay cạnh file code
        profile_dir_listing = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "nodriver_profile_listing" + (f"_{profile_suffix}" if profile_suffix else "")
        )
        
        # Tự tạo folder nếu chưa có
        if not os.path.exists(profile_dir_listing):
            os.makedirs(profile_dir_listing)

        # Cảnh báo nếu headless với site có Cloudflare (có thể bị chặn)
        if "batdongsan.com.vn" in start_url or "nhatot.com" in start_url:
            if not show_browser:
                print("[Listing] WARNING: Headless mode với Cloudflare site - có thể bị chặn 'Just a moment...'")

        # 2. Khởi động với tham số user_data_dir để nhớ Token/Cookie
        # Thay thế cho dòng browser = await uc.start(...) cũ
        browser = await uc.start(
            headless=not show_browser,  # Để False để bố thấy trình duyệt và đăng nhập lần đầu
            browser_args=BROWSER_CONFIG_TIET_KIEM, # Use Tuned Config
            user_data_dir=profile_dir_listing,  # <--- Restore Task Profile (72)
            sandbox=False  # Avoid "Failed to connect to browser" in scheduler runs
        )
        
        page = await browser.get(start_url)
        # Bring browser window to front để tránh lazy-loading issues
        try:
            await page.send(cdp.page.bring_to_front())
            print("[Listing] Brought browser to front")
        except Exception as e:
            print(f"[Listing] Could not bring to front: {e}")
        
        # Override Page Visibility API (không override IntersectionObserver vì gây crash)
        try:
            await page.evaluate("""
                Object.defineProperty(document, 'hidden', { value: false, writable: false });
                Object.defineProperty(document, 'visibilityState', { value: 'visible', writable: false });
                document.dispatchEvent(new Event('visibilitychange'));
            """)
            print("[Listing] Overridden Page Visibility API")
        except Exception as e:
            print(f"[Listing] Could not override visibility: {e}")
        
        # Chờ Cloudflare challenge hoàn thành (nếu có)
        cloudflare_wait_attempts = 0
        max_cloudflare_wait = 10  # Tối đa 10 lần check = 30 giây
        while cloudflare_wait_attempts < max_cloudflare_wait:
            try:
                title = await page.evaluate("document.title")
                if title and "just a moment" in title.lower():
                    cloudflare_wait_attempts += 1
                    print(f"[Listing] Cloudflare challenge detected, waiting... ({cloudflare_wait_attempts}/{max_cloudflare_wait})")
                    await asyncio.sleep(3)
                else:
                    break
            except Exception:
                break
        
        if cloudflare_wait_attempts >= max_cloudflare_wait:
            print("[Listing] WARNING: Cloudflare challenge timeout - page may not load correctly")
        
        # NOTE: Crawl4AI crawler đã bị loại bỏ vì không còn sử dụng
        # Links được lấy trực tiếp bằng nodriver (querySelectorAll) ở bên dưới
        # Việc khởi tạo Crawl4AI ở đây chỉ gây mở thêm browser không cần thiết

        for page_num in range(1, max_pages + 1):
            raw_links = []
            try:
                if cancel_callback and cancel_callback():
                    canceled = True
                    if log_callback:
                        log_callback(f"[Listing] Cancel requested at page {page_num}/{max_pages}")
                    break
                print(f"\n{'='*60}")
                print(f"[*] BAT DAU CAO TRANG {page_num}/{max_pages}")
                print(f"{'='*60}")
                if log_callback:
                    log_callback(f"[Listing] Crawling page {page_num}/{max_pages}: {current_url}")
                if progress_callback:
                    progress_callback(page_num, max_pages, f"Crawling page {page_num}...", len(total_links_collected))
                
                # Navigate đến URL hiện tại
                # Sync current_url with actual page to avoid forcing back to page 1
                try:
                    current_page_url = await page.evaluate("window.location.href")
                    if current_page_url:
                        current_url = current_page_url
                except Exception:
                    page = await browser.get(current_url)
                
                # Chờ trang load hoàn toàn - 30 giây
                wait_load = random.uniform(wait_load_min, wait_load_max)
                if progress_callback:
                    progress_callback(
                        page_num,
                        max_pages,
                        f"Waiting {wait_load:.1f}s for page to fully load...",
                        len(total_links_collected)
                    )
                await asyncio.sleep(wait_load)
                
                # Kiểm tra xem trang đã load hoàn toàn chưa (check document.readyState)
                try:
                    page_ready = await page.evaluate("document.readyState === 'complete'")
                    if not page_ready:
                        if progress_callback:
                            progress_callback(
                                page_num,
                                max_pages,
                                f"Page not fully loaded, waiting more...",
                                len(total_links_collected)
                            )
                        # Chờ thêm nếu chưa ready
                        for i in range(10):
                            await asyncio.sleep(1)
                            try:
                                page_ready = await page.evaluate("document.readyState === 'complete'")
                                if page_ready:
                                    break
                            except:
                                # Nếu page bị đóng, tạo lại
                                page = await browser.get(current_url)
                                await asyncio.sleep(2)
                except Exception as e:
                    if progress_callback:
                        progress_callback(
                            page_num,
                            max_pages,
                            f"Error checking page ready: {str(e)}",
                            len(total_links_collected)
                        )
                    print(f"Error checking page ready: {e}")
                    # Thử tạo lại page
                    try:
                        page = await browser.get(current_url)
                        await asyncio.sleep(2)
                    except:
                        pass

                # Pre-scroll on current page to allow lazy content (even for page 1)
                if enable_fake_scroll:
                    try:
                        if log_callback:
                            log_callback("[Listing] Fake scroll: start")
                        before_y = None
                        try:
                            before_y = await page.evaluate("() => window.scrollY || window.pageYOffset || 0")
                        except Exception:
                            before_y = None
                        scrolled = False
                        if hasattr(page, "scroll_down"):
                            steps = random.randint(12, 24)
                            for _ in range(steps):
                                await page.scroll_down(random.randint(3, 8))
                                await asyncio.sleep(random.uniform(0.4, 1.0))
                            scrolled = True
                        if not scrolled:
                            before_y = await page.evaluate("() => window.scrollY || window.pageYOffset || 0")
                            height = await page.evaluate(
                                "() => (document.scrollingElement || document.documentElement || document.body).scrollHeight || 2000"
                            )
                            max_steps = max(8, min(20, int(height / 250)))
                            pos = int(before_y)
                            for _ in range(max_steps):
                                step = random.randint(150, 450)
                                pos = min(int(height), pos + step)
                                await page.evaluate(f"window.scrollTo(0, {pos});")
                                await asyncio.sleep(random.uniform(0.3, 0.9))
                            after_y = await page.evaluate("() => window.scrollY || window.pageYOffset || 0")
                            if after_y == before_y:
                                for _ in range(max_steps):
                                    step = random.randint(150, 450)
                                    await page.evaluate(f"""() => {{
                                        const els = Array.from(document.querySelectorAll('*'));
                                        let target = null;
                                        let maxDiff = 0;
                                        for (const el of els) {{
                                            const style = window.getComputedStyle(el);
                                            if (!style) continue;
                                            const oy = style.overflowY;
                                            if (oy !== 'auto' && oy !== 'scroll') continue;
                                            const diff = el.scrollHeight - el.clientHeight;
                                            if (diff > maxDiff + 50) {{
                                                maxDiff = diff;
                                                target = el;
                                            }}
                                        }}
                                        if (target) {{
                                            target.scrollTop = (target.scrollTop || 0) + {step};
                                        }} else {{
                                            window.scrollBy(0, {step});
                                        }}
                                    }}""")
                                    await asyncio.sleep(random.uniform(0.3, 0.9))
                            await asyncio.sleep(random.uniform(0.6, 1.4))
                        after_y = None
                        try:
                            after_y = await page.evaluate("() => window.scrollY || window.pageYOffset || 0")
                        except Exception:
                            after_y = None
                        if log_callback and before_y is not None and after_y is not None:
                            log_callback(f"[Listing] ScrollY {before_y} -> {after_y}")
                    except Exception:
                        if log_callback:
                            log_callback("[Listing] Fake scroll error")
                        pass

                # Wait for selector to appear (JS-rendered pages)
                if item_selector:
                    try:
                        import json as _json
                        _sel = _json.dumps(item_selector)
                        waited = 0.0
                        count = 0
                        for _ in range(20):
                            try:
                                count = await page.evaluate(f"document.querySelectorAll({_sel}).length")
                            except Exception:
                                count = 0
                            if count and count > 0:
                                break
                            await asyncio.sleep(0.5)
                            waited += 0.5
                        if waited > 0:
                            print(f"[*] Waited {waited:.1f}s for selector, count={count}")
                    except Exception:
                        pass

                if not cookie_logged:
                    try:
                        cookie_str = await page.evaluate("document.cookie") or ""
                        cookies = _parse_cookie_string(cookie_str)
                        cflan_key = None
                        for key in cookies.keys():
                            if key.lower() == "cflan" or "cflan" in key.lower():
                                cflan_key = key
                                break
                        if not cflan_key:
                            for key in cookies.keys():
                                if key.lower() == "cf_clearance":
                                    cflan_key = key
                                    break
                        ab_key = _find_ab_cookie(cookies)
                        cflan_label = f"{cflan_key}={cookies.get(cflan_key)}" if cflan_key else "cflan=N/A"
                        ab_label = f"{ab_key}={cookies.get(ab_key)}" if ab_key else "ab_id=N/A"
                        msg = f"[Listing Cookies] {cflan_label}; {ab_label}"
                        print(msg)
                        if log_callback:
                            log_callback(msg)
                    except Exception as e:
                        msg = f"[Listing Cookies] unable to read: {e}"
                        print(msg)
                        if log_callback:
                            log_callback(msg)
                    cookie_logged = True

                # Quick selector sanity check (avoid arrow fn to match nodriver evaluate)
                try:
                    import json as _json
                    _sel = _json.dumps(item_selector)
                    count = await page.evaluate(f"document.querySelectorAll({_sel}).length")
                    href = await page.evaluate(
                        f"""(function(){{
                            var el = document.querySelectorAll({_sel})[0];
                            if (!el) return '';
                            return el.getAttribute('href') || el.href || '';
                        }})()"""
                    )
                    print(f"[*] Selector check: count={count}, href={href}")
                except Exception:
                    pass
                
                # Lay links bang nodriver + querySelectorAll (giong Tab3)
                raw_links = []
                try:
                    import json as _json
                    selector_js = _json.dumps(item_selector)
                    items_json = await page.evaluate("""
                        (function(){
                            var els = Array.from(document.querySelectorAll(SELECTOR));
                            var links = els.map(function(el) {
                                if (!el) return null;
                                var href = el.getAttribute && el.getAttribute('href');
                                if (href) return href;
                                if (el.href) return el.href;
                                var a = el.querySelector ? el.querySelector('a[href]') : null;
                                if (a) return a.getAttribute('href') || a.href;
                                return null;
                            }).filter(Boolean);
                            return JSON.stringify(links);
                        })()
                    """.replace("SELECTOR", selector_js))
                    raw_links = _json.loads(items_json) if items_json else []
                    raw_links = [l for l in raw_links if isinstance(l, str) and l.strip()]
                    print(f"[Nodriver] Lay duoc {len(raw_links)} link bang querySelectorAll")
                except Exception as e:
                    print(f"[Nodriver] Loi lay link: {e}")
                    raw_links = []

                page_links = [make_absolute_url(current_url, href) for href in raw_links if href and href.strip()]
                
                # Loại bỏ links đã có trong total_links_collected (quan trọng với trang "Load more")
                # Vì trang load more giữ lại items cũ, querySelectorAll sẽ lấy cả cũ lẫn mới
                existing_links_set = set(total_links_collected)
                new_page_links = [link for link in page_links if link not in existing_links_set]
                
                total_links_collected.extend(new_page_links)
                
                print(f"[OK] Tim thay {len(page_links)} link(s) tren trang, {len(new_page_links)} link(s) moi")
                print(f"[*] Tong so link da thu thap: {len(total_links_collected)}")
                if log_callback:
                    log_callback(f"[Listing] Page {page_num}: {len(new_page_links)} new links (found {len(page_links)}), total {len(total_links_collected)}")

                if progress_callback:
                    progress_callback(
                        page_num,
                        max_pages,
                        f"Found {len(new_page_links)} new links on page {page_num}...",
                        len(total_links_collected)
                    )

                if new_page_links:
                    new_count = db.add_collected_links(
                        new_page_links,  # Chỉ thêm links mới, không thêm links đã có
                        domain=domain,
                        loaihinh=loaihinh,
                        trade_type=trade_type,
                        city_id=city_id,
                        city_name=city_name,
                        ward_id=ward_id,
                        ward_name=ward_name,
                        new_city_id=new_city_id,
                        new_city_name=new_city_name,
                        new_ward_id=new_ward_id,
                        new_ward_name=new_ward_name,
                    )
                    if progress_callback:
                        progress_callback(
                            page_num,
                            max_pages,
                            f"Saved {new_count} new links to database...",
                            len(total_links_collected)
                        )
                    
                    if new_count > 0:
                        # Reset consecutive duplicates khi CÓ link mới thực sự được thêm vào DB
                        consecutive_duplicates = 0
                        print(f"[Listing] Page {page_num}: Added {new_count} NEW links to DB.")
                        if log_callback:
                            log_callback(f"[Listing] Page {page_num}: Added {new_count} NEW links to DB")
                    else:
                        # Có link mới trong session, nhưng DB đã có rồi -> Tính là duplicate
                        duplicates_in_db = len(new_page_links)
                        consecutive_duplicates += duplicates_in_db
                        print(f"[Listing] Page {page_num}: Found {len(new_page_links)} links but ALL existed in DB. (consecutive: {consecutive_duplicates}/{MAX_CONSECUTIVE_DUPLICATES})")
                        if log_callback:
                            log_callback(f"[Listing] Page {page_num}: All found links existed in DB. Duplicates: {consecutive_duplicates}/{MAX_CONSECUTIVE_DUPLICATES}")

                else:
                    # Không có link mới trong session = tất cả đều trùng
                    duplicate_count_this_page = len(page_links)
                    consecutive_duplicates += duplicate_count_this_page
                    print(f"[Listing] Page {page_num}: 0 new links session, {duplicate_count_this_page} duplicates (consecutive: {consecutive_duplicates}/{MAX_CONSECUTIVE_DUPLICATES})")
                    if log_callback:
                        log_callback(f"[Listing] Duplicates: {consecutive_duplicates}/{MAX_CONSECUTIVE_DUPLICATES} consecutive")
                
                # Kiểm tra điều kiện dừng chung cho cả 2 trường hợp
                if consecutive_duplicates >= MAX_CONSECUTIVE_DUPLICATES:
                    stopped_due_to_duplicates = True
                    stop_msg = f"[Listing] AUTO-STOP: Detected {consecutive_duplicates} consecutive duplicate links. Stopping to avoid wasting resources."
                    print(stop_msg)
                    if log_callback:
                        log_callback(stop_msg)
                    if progress_callback:
                        progress_callback(
                            page_num,
                            max_pages,
                            f"STOPPED: {consecutive_duplicates} consecutive duplicates detected",
                            len(total_links_collected)
                        )
                    break

                pages_crawled = page_num
                
                # Optimize: Clear cache periodically to prevent VPS crash
                if page_num % 3 == 0:
                    try:
                        await page.evaluate("""
                            if ('caches' in window) {
                                caches.keys().then(names => {
                                    names.forEach(name => caches.delete(name));
                                });
                            }
                        """)
                        print(f"[INFO] Cleared browser cache at page {page_num}")
                    except Exception:
                        pass

                print(f"[DEBUG] page_num={page_num}, max_pages={max_pages}, will check next: {page_num < max_pages}")

                if page_num < max_pages:
                    WAIT_BEFORE_CLICK = random.uniform(wait_next_min, wait_next_max)
                    print(f"[DEBUG] Waiting {WAIT_BEFORE_CLICK:.1f}s before finding next button...")
                    if progress_callback:
                        progress_callback(page_num, max_pages, f"Waiting {WAIT_BEFORE_CLICK}s before clicking next...", len(total_links_collected))
                    await asyncio.sleep(WAIT_BEFORE_CLICK)
                    
                    # Bring browser to front để trigger lazy-loading/Intersection Observer
                    try:
                        await page.send(cdp.page.bring_to_front())
                        print(f"[DEBUG] Brought page to front")
                    except Exception as e:
                        print(f"[DEBUG] Could not bring to front: {e}")
                    
                    # Kiểm tra browser còn sống không
                    try:
                        test_url = await page.evaluate("window.location.href")
                        print(f"[DEBUG] Browser still alive, current URL: {test_url[:80] if test_url else 'None'}")
                    except Exception as e:
                        print(f"[DEBUG] Browser connection lost: {e}")
                        break
                    
                    # Re-inject visibility override (quan trọng khi tab bị hidden)
                    try:
                        await page.evaluate("""
                            Object.defineProperty(document, 'hidden', { value: false, writable: false, configurable: true });
                            Object.defineProperty(document, 'visibilityState', { value: 'visible', writable: false, configurable: true });
                            document.dispatchEvent(new Event('visibilitychange'));
                            // Force trigger scroll event to activate lazy-loaded elements
                            window.dispatchEvent(new Event('scroll'));
                            window.dispatchEvent(new Event('resize'));
                        """)
                        print(f"[DEBUG] Visibility override injected")
                    except Exception as e:
                        print(f"[DEBUG] Visibility override failed: {e}")
                    
                    # Scroll về cuối trang để thấy pagination (thường ở footer)
                    print(f"[DEBUG] Scrolling to bottom to find pagination...")
                    try:
                        # Scroll từ từ xuống cuối trang để trigger lazy-loading
                        scroll_height = await page.evaluate("document.body.scrollHeight")
                        for i in range(5):
                            scroll_pos = int(scroll_height * (i + 1) / 5)
                            await page.evaluate(f"window.scrollTo(0, {scroll_pos});")
                            await asyncio.sleep(0.3)
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"[DEBUG] Scroll failed: {e}")
                    
                    print(f"[DEBUG] Done waiting, now finding next button with selector: {next_page_selector}")
                    if progress_callback:
                        progress_callback(page_num, max_pages, "Dang tim nut Next...", len(total_links_collected))
                    
                    # (Debug visibility removed)

                    # === PURE JS CLICK - NO NODRIVER SELECTOR (Fix CBOR Stack Limit) ===
                    print(f"[DEBUG] Clicking next with JS...")
                    try:
                        import json as _json
                        selector_js = _json.dumps(next_page_selector or "")
                        js_click = """
                            (() => {
                                const selector = __SELECTOR__ || '';
                                let next = null;
                                let strategy = '';

                                // Strategy 1: use template selector (if any)
                                if (selector) {
                                    try { next = document.querySelector(selector); } catch (e) {}
                                    if (next) {
                                        const isNumber = next.classList && next.classList.contains('re__pagination-number');
                                        const hasRight = next.querySelector && next.querySelector('.re__icon-chevron-right--sm');
                                        // Discard non-right icon if selector picked a number link
                                        if (isNumber && !hasRight) {
                                            next = null;
                                        } else {
                                            strategy = 'selector';
                                        }
                                    }
                                }

                                // Fallback: only if selector empty
                                if (!next && !selector) {
                                    const links = Array.from(document.querySelectorAll('.re__pagination-group a'));
                                    next = links.find(a => a.querySelector('.re__icon-chevron-right--sm')) || null;
                                    if (next) strategy = 'icon';
                                }

                                if (!next) return {ok: false, reason: 'not_found'};

                                next.scrollIntoView({behavior: 'instant', block: 'center'});
                                const href = next.getAttribute('href') || next.href || '';
                                const pid = next.getAttribute('pid') || '';
                                try { next.click(); } catch (e) {}

                                return {ok: true, href, pid, strategy};
                            })()
                            """
                        js_click = js_click.replace("__SELECTOR__", selector_js)
                        clicked = await page.evaluate(js_click)
                        # nodriver may return list of key/value pairs instead of dict
                        if isinstance(clicked, list):
                            try:
                                clicked = {
                                    k: (v.get('value') if isinstance(v, dict) else v)
                                    for k, v in clicked
                                }
                            except Exception:
                                clicked = {}
                        if not isinstance(clicked, dict):
                            clicked = {}

                        if clicked and clicked.get('ok'):
                            print(f"[OK] Clicked next: {clicked.get('href', '')[:80]}")
                            if log_callback:
                                log_callback(
                                    f"[Listing] Next clicked: {clicked.get('href', '')} "
                                    f"(strategy={clicked.get('strategy','')}, pid={clicked.get('pid','')})"
                                )
                            if progress_callback:
                                progress_callback(page_num, max_pages, "Da click Next (Pure JS)...", len(total_links_collected))
                            await asyncio.sleep(5)
                            prev_url = current_url
                            try:
                                curr_url_after = await page.evaluate("location.href")
                            except Exception:
                                curr_url_after = ""
                            if log_callback:
                                log_callback(f"[Listing] URL after click: {curr_url_after}")
                            # Update current_url to avoid falling back to page 1
                            if curr_url_after:
                                current_url = curr_url_after
                            # If URL didn't change, navigate directly using href
                            try:
                                if clicked.get('href') and curr_url_after and curr_url_after == prev_url:
                                    from urllib.parse import urljoin
                                    next_url = urljoin(current_url, clicked.get('href'))
                                    if log_callback:
                                        log_callback(f"[Listing] URL not changed, forcing GET: {next_url}")
                                    await page.get(next_url)
                                    current_url = next_url
                            except Exception as nav_err:
                                if log_callback:
                                    log_callback(f"[Listing] Force GET failed: {nav_err}")
                        else:
                            reason = clicked.get('reason', 'unknown') if clicked else 'null_result'
                            print(f"[STOP] Next not found: {reason}")
                            if log_callback:
                                log_callback(f"[Listing] Stop: Next not found. Reason={reason}")
                            break

                    except Exception as e:
                        print(f"[ERROR] Click failed: {e}")
                        if log_callback:
                            log_callback(f"[Listing] Click failed: {e}")
                        import traceback
                        traceback.print_exc()
                        break
            except Exception as e:
                if log_callback:
                    log_callback(f"[Listing] ERROR page {page_num}: {e}")
                if progress_callback:
                    progress_callback(page_num, max_pages, f"Error: {str(e)}", len(total_links_collected))
                print(f"Error crawling page {page_num}: {e}")
                import traceback
                traceback.print_exc()  # In full traceback để debug
                # Thử tiếp tục với trang tiếp theo thay vì break
                # Nhưng nếu lỗi nghiêm trọng (browser đóng), thì break
                try:
                    # Kiểm tra xem browser còn sống không
                    test_url = await page.evaluate("window.location.href")
                    if not test_url:
                        break  # Browser đã đóng
                except:
                    # Browser đã đóng hoặc page không còn valid
                    print("Browser seems to be closed, stopping...")
                break

    finally:
        # NOTE: Crawl4AI crawler đã bị loại bỏ, không cần đóng nữa
        
        # Không đóng browser tự động - để user có thể xem kết quả
        # Browser sẽ tự đóng khi script kết thúc hoặc user đóng thủ công
        if progress_callback:
            progress_callback(
                pages_crawled,
                max_pages,
                f"Crawling completed. Browser will stay open for inspection.",
                len(total_links_collected)
            )
        print("Crawling completed. Browser will stay open.")

    unique_links = list(dict.fromkeys(total_links_collected))
    if log_callback:
        log_callback(f"[Listing] DONE: crawled {pages_crawled} page(s), total unique links {len(unique_links)}")

    return {
        'success': True,
        'total_links': len(unique_links),
        'pages_crawled': pages_crawled,
        'new_links_added': len(unique_links),  # db already handles duplicates
        'canceled': canceled,
        'stopped_due_to_duplicates': stopped_due_to_duplicates,
        'consecutive_duplicates': consecutive_duplicates
    }
