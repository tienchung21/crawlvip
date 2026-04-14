"""
Simple listing crawler used by scheduler (same behavior as Tab 3).
"""
import asyncio
import os
import random
import sys
from typing import Optional
from urllib.parse import urljoin

# IMPORTANT: Bypass ALL proxy to avoid ERR_TUNNEL_CONNECTION_FAILED
os.environ['no_proxy'] = '*'
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)
os.environ.pop('HTTP_PROXY', None)
os.environ.pop('HTTPS_PROXY', None)

import nodriver as uc


BROWSER_CONFIG_TIET_KIEM = [
    "--mute-audio",
    # Anti-detection flags
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]


async def crawl_listing_simple(
    target_url: str,
    item_selector: str,
    next_selector: str,
    max_pages: int,
    db,
    progress_callback=None,
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
):
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    browser = None
    total_links = []
    new_links_total = 0

    try:
        # fallback selectors for batdongsan if template is empty/XPath
        if (not item_selector) or item_selector.strip().startswith(("/", "(")):
            if "batdongsan.com.vn" in target_url:
                item_selector = ".js__product-link-for-product-id"
        if not next_selector:
            if "batdongsan.com.vn" in target_url:
                next_selector = ".re__pagination-icon > .re__icon-chevron-right--sm"
        print(f"[Scheduler Listing] Using item_selector: {item_selector}")
        print(f"[Scheduler Listing] Using next_selector: {next_selector}")

        # Dùng profile có suffix để share cookie với listing_crawler.py
        profile_name = "nodriver_profile_listing" + (f"_{profile_suffix}" if profile_suffix else "")
        profile_dir_listing = os.path.join(os.path.dirname(os.path.abspath(__file__)), profile_name)
        os.makedirs(profile_dir_listing, exist_ok=True)
        print(f"[Scheduler Listing] Using profile: {profile_dir_listing}")

        # Cảnh báo nếu headless với site có Cloudflare (có thể bị chặn)
        if "batdongsan.com.vn" in target_url or "nhatot.com" in target_url:
            if not show_browser:
                print("[Scheduler Listing] WARNING: Headless mode với Cloudflare site - có thể bị chặn 'Just a moment...'")

        browser = await uc.start(
            headless=not show_browser,
            browser_args=BROWSER_CONFIG_TIET_KIEM,
            user_data_dir=profile_dir_listing,
            sandbox=False  # <--- FIX: Tránh lỗi "Failed to connect to browser"
        )

        page = await browser.get(target_url)
        
        # Chờ Cloudflare challenge hoàn thành (nếu có)
        cloudflare_wait_attempts = 0
        max_cloudflare_wait = 10  # Tối đa 10 lần check = 30 giây
        while cloudflare_wait_attempts < max_cloudflare_wait:
            try:
                title = await page.evaluate("document.title")
                if title and "just a moment" in title.lower():
                    cloudflare_wait_attempts += 1
                    print(f"[Scheduler Listing] Cloudflare challenge detected, waiting... ({cloudflare_wait_attempts}/{max_cloudflare_wait})")
                    await asyncio.sleep(3)
                else:
                    break
            except Exception:
                break
        
        if cloudflare_wait_attempts >= max_cloudflare_wait:
            print("[Scheduler Listing] WARNING: Cloudflare challenge timeout - page may not load correctly")
        
        await asyncio.sleep(random.uniform(wait_load_min, wait_load_max))

        for current_page in range(1, max_pages + 1):
            if progress_callback:
                progress_callback(current_page, max_pages, f"Page {current_page}/{max_pages}", len(total_links))

            # Fake scroll - upgraded logic from listing_crawler.py
            if enable_fake_scroll:
                try:
                    print("[Scheduler Listing] Starting fake scroll...")
                    before_y = None
                    try:
                        before_y = await page.evaluate("() => window.scrollY || window.pageYOffset || 0")
                    except Exception:
                        before_y = None
                    
                    scrolled = False
                    # Try native scroll_down first (more realistic)
                    if hasattr(page, "scroll_down"):
                        print("[Scheduler Listing] Using native scroll_down()")
                        steps = random.randint(12, 24)
                        for _ in range(steps):
                            await page.scroll_down(random.randint(3, 8))
                            await asyncio.sleep(random.uniform(0.4, 1.0))
                        scrolled = True
                    
                    # Fallback to manual scrollTo
                    if not scrolled:
                        print("[Scheduler Listing] Using manual scrollTo()")
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
                        
                        # Check if window scroll worked
                        after_y = await page.evaluate("() => window.scrollY || window.pageYOffset || 0")
                        print(f"[Scheduler Listing] Window scroll: {before_y} -> {after_y}")
                        
                        # If window didn't scroll, try scrolling scrollable containers
                        if after_y == before_y:
                            print("[Scheduler Listing] Window didn't scroll, trying scrollable containers...")
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
                                        console.log('[Scroll] Scrolled container by {step}px');
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
                    if before_y is not None and after_y is not None:
                        print(f"[Scheduler Listing] Final scroll position: {before_y} -> {after_y}")
                except Exception as e:
                    print(f"[Scheduler Listing] Fake scroll error: {e}")
            # Fake hover
            if enable_fake_hover:
                try:
                    width = await page.evaluate("() => window.innerWidth || 1200")
                    height = await page.evaluate("() => window.innerHeight || 800")
                    for _ in range(3):
                        x = random.randint(0, max(int(width) - 1, 1))
                        y = random.randint(0, max(int(height) - 1, 1))
                        await page.mouse.move(x, y)
                        await asyncio.sleep(0.2)
                except Exception:
                    pass

            # Extract links via JS querySelectorAll (with retries)
            page_links = []
            try:
                import json as _json
                selector_js = _json.dumps(item_selector)
                js_code = """
                    (() => {
                        const getLink = (el) => {
                            if (!el) return null;
                            let href = (el.getAttribute && el.getAttribute('href')) || el.href || null;
                            if (href) return href;
                            href = el.getAttribute && (el.getAttribute('data-href') || el.getAttribute('data-url') || el.getAttribute('data-detail-url'));
                            if (href) return href;
                            const a = el.querySelector ? el.querySelector('a[href]') : null;
                            if (a) return a.getAttribute('href') || a.href;
                            return null;
                        };
                        const fromDoc = (doc) => {
                            const els = Array.from(doc.querySelectorAll(SELECTOR));
                            const links = els.map(getLink).filter(Boolean);
                            const sample = els[0] ? {
                                tag: els[0].tagName,
                                hrefAttr: els[0].getAttribute && els[0].getAttribute('href'),
                                hrefProp: els[0].href || null,
                                dataHref: els[0].getAttribute && els[0].getAttribute('data-href'),
                                dataUrl: els[0].getAttribute && els[0].getAttribute('data-url'),
                                outer: els[0].outerHTML ? els[0].outerHTML.slice(0, 200) : null
                            } : null;
                            return { count: els.length, sample, links };
                        };
                        const main = fromDoc(document);
                        const iframeInfo = [];
                        const iframes = Array.from(document.querySelectorAll("iframe"));
                        for (const f of iframes) {
                            try {
                                const doc = f.contentDocument || (f.contentWindow && f.contentWindow.document);
                                if (!doc) continue;
                                const res = fromDoc(doc);
                                iframeInfo.push({ src: f.src || "", count: res.count, sample: res.sample, links: res.links });
                            } catch (e) {
                                iframeInfo.push({ src: f.src || "", error: "cross-origin" });
                            }
                        }
                        return JSON.stringify({
                            url: location.href,
                            title: document.title,
                            count: main.count,
                            sample: main.sample,
                            links: main.links,
                            iframeCount: iframes.length,
                            iframes: iframeInfo
                        });
                    })()
                """.replace("SELECTOR", selector_js)

                attempts = 3
                raw_links = []
                for attempt in range(1, attempts + 1):
                    items_json = await page.evaluate(js_code)
                    parsed = _json.loads(items_json) if items_json else {}
                    if not isinstance(parsed, dict):
                        parsed = {}
                    count = parsed.get("count", 0)
                    sample = parsed.get("sample")
                    raw_links = parsed.get("links") or []
                    if not raw_links and parsed.get("iframes"):
                        for info in parsed.get("iframes", []):
                            info_links = info.get("links") or []
                            if info_links:
                                raw_links = info_links
                                break
                    if attempt == 1:
                        print(f"[Scheduler Listing] Page URL: {parsed.get('url')}")
                        print(f"[Scheduler Listing] Page title: {parsed.get('title')}")
                        print(f"[Scheduler Listing] iframe count: {parsed.get('iframeCount')}")
                    print(f"[Scheduler Listing] DOM count for selector: {count}")
                    if sample:
                        print(f"[Scheduler Listing] Sample element: {sample}")
                    if raw_links:
                        break
                    await asyncio.sleep(1.5)

                for link in raw_links:
                    if link and isinstance(link, str):
                        if link.startswith("/"):
                            link = urljoin(target_url, link)
                        page_links.append(link)
                
                print(f"[Scheduler Listing] Extracted {len(page_links)} links from raw_links")
            except Exception as e:
                print(f"[Scheduler Listing] Extract error: {e}")
                import traceback
                traceback.print_exc()
                page_links = []

            total_links.extend(page_links)
            print(f"[Scheduler Listing] Page {current_page}: found {len(page_links)} link(s), total so far: {len(total_links)}")
            if page_links:
                new_count = db.add_collected_links(
                    page_links,
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
                new_links_total += new_count
                print(f"[Scheduler Listing] Saved {new_count} new links")

            # Next page
            if current_page < max_pages and next_selector:
                try:
                    print(f"[Scheduler Listing] Looking for next page button with selector: {next_selector}")
                    await asyncio.sleep(random.uniform(wait_next_min, wait_next_max))
                    # Pure JS click to avoid nodriver select (CBOR stack limit)
                    clicked = await page.evaluate(
                        """
                        (() => {
                            let el = null;
                            try {
                                el = document.querySelector(arguments[0]);
                            } catch (e) {}
                            if (!el) {
                                const links = Array.from(document.querySelectorAll('a.re__pagination-icon'));
                                el = links.find(a => a.querySelector('.re__icon-chevron-right--sm')) || links[links.length - 1];
                            }
                            if (!el) return {ok:false, reason:'not_found'};
                            const href = el.getAttribute('href') || el.href || '';
                            el.scrollIntoView({behavior:'instant', block:'center'});
                            try { el.click(); } catch (e) {}
                            return {ok:true, href};
                        })()
                        """,
                        next_selector,
                    )
                    if clicked and clicked.get("ok"):
                        print(f"[Scheduler Listing] Clicked next (href={clicked.get('href','')[:80]})")
                        await asyncio.sleep(2)
                    else:
                        print(f"[Scheduler Listing] Next not found - stopping at page {current_page}. Reason={clicked}")
                        break
                except Exception as e:
                    print(f"[Scheduler Listing] Error clicking next button: {e} - stopping at page {current_page}")
                    break

        return {
            "success": True,
            "total_links": len(total_links),
            "pages_crawled": max_pages,
            "new_links_added": new_links_total,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "total_links": len(total_links),
            "pages_crawled": 0,
            "new_links_added": 0,
        }
    finally:
        if browser:
            try:
                await browser.stop()
            except Exception:
                pass
