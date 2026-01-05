"""
Core scraping helpers for scheduler and background services.
Uses lxml extraction to support CSS/XPath selectors.
"""

import asyncio
import random
import re
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urlparse

from lxml import html as lxml_html

from web_scraper import WebScraper


def parse_latlng_from_url(url: str) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    try:
        m = re.search(r'(?:center|q)=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)', url, re.IGNORECASE)
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
    return ("batdongsan.com.vn" in host) or ("nhatot.com" in host)


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
                const findBdsPhone = () => {
                    const bdsSel = '.js__phone-event[mobile], .js__phone[mobile], .phoneEvent[mobile]';
                    const bdsNode = document.querySelector(bdsSel);
                    return readPhone(bdsNode);
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
                const el = findEl();
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


def format_extracted_data_fixed(extracted_data: Any, template: Dict) -> Dict:
    if not isinstance(extracted_data, dict):
        return {}
    formatted = {}
    for field in template.get('fields', []):
        name = field.get('name')
        val = extracted_data.get(name)
        value_type = field.get('valueType') or field.get('type', 'text')
        val = _apply_exclude_words(val, field)
        formatted[name] = val if val else None
    return formatted


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


async def scrape_url(
    url: str,
    template: Dict,
    scraper: Optional[WebScraper] = None,
    wait_load_min: float = 0.0,
    wait_load_max: float = 0.0,
    show_browser: bool = True,
    fake_scroll: bool = False,
    fake_hover: bool = False,
) -> Dict[str, Any]:
    """
    Scrape a single URL using lxml so XPath is supported.
    """
    try:
        if wait_load_max and wait_load_max > 0:
            wait_time = random.uniform(wait_load_min or 0.0, wait_load_max)
            if wait_time > 0:
                await asyncio.sleep(wait_time)

        if not isinstance(url, str) or not url.startswith(("http://", "https://", "file://", "raw:")):
            return {'success': False, 'url': url, 'error': 'Invalid URL', 'timestamp': datetime.now().isoformat()}

        result = None
        html_content = ""
        tree_before = None
        phone_selector = None
        phone_override = None
        
        # Ưu tiên dùng display_page (trình duyệt hiển thị) để cào - không fallback sang scrape_simple ẩn
        if scraper and show_browser and hasattr(scraper, "display_page") and scraper.display_page:
            page = scraper.display_page
            try:
                print(f"[scrape_url] Navigating display_page to: {url[:100]}")
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass
                try:
                    await page.bring_to_front()
                except Exception:
                    pass

                # Thao tác hover/scroll giả nếu yêu cầu
                if fake_hover:
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
                if fake_scroll:
                    try:
                        # Cuộn xuống từ từ để load lazy content, rồi cuộn lại lên đầu
                        await page.evaluate("""() => {
                            return new Promise((resolve) => {
                                const doc = document.scrollingElement || document.documentElement || document.body;
                                const height = doc?.scrollHeight || document.body.scrollHeight || 2000;
                                let pos = 0;
                                const steps = Math.max(4, Math.min(8, Math.floor(height / 400)));
                                let currentStep = 0;
                                
                                function doScroll() {
                                    if (currentStep < steps) {
                                        pos = Math.min(height, pos + Math.floor(height / steps) + Math.floor(Math.random()*100));
                                        window.scrollTo(0, pos);
                                        currentStep++;
                                        setTimeout(doScroll, 150 + Math.random()*200);
                                    } else {
                                        // Cuộn lại lên đầu để lấy nội dung chính
                                        setTimeout(() => {
                                            window.scrollTo(0, 0);
                                            resolve();
                                        }, 300);
                                    }
                                }
                                doScroll();
                            });
                        }""")
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"[scrape_url] fake_scroll error: {e}")

                html_before_click = ""
                tree_before = None
                phone_selector = None
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
                current_url = page.url or url
                print(f"[scrape_url] Got HTML from display_page, length={len(html_content)}, url={current_url[:80]}")
                result = {"success": True, "url": current_url, "html": html_content}
            except Exception as e:
                error_msg = str(e)
                print(f"[scrape_url] display_page error: {error_msg}")
                result = {"success": False, "url": url, "error": error_msg, "html": ""}
        else:
            # Fallback khi không có display_page (headless mode hoặc display_page không tồn tại)
            print(f"[scrape_url] No display_page, using scrape_simple for: {url[:100]}")
            if scraper:
                result = await scraper.scrape_simple(url, bypass_cache=True)
            else:
                async with WebScraper(headless=not show_browser, verbose=False) as new_scraper:
                    result = await new_scraper.scrape_simple(url, bypass_cache=True)

        if not result.get('success'):
            return {'success': False, 'url': url, 'error': result.get('error'), 'timestamp': datetime.now().isoformat()}

        if not html_content:
            html_content = result.get('html', '')
        if not html_content:
            return {'success': False, 'url': url, 'error': 'Empty HTML', 'timestamp': datetime.now().isoformat()}

        tree = lxml_html.fromstring(html_content)
        extracted_data = {}

        for field in template.get('fields', []):
            field_name = field.get('name')
            selector = (field.get('selector') or field.get('cssSelector') or field.get('xpath') or '').strip()
            value_type = field.get('valueType', 'text')
            if not selector:
                extracted_data[field_name] = None
                continue

            try:
                if selector.startswith('/') or selector.startswith('('):
                    elements = tree.xpath(selector)
                else:
                    elements = tree.cssselect(selector)

                values = []

                def _add_value(val):
                    if val and val not in values:
                        values.append(val)
                for el in elements:
                    if value_type == 'src':
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
                        elif hasattr(el, 'xpath'):
                            child_media = el.xpath('.//img|.//video|.//source|.//iframe')
                            for media in child_media:
                                val = media.get('data-src') or media.get('data-lazy-src') or media.get('src')
                                _add_value(val)
                    elif value_type == 'href':
                        if hasattr(el, 'tag') and el.tag == 'a':
                            val = el.get('href')
                            _add_value(val)
                        elif hasattr(el, 'xpath'):
                            child_as = el.xpath('.//a')
                            for a in child_as:
                                val = a.get('href')
                                _add_value(val)
                    elif value_type == 'html':
                        val = _get_inner_html(el)
                        _add_value(val)
                    else:
                        val = None
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
                        if not val:
                            val = el if isinstance(el, str) else el.text_content().strip()
                        if value_type in ('text', 'innerText'):
                            val = _apply_exclude_words(val, field)
                        _add_value(val)

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
                    extracted_data[field_name] = values
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
                        extracted_data[field_name] = values[0]
            except Exception as e:
                print(f"Error extract field '{field_name}': {e}")
                extracted_data[field_name] = None

        # Debug log: hiển thị dữ liệu đã extract
        non_empty_fields = {k: v for k, v in extracted_data.items() if v is not None}
        print(f"[scrape_url] Extracted {len(non_empty_fields)}/{len(extracted_data)} fields with data")
        if non_empty_fields:
            for k, v in list(non_empty_fields.items())[:5]:
                val_preview = str(v)[:80] if v else "None"
                print(f"  - {k}: {val_preview}")
        else:
            print(f"[scrape_url] WARNING: No data extracted! Template fields: {[f.get('name') for f in template.get('fields', [])]}")

        formatted_data = format_extracted_data_fixed(extracted_data, template)
        return {
            'success': True,
            'url': url,
            'data': formatted_data,
            'html': html_content,
            'timestamp': datetime.now().isoformat()
        }

    except Exception as e:
        return {'success': False, 'url': url, 'error': str(e), 'timestamp': datetime.now().isoformat()}
