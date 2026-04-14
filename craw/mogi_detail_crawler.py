#!/usr/bin/env python3
"""
Mogi Detail Crawler - Multi-threaded script to scrape detail pages.
Reads PENDING links from collected_links, scrapes using mogidetails template,
saves to scraped_details_flat with location IDs extracted from URL.

Usage: python mogi_detail_crawler.py [--threads 3] [--batch 20] [--delay 1.5]
"""

import os
import re
import sys
import time
import json
import random
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html as lxml_html

# Setup path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database

# === CONFIGURATION ===
DEFAULT_THREADS = 3
DEFAULT_BATCH_SIZE = 20
DEFAULT_DELAY_MIN = 1.0
DEFAULT_DELAY_MAX = 2.0

# Load template
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'template', 'mogidetails.json')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Thread lock for DB operations
db_lock = threading.Lock()


def load_template() -> List[Dict]:
    """Load mogidetails.json template fields."""
    with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        template = json.load(f)
    return template.get('fields', [])


def extract_location_ids_from_url(url: str) -> Dict[str, Optional[int]]:
    """
    Extract location IDs from Mogi URL or page meta.
    Looking for patterns like: did=295&cid=24 or "did":295,"psid":9
    Returns dict with city_id, district_id, ward_id (psid)
    """
    result = {
        'mogi_city_id': None,
        'mogi_district_id': None,
        'mogi_ward_id': None,
    }
    
    # Try to extract from URL query params
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    
    if 'cid' in query:
        try:
            result['mogi_city_id'] = int(query['cid'][0])
        except:
            pass
    if 'did' in query:
        try:
            result['mogi_district_id'] = int(query['did'][0])
        except:
            pass
    if 'psid' in query:  # Ward ID
        try:
            result['mogi_ward_id'] = int(query['psid'][0])
        except:
            pass
    
    return result


def extract_location_ids_from_html(html: str) -> Dict[str, Optional[int]]:
    """
    Extract location IDs from page HTML/JavaScript.
    Look for patterns in pageData variable and MarketPriceUrl.
    Priority:
    - mogi_street_id: "streetId": 9427
    - mogi_district_id: "did": 295
    - mogi_city_id: "cid": 24 or inside MarketPriceUrl
    - mogi_ward_id: "wid": 4021 (inside MarketPriceUrl) or "psid" if wid is missing (fallback)
    """
    result = {
        'mogi_city_id': None,
        'mogi_district_id': None,
        'mogi_ward_id': None,
        'mogi_street_id': None,
    }
    
    # 1. District ID (did)
    did_match = re.search(r'"did"\s*:\s*(\d+)', html)
    if did_match:
        result['mogi_district_id'] = int(did_match.group(1))
        
    # 2. Street ID (streetId)
    street_match = re.search(r'"streetId"\s*:\s*(\d+)', html)
    if street_match:
        result['mogi_street_id'] = int(street_match.group(1))

    # 3. City ID (cid)
    # Check "cid": 24 pattern first
    cid_match = re.search(r'"cid"\s*:\s*(\d+)', html)
    if cid_match:
        result['mogi_city_id'] = int(cid_match.group(1))
    else:
        # Check inside MarketPriceUrl or any url params in JS: &cid=24 or ?cid=24
        cid_url_match = re.search(r'[?&]cid=(\d+)', html)
        if cid_url_match:
            result['mogi_city_id'] = int(cid_url_match.group(1))
            
    # 4. Ward ID (wid) - This is likely 'wid' in URL params, NOT 'psid'
    # Check URL params: &wid=4021
    wid_match = re.search(r'[?&]wid=(\d+)', html)
    if wid_match:
        result['mogi_ward_id'] = int(wid_match.group(1))
    else:
        # Fallback to psid if wid not found (preserving old logic just in case, or user suggestion)
        psid_match = re.search(r'"psid"\s*:\s*(\d+)', html)
        if psid_match:
            # Note: psid might not be Ward ID, but keeping as fallback if wid is absent
            result['mogi_ward_id'] = int(psid_match.group(1))
    
    return result


def extract_phone_from_html(html: str) -> Optional[str]:
    """Extract phone number from JS variables or angular bindings."""
    # 1. Try AgentMobile in pageData
    agent_mobile = re.search(r'"AgentMobile"\s*:\s*"(\d+)"', html)
    if agent_mobile:
        return agent_mobile.group(1)
        
    # 2. Try PhoneFormat binding
    phone_format = re.search(r"PhoneFormat\('(\d+)'\)", html)
    if phone_format:
        return phone_format.group(1)
        
    return None


def extract_value_by_selector(tree, selector: str, value_type: str = 'text') -> Optional[str]:
    """Extract value from HTML tree using selector (CSS or XPath)."""
    try:
        if selector.startswith('//'):
            # XPath
            elements = tree.xpath(selector)
        else:
            # CSS selector - convert to xpath
            from lxml.cssselect import CSSSelector
            css_sel = CSSSelector(selector)
            elements = css_sel(tree)
        
        if not elements:
            return None
        
        element = elements[0]
        
        if value_type == 'text':
            return element.text_content().strip() if hasattr(element, 'text_content') else str(element).strip()
        elif value_type == 'src':
            # For images, get src attribute
            if hasattr(element, 'get'):
                return element.get('src')
            # Return count of images if gallery
            img_elements = tree.xpath('//div[@id="gallery"]//img/@src')
            return str(len(img_elements)) if img_elements else "0"
        elif value_type == 'src_list':
            # Specific for Mogi gallery
            if selector == '#gallery' or 'gallery' in selector:
                # Get both src and data-src
                urls = []
                # 1. Get all img elements in gallery
                imgs = tree.xpath('//div[@id="gallery"]//img')
                for img in imgs:
                    # Prefer data-src (lazy load), fallback to src
                    src = img.get('data-src') or img.get('src')
                    if src and src not in urls:
                        urls.append(src)
                return urls
            
            # Generic list of src
            urls = []
            for el in elements:
                if hasattr(el, 'get'):
                    s = el.get('src')
                    if s: urls.append(s)
            return urls
        elif value_type == 'attribute':
            return element.get('href') if hasattr(element, 'get') else None
        elif value_type == 'html':
            # Return inner HTML to preserve tags like <br>
            # tostring returns bytes, so decode. with_tail=False avoids getting text after the element.
            from lxml import html as lxml_html
            # Note: tostring includes the tag itself (e.g. <div>...</div>). 
            # If we want INNER html, we might need to strip the outer tag or iterate children.
            # But usually receiving the div with <br> inside is fine.
            return lxml_html.tostring(element, encoding='unicode', with_tail=False).strip()
        else:
            return element.text_content().strip() if hasattr(element, 'text_content') else None
    except Exception as e:
        return None


def scrape_detail_page(driver, url: str, template_fields: List[Dict]) -> Dict:
    """Scrape a detail page and return extracted data."""
    data = {}
    
    try:
        driver.get(url)
        # Increase wait time for JS to load
        time.sleep(random.uniform(3.0, 5.0))
        
        page_source = driver.page_source
        
        # Check for errors
        if '404' in driver.title.lower() or 'không tìm thấy' in driver.title.lower():
            return {'_error': '404 - Page not found'}
        
        if 'cloudflare' in driver.title.lower():
            time.sleep(5)
            page_source = driver.page_source
        
        # Parse HTML with lxml
        tree = lxml_html.fromstring(page_source)
        
        # Extract location IDs from HTML
        location_ids = extract_location_ids_from_html(page_source)
        data.update(location_ids)
        
        # Also try from URL
        url_ids = extract_location_ids_from_url(url)
        for k, v in url_ids.items():
            if v and not data.get(k):
                data[k] = v
        
        # Extract fields based on template
        for field in template_fields:
            name = field.get('name')
            selector = field.get('selector')
            value_type = field.get('valueType', 'text')
            
            if not name or not selector:
                continue
            
            value = extract_value_by_selector(tree, selector, value_type)
            if value:
                data[name] = value

        # Extract phone from JS (OVERRIDE template result because template gets masked phone)
        phone = extract_phone_from_html(page_source)
        if phone:
            data['sodienthoai'] = phone
        # Removed logging as per user request
        
        # Special handling for map - extract coordinates from iframe src OR google-maps-link
        # Always try to find map iframe or link regardless of template result
        try:
            # 1. Try iframe src/data-src (CHECK BOTH like fast_crawler)
            map_iframes = tree.xpath('//div[contains(@class,"map-content")]//iframe')
            found_map = False
            for iframe in map_iframes:
                # Check src then data-src
                src = iframe.get('src')
                if not src or 'google.com/maps' not in src:
                    src = iframe.get('data-src')
                
                if src:
                    coord_match = re.search(r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)', src)
                    if coord_match:
                        data['map'] = f"{coord_match.group(1)},{coord_match.group(2)}"
                        found_map = True
                        break
            
            # 2. If valid map not found, try NEW method (google-maps-link)
            if not found_map:
                map_links = tree.xpath('//div[contains(@class,"google-maps-link")]//a/@href')
                if map_links:
                    href = map_links[0]
                    # Format: maps?ll=21.055636,105.867107&...
                    # Regex for ll=lat,long
                    ll_match = re.search(r'[?&]ll=(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
                    if ll_match:
                        data['map'] = f"{ll_match.group(1)},{ll_match.group(2)}"
                    else:
                        # Fallback try q=lat,long inside href
                        q_match = re.search(r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
                        if q_match:
                            data['map'] = f"{q_match.group(1)},{q_match.group(2)}"
                            
        except Exception as e:
            logger.warning(f"Failed to extract map coordinates: {e}")
        
        # Extract thuocduan (project name) via projectid API
        try:
            # Extract projectid from page source
            # Pattern: fetchText('/template/ProjectInfo?projectid=' + 0,
            pid_match = re.search(r"projectid='\s*\+\s*(\d+)", page_source)
            if not pid_match:
                pid_match = re.search(r"projectid=(\d+)", page_source)
                
            if pid_match:
                project_id = int(pid_match.group(1))
                if project_id > 0:
                    # Fetch project info via API
                    import requests
                    api_url = f"https://mogi.vn/template/ProjectInfo?projectid={project_id}"
                    try:
                        resp = requests.get(api_url, timeout=5, headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
                        })
                        if resp.status_code == 200:
                            p_tree = lxml_html.fromstring(resp.text)
                            from lxml.cssselect import CSSSelector
                            titles = CSSSelector('.project-info .project-title')(p_tree)
                            if titles:
                                data['thuocduan'] = titles[0].text_content().strip()
                    except Exception as api_err:
                        pass  # Silent fail for API call
        except Exception as e:
            pass  # Silent fail for project extraction
        
        # Parse address into components - Disabled as per user request to speed up
        # if 'diachi' in data:
        #     parse_address_components(data)
        
    except Exception as e:
        data['_error'] = str(e)
        logger.error(f"Error scraping {url}: {e}")
    
    return data


def parse_address_components(data: Dict):
    """Parse diachi into street_ext, ward_ext, district_ext, city_ext."""
    diachi = data.get('diachi', '')
    if not diachi:
        return
    
    # Split by comma
    parts = [p.strip() for p in diachi.split(',')]
    
    # Typical format: "Street, Ward, District, City"
    if len(parts) >= 4:
        data['street_ext'] = parts[0]
        data['ward_ext'] = parts[1]
        data['district_ext'] = parts[2]
        data['city_ext'] = parts[3]
    elif len(parts) == 3:
        data['ward_ext'] = parts[0]
        data['district_ext'] = parts[1]
        data['city_ext'] = parts[2]
    elif len(parts) == 2:
        data['district_ext'] = parts[0]
        data['city_ext'] = parts[1]
    elif len(parts) == 1:
        data['city_ext'] = parts[0]


class MogiDetailCrawler:
    def __init__(self, db: Database, delay_min: float = 1.0, delay_max: float = 2.0, limit: int = 0):
        self.db = db
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.limit = limit
        self.should_stop = False
        self.template_fields = load_template()
        self.stats = {
            'total': 0,
            'success': 0,
            'error': 0,
        }
        self.stats_lock = threading.Lock()
    
    def stop(self):
        self.should_stop = True
    
    def _start_driver(self, proxy: Optional[str] = None):
        """Start a new SeleniumBase driver."""
        return Driver(uc=True, headless=True, proxy=proxy)
    
    def worker(self, worker_id: int, batch_size: int, domain: str, trade_type: Optional[str], proxy: Optional[str]):
        """Worker thread that processes links."""
        logger.info(f"Worker {worker_id} started")
        driver = None
        
        try:
            driver = self._start_driver(proxy)
            
            while not self.should_stop:
                # Check limit before getting batch
                if self.limit > 0:
                    with self.stats_lock:
                        if self.stats['total'] >= self.limit:
                            break
                            
                # Get batch of pending links
                # Get batch of pending links
                links = []
                with db_lock:
                    pending_links = self.db.get_pending_links(
                        limit=batch_size,
                        domain=domain,
                        trade_type=trade_type
                    )
                    
                    # Mark extracted links as PROCESSING to avoid other workers picking them up
                    if pending_links:
                        for link in pending_links:
                            self.db.update_link_status(link['url'], 'PROCESSING')
                            links.append(link)
                
                if not links:
                    logger.info(f"Worker {worker_id}: No more pending links")
                    break
                
                for link_info in links:
                    if self.should_stop:
                        break
                    
                    if self.limit > 0:
                        with self.stats_lock:
                            if self.stats['total'] >= self.limit:
                                break
                    
                    link_id = link_info['id']
                    url = link_info['url']
                    loaihinh = link_info.get('loaihinh')
                    link_trade_type = link_info.get('trade_type')
                    
                    logger.info(f"Worker {worker_id}: Processing link {link_id} - {url[:60]}...")
                    
                    retry_count = 0
                    max_retries = 3
                    
                    while retry_count < max_retries:
                        try:
                            # Scrape the page
                            data = scrape_detail_page(driver, url, self.template_fields)
                            
                            # Remove phone logging as per user request
                            
                            if '_error' in data:
                                error = data.pop('_error')
                                
                                # Don't retry 404
                                if '404' in str(error):
                                    logger.warning(f"Worker {worker_id}: 404 - Page not found for {link_id}")
                                    with db_lock:
                                        self.db.update_link_status(url, 'ERROR')
                                    with self.stats_lock:
                                        self.stats['error'] += 1
                                    break
                                
                                # Retryable error
                                retry_count += 1
                                if retry_count < max_retries:
                                    logger.warning(f"Worker {worker_id}: Error {error} on {link_id}. Retrying {retry_count}/{max_retries}...")
                                    try: driver.quit()
                                    except: pass
                                    driver = self._start_driver(proxy)
                                    time.sleep(2)
                                    continue
                                else:
                                    logger.error(f"Worker {worker_id}: Max retries reached for {link_id}. Error: {error}")
                                    with db_lock:
                                        self.db.update_link_status(url, 'ERROR')
                                    with self.stats_lock:
                                        self.stats['error'] += 1
                                    break
                            
                            # Success
                            with db_lock:
                                detail_id = self.db.add_scraped_detail_flat(
                                    url=url,
                                    data=data,
                                    domain='mogi',
                                    link_id=link_id,
                                    loaihinh=loaihinh,
                                    trade_type=link_trade_type
                                )
                                if detail_id and 'img' in data and isinstance(data['img'], list):
                                    self.db.add_detail_images(detail_id, data['img'])
                                    # Update img count in data for flat table if needed, 
                                    # but flat table 'img_count' might be derived or separate.
                                    # For now, just save images.
                                
                                self.db.update_link_status(url, 'DONE')
                            
                            with self.stats_lock:
                                self.stats['success'] += 1
                                self.stats['total'] += 1
                            
                            logger.info(f"Worker {worker_id}: Saved link {link_id} ✓")
                            
                            # Delay between requests
                            time.sleep(random.uniform(self.delay_min, self.delay_max))
                            break # Success, break retry loop

                        except Exception as e:
                            retry_count += 1
                            if retry_count < max_retries:
                                logger.warning(f"Worker {worker_id}: Exception {e} on {link_id}. Retrying {retry_count}/{max_retries}...")
                                try: driver.quit()
                                except: pass
                                driver = self._start_driver(proxy)
                                time.sleep(2)
                            else:
                                logger.error(f"Worker {worker_id}: Max retries (exception) for {link_id}: {e}")
                                with db_lock:
                                    self.db.update_link_status(url, 'ERROR')
                                with self.stats_lock:
                                    self.stats['error'] += 1
        
        except Exception as e:
            logger.error(f"Worker {worker_id}: Critical error: {e}")
        
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            logger.info(f"Worker {worker_id} finished")
    
    def run(self, threads: int, batch_size: int, domain: str = 'mogi.vn', 
            trade_type: Optional[str] = None, proxy: Optional[str] = None):
        """Run the crawler with multiple threads."""
        logger.info(f"Starting Mogi Detail Crawler with {threads} threads")
        logger.info(f"Domain: {domain}, Trade type: {trade_type or 'all'}")
        logger.info(f"Batch size: {batch_size}, Delay: {self.delay_min}-{self.delay_max}s")
        if self.limit > 0:
            logger.info(f"Limit: {self.limit} links")
        if proxy:
            logger.info(f"Proxy: {proxy}")
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            for i in range(threads):
                future = executor.submit(
                    self.worker, 
                    worker_id=i+1, 
                    batch_size=batch_size,
                    domain=domain,
                    trade_type=trade_type,
                    proxy=proxy
                )
                futures.append(future)
            
            # Wait for all workers
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Worker exception: {e}")
        
        logger.info("=" * 50)
        logger.info("CRAWL COMPLETED")
        logger.info(f"Total processed: {self.stats['total']}")
        logger.info(f"Success: {self.stats['success']}")
        logger.info(f"Errors: {self.stats['error']}")
        logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description='Mogi Detail Crawler')
    parser.add_argument('--threads', type=int, default=DEFAULT_THREADS, help='Number of threads')
    parser.add_argument('--batch', type=int, default=DEFAULT_BATCH_SIZE, help='Batch size per thread')
    parser.add_argument('--delay-min', type=float, default=DEFAULT_DELAY_MIN, help='Min delay between requests')
    parser.add_argument('--delay-max', type=float, default=DEFAULT_DELAY_MAX, help='Max delay between requests')
    parser.add_argument('--trade-type', type=str, default=None, help='Filter by trade type (mua/thue)')
    parser.add_argument('--proxy', type=str, default=None, help='Proxy server (http://host:port)')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of links to process (0=unlimited)')
    
    args = parser.parse_args()
    
    # Try proxy from env if not specified
    proxy = args.proxy or os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
    
    db = Database()
    crawler = MogiDetailCrawler(
        db=db,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        limit=args.limit
    )
    
    try:
        crawler.run(
            threads=args.threads,
            batch_size=args.batch,
            domain='mogi.vn',
            trade_type=args.trade_type,
            proxy=proxy
        )
    except KeyboardInterrupt:
        logger.info("Stopping crawler...")
        crawler.stop()

if __name__ == '__main__':
    main()
