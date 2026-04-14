#!/usr/bin/env python3
"""
Mogi Fast Crawler - Uses requests + lxml for high-speed scraping.
No Selenium involved. Checks for Cloudflare; if hit, retries or stops.
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
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
from lxml import html as lxml_html
from lxml.cssselect import CSSSelector
from urllib.parse import urlparse, parse_qs

# Setup path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database

# === CONFIGURATION ===
DEFAULT_THREADS = 10
DEFAULT_BATCH_SIZE = 50
DEFAULT_DELAY_MIN = 0.5
DEFAULT_DELAY_MAX = 1.0

# Load template
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'template', 'mogidetails.json')

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("FastMogi")

# Thread lock
db_lock = threading.Lock()
stats_lock = threading.Lock()
stats = {'success': 0, 'error': 0, 'captcha': 0, 'total': 0}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def load_template() -> list:
    with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        template = json.load(f)
    return template.get('fields', [])

def get_random_ua():
    return random.choice(USER_AGENTS)

def create_session(proxy=None):
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    session.headers.update({
        'User-Agent': get_random_ua(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://mogi.vn/',
        'DNT': '1',
    })
    
    if proxy:
        session.proxies = {
            'http': proxy,
            'https': proxy
        }
    return session

# === DATA EXTRACTION HELPERS (Ported from mogi_detail_crawler.py) ===

def extract_location_ids_from_url(url: str) -> dict:
    result = {'mogi_city_id': None, 'mogi_district_id': None, 'mogi_ward_id': None}
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if 'cid' in query:
        try: result['mogi_city_id'] = int(query['cid'][0])
        except: pass
    if 'did' in query:
        try: result['mogi_district_id'] = int(query['did'][0])
        except: pass
    if 'psid' in query:
        try: result['mogi_ward_id'] = int(query['psid'][0])
        except: pass
    return result

def extract_location_ids_from_html(html_content: str) -> dict:
    result = {
        'mogi_city_id': None,
        'mogi_district_id': None,
        'mogi_ward_id': None,
        'mogi_street_id': None,
    }
    
    did_match = re.search(r'"did"\s*:\s*(\d+)', html_content)
    if did_match: result['mogi_district_id'] = int(did_match.group(1))
    
    street_match = re.search(r'"streetId"\s*:\s*(\d+)', html_content)
    if street_match: result['mogi_street_id'] = int(street_match.group(1))

    cid_match = re.search(r'"cid"\s*:\s*(\d+)', html_content)
    if cid_match:
        result['mogi_city_id'] = int(cid_match.group(1))
    else:
        cid_url_match = re.search(r'[?&]cid=(\d+)', html_content)
        if cid_url_match: result['mogi_city_id'] = int(cid_url_match.group(1))
            
    wid_match = re.search(r'[?&]wid=(\d+)', html_content)
    if wid_match:
        result['mogi_ward_id'] = int(wid_match.group(1))
    else:
        psid_match = re.search(r'"psid"\s*:\s*(\d+)', html_content)
        if psid_match: result['mogi_ward_id'] = int(psid_match.group(1))
    
    return result

def extract_phone_from_html(html_content: str) -> str:
    # 1. Try AgentMobile in pageData
    agent_mobile = re.search(r'"AgentMobile"\s*:\s*"(\d+)"', html_content)
    if agent_mobile: return agent_mobile.group(1)
    # 2. Try PhoneFormat binding
    phone_format = re.search(r"PhoneFormat\('(\d+)'\)", html_content)
    if phone_format: return phone_format.group(1)
    return None

def extract_value_by_selector(tree, selector: str, value_type: str = 'text'):
    try:
        if selector.startswith('//'):
            elements = tree.xpath(selector)
        else:
            css_sel = CSSSelector(selector)
            elements = css_sel(tree)
        
        if not elements: return None
        element = elements[0]
        
        if value_type == 'text':
            return element.text_content().strip() if hasattr(element, 'text_content') else str(element).strip()
        elif value_type == 'src':
            if hasattr(element, 'get'): return element.get('src')
            # Gallery fallback
            img_elements = tree.xpath('//div[@id="gallery"]//img/@src')
            return str(len(img_elements)) if img_elements else "0"
        elif value_type == 'src_list':
            urls = []
            # Specific for gallery
            if selector == '#gallery' or 'gallery' in selector:
                imgs = tree.xpath('//div[@id="gallery"]//img')
                for img in imgs:
                    src = img.get('data-src') or img.get('src')
                    if src and src not in urls: urls.append(src)
                return urls
            # Generic
            for el in elements:
                if hasattr(el, 'get'):
                    s = el.get('src')
                    if s: urls.append(s)
            return urls
        elif value_type == 'attribute':
            return element.get('href') if hasattr(element, 'get') else None
        elif value_type == 'html':
            from lxml import html as lxml_html
            return lxml_html.tostring(element, encoding='unicode', with_tail=False).strip()
        else:
            return element.text_content().strip()
    except:
        return None

def scrape_page(session, url, template_fields):
    try:
        resp = session.get(url, timeout=10)
        
        if resp.status_code != 200:
            if resp.status_code == 403 or resp.status_code == 503:
                if "cloudflare" in resp.text.lower():
                    return {'_error': 'CLOUDFLARE'}
            
            if resp.status_code == 404:
                return {'_error': '404'}
                
            return {'_error': f'HTTP {resp.status_code}'}
            
        html_content = resp.text
        tree = lxml_html.fromstring(html_content)
        data = {}
        
        # Metadata
        data.update(extract_location_ids_from_html(html_content))
        url_ids = extract_location_ids_from_url(url)
        for k, v in url_ids.items():
            if v and not data.get(k): data[k] = v
            
        # Fields
        for field in template_fields:
            name = field.get('name')
            selector = field.get('selector')
            val_type = field.get('valueType', 'text')
            if name and selector:
                val = extract_value_by_selector(tree, selector, val_type)
                if val: data[name] = val
                
        # Phone override
        phone = extract_phone_from_html(html_content)
        if phone: data['sodienthoai'] = phone
        
        # Map override
        # Map override
        # Always try to find map iframe or link regardless of template result
        
        # 1. Try iframe src (Old method) - CHECK BOTH src AND data-src
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
                ll_match = re.search(r'[?&]ll=(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
                if ll_match:
                    data['map'] = f"{ll_match.group(1)},{ll_match.group(2)}"
                else:
                    q_match = re.search(r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
                    if q_match:
                        data['map'] = f"{q_match.group(1)},{q_match.group(2)}"
        
        # Project Info Logic (Dynamic Fetch)
        try:
            # Extract projectid from script
            # patterns: fetchText('/template/ProjectInfo?projectid=' + 0,
            pid_match = re.search(r"projectid='\s*\+\s*(\d+)", html_content)
            if not pid_match:
                pid_match = re.search(r"projectid=(\d+)", html_content)
                
            if pid_match:
                project_id = int(pid_match.group(1))
                if project_id > 0:
                    # Fetch dynamic content
                    api_url = f"https://mogi.vn/template/ProjectInfo?projectid={project_id}"
                    try:
                        p_resp = session.get(api_url, timeout=5)
                        if p_resp.status_code == 200:
                            p_tree = lxml_html.fromstring(p_resp.text)
                            # Selector: .project-info .project-title
                            titles = p_tree.cssselect('.project-info .project-title')
                            if titles:
                                data['thuocduan'] = titles[0].text_content().strip()
                    except:
                        pass
        except:
            pass
            
        return data
        
        return data
        
    except Exception as e:
        return {'_error': str(e)}

def worker(worker_id, links, proxy, template_fields, delay_min, delay_max):
    db = Database() # New connection per thread
    session = create_session(proxy)
    
    count = 0
    for item in links:
        url = item['url']
        link_id = item['id']
        loaihinh = item.get('loaihinh')
        trade_type = item.get('trade_type')
        
        # Delay
        time.sleep(random.uniform(delay_min, delay_max))
        
        try:
            data = scrape_page(session, url, template_fields)
            
            if '_error' in data:
                err = data['_error']
                if err == 'CLOUDFLARE':
                    logger.warning(f"Worker {worker_id}: Cloudflare detected on {url}. Stopping thread.")
                    with stats_lock: stats['captcha'] += 1
                    break # Stop this thread? Or skip? Better stop/retry later logic.
                elif err == '404':
                    logger.info(f"Worker {worker_id}: 404 Not Found {url}")
                    with db_lock:
                        db.update_link_status(url, 'ERROR') # Mark as error/done so we don't retry immediately
                    with stats_lock: stats['error'] += 1
                else:
                    logger.error(f"Worker {worker_id}: Error {err} on {url}")
                    # Keep as PENDING or mark ERROR? If http error, maybe retry later.
                    with stats_lock: stats['error'] += 1
            else:
                # Success
                with db_lock:
                    detail_id = db.add_scraped_detail_flat(
                        url=url,
                        data=data,
                        domain='mogi',
                        link_id=link_id,
                        loaihinh=loaihinh,
                        trade_type=trade_type
                    )
                    if detail_id and 'img' in data and isinstance(data['img'], list):
                        db.add_detail_images(detail_id, data['img'])
                    db.update_link_status(url, 'DONE')
                
                with stats_lock: stats['success'] += 1
                logger.info(f"Worker {worker_id}: scraped {url}")
                
        except Exception as e:
            logger.error(f"Worker {worker_id}: Exception on {url}: {e}")
            with stats_lock: stats['error'] += 1
            
        count += 1
        
    # db.close_connection() - Not needed as Database class doesn't hold persistent connection

def main():
    parser = argparse.ArgumentParser(description="Mogi Fast Crawler (Requests)")
    parser.add_argument('--threads', type=int, default=DEFAULT_THREADS, help='Number of threads')
    parser.add_argument('--batch', type=int, default=DEFAULT_BATCH_SIZE, help='Batch size per thread loop')
    parser.add_argument('--delay-min', type=float, default=DEFAULT_DELAY_MIN, help='Min delay')
    parser.add_argument('--delay-max', type=float, default=DEFAULT_DELAY_MAX, help='Max delay')
    parser.add_argument('--proxy', type=str, default=None, help='Proxy (http://ip:port)')
    parser.add_argument('--test-limit', type=int, default=0, help='Run only N links for testing')
    args = parser.parse_args()
    
    db = Database()
    template_fields = load_template()
    
    logger.info(f"Starting Fast Crawler with {args.threads} threads. Proxy: {args.proxy}")
    
    while True:
        # Fetch pending links
        # fetch_size = threads * batch
        limit = args.batch * args.threads
        if args.test_limit > 0:
            limit = args.test_limit
            
        pending_links = db.get_pending_links(limit=limit, domain='mogi.vn') # Filter for mogi.vn input
        
        if not pending_links:
            logger.info("No pending links found. Exiting.")
            break
            
        logger.info(f"Fetched {len(pending_links)} links.")
        
        # Split into chunks for threads
        # Actually simpler: just pass full list to executor and let them pop? 
        # Or split evenly. Splitting evenly is easier for session reuse.
        chunk_size = (len(pending_links) + args.threads - 1) // args.threads
        chunks = [pending_links[i:i + chunk_size] for i in range(0, len(pending_links), chunk_size)]
        
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for i, chunk in enumerate(chunks):
                futures.append(executor.submit(
                    worker, 
                    i+1, 
                    chunk, 
                    args.proxy, 
                    template_fields, 
                    args.delay_min, 
                    args.delay_max
                ))
            
            for f in futures:
                f.result()
                
        # Update UI stats or log
        logger.info(f"Batch finished. Stats: {stats}")
        
        if args.test_limit > 0:
            logger.info("Test limit reached. Stopping.")
            break
            
        if stats['captcha'] > 5:
            logger.warning("Too many Cloudflare blocks. Stopping.") # Basic circuit breaker
            break
            
        time.sleep(1)

if __name__ == "__main__":
    main()
