
import time
import random
import threading
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Callable
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from seleniumbase import Driver
from selenium.webdriver.common.by import By

# Setup path to import database
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database

# Replicate Categories
MOGI_CATEGORIES = {
    "Thuê": {
        "Nhà mặt tiền phố": "https://mogi.vn/thue-nha-mat-tien-pho",
        "Nhà biệt thự, liền kề": "https://mogi.vn/thue-nha-biet-thu-lien-ke",
        "Đường nội bộ": "https://mogi.vn/thue-duong-noi-bo",
        "Nhà hẻm ngõ": "https://mogi.vn/thue-nha-hem-ngo",
        "Căn hộ chung cư": "https://mogi.vn/thue-can-ho-chung-cu",
        "Căn hộ tập thể, cư xá": "https://mogi.vn/thue-can-ho-tap-the-cu-xa",
        "Căn hộ Penthouse": "https://mogi.vn/thue-can-ho-penthouse",
        "Căn hộ dịch vụ": "https://mogi.vn/thue-can-ho-dich-vu",
        "Căn hộ Officetel": "https://mogi.vn/thue-can-ho-officetel",
        "Phòng trọ, nhà trọ": "https://mogi.vn/thue-phong-tro-nha-tro",
        "Văn phòng": "https://mogi.vn/thue-van-phong",
        "Nhà xưởng, kho bãi": "https://mogi.vn/thue-nha-xuong-kho-bai-dat",
    },
    "Mua": {
        "Nhà mặt tiền phố": "https://mogi.vn/mua-nha-mat-tien-pho",
        "Nhà biệt thự, liền kề": "https://mogi.vn/mua-nha-biet-thu-lien-ke",
        "Đường nội bộ": "https://mogi.vn/mua-duong-noi-bo",
        "Nhà hẻm ngõ": "https://mogi.vn/mua-nha-hem-ngo",
        "Căn hộ chung cư": "https://mogi.vn/mua-can-ho-chung-cu",
        "Căn hộ tập thể, cư xá": "https://mogi.vn/mua-can-ho-tap-the-cu-xa",
        "Căn hộ Penthouse": "https://mogi.vn/mua-can-ho-penthouse",
        "Căn hộ Officetel": "https://mogi.vn/mua-can-ho-officetel",
        "Đất thổ cư": "https://mogi.vn/mua-dat-tho-cu",
        "Đất nền dự án": "https://mogi.vn/mua-dat-nen-du-an",
        "Đất nông nghiệp": "https://mogi.vn/mua-dat-nong-nghiep",
        "Đất kho xưởng": "https://mogi.vn/mua-dat-kho-xuong",
    }
}

class ManualMogiCrawler:
    def __init__(self, db_connector=None):
        self.db = db_connector
        self.should_stop = False

    def build_url(self, base_url: str, page: int) -> str:
        parsed = urlparse(base_url)
        query = parse_qs(parsed.query)
        query['cp'] = [str(page)]
        new_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def _start_driver(self, proxy: Optional[str] = None):
        return Driver(uc=True, headless=True, proxy=proxy)

    def crawl_task(self, base_url: str, trade_type: str, cat_name: str, start_page: int, log_queue, proxy=None):
        task_id = f"[{cat_name}]"
        def log(msg):
            print(f"{time.strftime('%H:%M:%S')} {msg}") # Print directly to stdout

        log(f"{task_id} Starting...")
        if proxy:
            log(f"{task_id} Using Proxy: {proxy}")
        
        driver = None
        page = start_page
        consecutive_errors = 0
        consecutive_duplicates = 0 # Local counter for this task
        total_collected = 0
        
        try:
            driver = self._start_driver(proxy=proxy)
            
            while not self.should_stop:
                url = self.build_url(base_url, page)
                page_retries = 3
                found_on_page = False
                
                for attempt in range(page_retries):
                    try:
                        driver.get(url)
                        # FASTER SLEEP: 1 - 1.5s
                        time.sleep(random.uniform(1.0, 1.5))
                        
                        page_source = driver.page_source.lower()
                        title = driver.title.lower()
                        
                        if "404" in title or "page not found" in page_source:
                            log(f"{task_id} -> 404/Not Found at page {page}")
                            break
                        elif "cloudflare" in title or "attention required" in title:
                            log(f"{task_id} -> Cloudflare detected! Waiting 10s...")
                            time.sleep(10)
                            continue
                        else:
                            elements = driver.find_elements(By.CSS_SELECTOR, "ul.props .link-overlay")
                            links_batch = []
                            for el in elements:
                                try:
                                    href = el.get_attribute("href")
                                    if href and "/news/" not in href:
                                        if "mogi.vn" not in href and href.startswith("/"):
                                            href = "https://mogi.vn" + href
                                        links_batch.append(href)
                                except:
                                    pass
                            
                            count = len(links_batch)
                            if count == 0:
                                log(f"{task_id} -> Page {page} loaded but 0 items. (Attempt {attempt+1})")
                                if attempt < page_retries - 1:
                                    time.sleep(1)
                                    continue
                            else:
                                found_on_page = True
                                consecutive_errors = 0
                                total_collected += count
                                log(f"{task_id} -> Page {page}: Found {count} links")

                                if self.db:
                                    # DATE CHECK LOGIC (Optional)
                                    # Try to extract date from listing item to stop early
                                    if hasattr(self, 'stop_year') and self.stop_year:
                                        try:
                                            # Mogi listing date often in .prop-created or .prop-attr
                                            # e.g., "Hôm nay", "1 ngày trước", "02/01/2024"
                                            # This is tricky because Mogi listing structure varies.
                                            # For now, we will rely on text content search if structured generic fails.
                                            pass
                                            # Note: Accurate listing date parsing requires more DOM analysis.
                                            # Given time constraint and "lay matin truyen vao url" instruction,
                                            # user might prefer simpler logic: Crawl recent pages until we see data we know is old?
                                            # Or just rely on re-crawling relevant IDs.
                                        except:
                                            pass

                                    added = self.db.add_collected_links(
                                        links_list=links_batch,
                                        domain='mogi.vn',
                                        loaihinh=cat_name,
                                        trade_type=trade_type.lower()
                                    )

                                    # Logic check duplicate stop
                                    if added == 0 and count > 0:
                                        consecutive_duplicates += count
                                        log(f"{task_id} -> All {count} links are duplicates. Consecutive dups: {consecutive_duplicates}/100")
                                    else:
                                        if consecutive_duplicates > 0:
                                            log(f"{task_id} -> Found {added} new links. Resetting duplicate counter (was {consecutive_duplicates}).")
                                        consecutive_duplicates = 0

                                    if consecutive_duplicates >= 100:
                                        log(f"{task_id} Stopping: Reached 100 consecutive duplicate links (No new data).")
                                        # Set stop flag solely for this loop of return?
                                        return

                                break
                                
                    except Exception as e:
                        log(f"{task_id} Error page {page}: {str(e)[:50]}")
                        time.sleep(1)
                
                if not found_on_page:
                    log(f"{task_id} -> Failed page {page}. Restarting driver...")
                    try:
                        driver.quit()
                        time.sleep(1)
                        driver = self._start_driver(proxy=proxy)
                    except:
                        pass
                    consecutive_errors += 1

                if consecutive_errors >= 5:
                    log(f"{task_id} Stopping: Too many consecutive errors.")
                    break
                
                page += 1
                
        except Exception as e:
            log(f"{task_id} Critical Error: {e}")
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            log(f"{task_id} Finished. Total: {total_collected}")

    def run_crawling(self, threads=3, proxy=None):
        tasks = []
        # Create tasks for ALL categories (Thuê + Mua)
        for trade_type, cats in MOGI_CATEGORIES.items():
            for name, url in cats.items():
                tasks.append({
                    "url": url,
                    "trade_type": trade_type,
                    "cat_name": name,
                    "start_page": 1
                })
        
        print(f"Starting {len(tasks)} tasks (Rent + Buy) with {threads} threads...")
        if proxy:
            print(f"Proxy: {proxy}")
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for t in tasks:
                executor.submit(
                    self.crawl_task,
                    t['url'],
                    t['trade_type'],
                    t['cat_name'],
                    t['start_page'],
                    None, # log_queue not used
                    proxy
                )

if __name__ == "__main__":
    db = Database()
    
    # Try to get proxy from env
    proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    
    # Or hardcoded if user wants (but trying env first as per previous interaction)
    # User said: "vừa setup xong proxy... tao muốn mày tạo 1 file riêng chạy tạm thời"
    if not proxy:
        # Fallback to hardcoded if env not set in this session
        proxy = "http://100.53.5.135:3128"
    
    crawler = ManualMogiCrawler(db)
    # Run with 5 threads for speed
    crawler.run_all_rent(threads=5, proxy=proxy)
