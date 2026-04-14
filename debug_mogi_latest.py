import sys
import os
import time
import random
from seleniumbase import Driver
from selenium.webdriver.common.by import By
import pymysql

# Setup DB connection directly
def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='craw_db',
        cursorclass=pymysql.cursors.DictCursor
    )

def normalize_url(url):
    # Simple normalization matching database.py logic roughly
    if '?' in url:
        return url.split('?')[0] # Often params are tracking
    return url

def main():
    print("=== DEBUG MOGI LATEST LISTINGS ===")
    
    # Unset proxy to avoid interfering with Selenium internal localhost connection
    for k in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        if k in os.environ:
            del os.environ[k]
            
    base_url = "https://mogi.vn/thue-phong-tro-nha-tro?cp="
    
    driver = Driver(uc=True, headless=True)
    all_links = []
    
    try:
        for page in range(1, 4): # Pages 1, 2, 3 (~45-60 items)
            url = f"{base_url}{page}"
            print(f"Fetching Page {page}: {url}")
            
            driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            elements = driver.find_elements(By.CSS_SELECTOR, "ul.props .link-overlay")
            page_links = []
            for el in elements:
                href = el.get_attribute("href")
                if href:
                    if "mogi.vn" not in href and href.startswith("/"):
                        href = "https://mogi.vn" + href
                    # Normalize simple
                    if '?' in href:
                        href = href.split('?')[0]
                    page_links.append(href)
            
            print(f" -> Found {len(page_links)} links")
            all_links.extend(page_links)
            
            if len(all_links) >= 50:
                print("Reached 50+ links, stopping fetch.")
                break
                
        print(f"\nTotal Collected: {len(all_links)} unique items (checking duplicates in list...)")
        all_links = list(set(all_links))
        print(f"Unique URLs to check: {len(all_links)}")

        # Check DB
        conn = get_db_connection()
        cursor = conn.cursor()
        
        existing_count = 0
        new_count = 0
        
        print("\n--- DB CHECK ---")
        for l in all_links:
            cursor.execute("SELECT id, created_at FROM collected_links WHERE url LIKE %s", (l + '%',))
            res = cursor.fetchone()
            if res:
                # print(f"[EXIST] {l} (Created: {res['created_at']})")
                existing_count += 1
            else:
                print(f"[NEW!!] {l}")
                new_count += 1
                
        print(f"\nSummary for {len(all_links)} links:")
        print(f" - Existing (Old): {existing_count}")
        print(f" - NEW (Missed):   {new_count}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
