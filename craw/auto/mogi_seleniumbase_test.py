
import time
import json
import os
from seleniumbase import Driver
from selenium.webdriver.common.by import By

def run_test():
    # URL template
    base_url = "https://mogi.vn/ho-chi-minh/mua-can-ho?cp={}"
    start_page = 2
    end_page = 100
    
    output_file = "mogi_links_test.json"
    results = []
    
    print("Initializing SeleniumBase Driver (UC Mode)...")
    try:
        # UC mode is essential for anti-bot
        driver = Driver(uc=True, headless=True) 
    except Exception as e:
        print(f"Error initializing driver: {e}")
        return

    try:
        for page in range(start_page, end_page + 1):
            url = base_url.format(page)
            print(f"[{page}/{end_page}] Visiting: {url}")
            
            try:
                driver.get(url)
                time.sleep(1) # Wait for initial load
                
                # Check for status indicators
                page_source = driver.page_source.lower()
                title = driver.title.lower()
                
                status = "OK"
                if "404" in title or "page not found" in page_source:
                    status = "404 Not Found"
                elif "403" in title or "forbidden" in page_source:
                    status = "403 Forbidden"
                elif "429" in title or "too many requests" in page_source:
                    status = "429 Too Many Requests"
                elif "cloudflare" in title or "attention required" in title:
                    status = "Blocked (Cloudflare)"
                
                print(f"  Status: {status}")
                
                if status != "OK":
                    print(f"  Stopping or skipping due to status: {status}")
                    # You might want to break here or continue observing
                    # For now, let's record the error and continue if it's just 404
                    results.append({
                        "url": url,
                        "page": page,
                        "status": status,
                        "links": []
                    })
                    continue

                # Extract links
                # Selector from template v2: ul.props .link-overlay
                # This targets ONLY the main listing items, avoiding sidebars/news
                elements = driver.find_elements(By.CSS_SELECTOR, "ul.props .link-overlay")
                links = []
                for el in elements:
                    try:
                        href = el.get_attribute("href")
                        if href:
                            # Strict filtering: Exclude news and ensure it is a property link
                            if "/news/" in href:
                                continue
                            if "mogi.vn" not in href and href.startswith("/"):
                                href = "https://mogi.vn" + href
                                
                            links.append(href)
                    except:
                        pass
                
                print(f"  Found {len(links)} links")
                
                results.append({
                    "url": url,
                    "page": page,
                    "status": "OK",
                    "links": links
                })
                
                # Update file continuously
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                    
                time.sleep(1) # User requested delay 1s
                
            except Exception as e:
                print(f"  Error scraping page {page}: {e}")
                results.append({
                    "url": url,
                    "page": page,
                    "status": f"Error: {str(e)}",
                    "links": []
                })

    finally:
        print("Closing driver...")
        driver.quit()
        print(f"Done. Saved to {output_file}")

if __name__ == "__main__":
    run_test()
