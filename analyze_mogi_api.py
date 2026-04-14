
from playwright.sync_api import sync_playwright
import time

def analyze_api():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("Navigating to https://mogi.vn/ho-chi-minh/mua-nha...")
        page.goto("https://mogi.vn/ho-chi-minh/mua-nha", wait_until="domcontentloaded")
        
        # Subscribe to requests
        page.on("request", lambda request: print(f">> {request.method} {request.url}"))
        
        print("Waiting for page load...")
        time.sleep(5)
        
        print("Done capturing.")
        browser.close()

if __name__ == "__main__":
    analyze_api()
