
from seleniumbase import SB
import sys

def main():
    try:
        print("Starting SeleniumBase (UC Mode)...")
        # headless=True often triggers bot detection, but we have no choice on server.
        # uc=True enables undetected-chromedriver mode
        with SB(uc=True, headless=True, page_load_strategy="eager") as sb:
            url = "https://batdongsan.com.vn/sitemap/detailed-listings.xml"
            print(f"Opening {url}...")
            sb.open(url)
            
            print("Waiting for title/content...")
            sb.sleep(10) # Wait for Cloudflare
            
            # Check title
            title = sb.get_title()
            print(f"Title: {title}")
            
            # Get text
            content = sb.get_page_source()
            print("\n--- CONTENT START ---")
            print(content[:2000])
            print("\n--- CONTENT END ---")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
