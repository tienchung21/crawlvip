
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import re
import time

def main():
    try:
        print("Starting Deep Analysis (Visible)...")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            # 1. Fetch Index
            url_index = "https://batdongsan.com.vn/sitemap/detailed-listings.xml"
            print(f"Fetching Index: {url_index}")
            sb.open(url_index)
            sb.sleep(10)
            
            content_index = sb.get_page_source()
            
            # Extract child URLs (simple regex)
            # Pattern: <loc>(https://.*?)</loc>
            child_urls = re.findall(r'<loc>(https://.*?)</loc>', content_index)
            print(f"Found {len(child_urls)} child sitemaps.")
            
            # 2. Visit First 2 Children
            for i, child_url in enumerate(child_urls[:2]):
                print(f"\n--- Analyzing Child {i+1}: {child_url} ---")
                sb.open(child_url)
                sb.sleep(5) # Wait for load
                
                child_content = sb.get_page_source()
                # Extract Listing URLs
                listing_urls = re.findall(r'<loc>(https://.*?)</loc>', child_content)
                
                print(f"Contains {len(listing_urls)} listings.")
                print("Samples:")
                for l in listing_urls[:3]:
                    print(f" - {l}")
                    
                # Check for other tags like lastmod or changefreq if visible
                # (Raw XML usually just has loc, lastmod)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
