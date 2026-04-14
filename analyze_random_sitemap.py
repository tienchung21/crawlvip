
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import re
import sys

def main():
    index_url = "https://batdongsan.com.vn/sitemap/detailed-listings.xml"
    try:
        print(f"Fetching Index: {index_url}")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            sb.open(index_url)
            sb.sleep(10)
            
            content_index = sb.get_page_source()
            child_urls = re.findall(r'<loc>(https://.*?)</loc>', content_index)
            print(f"Index contains {len(child_urls)} files.")
            
            if len(child_urls) > 50:
                target_url = child_urls[50] # Pick #51
            elif len(child_urls) > 0:
                target_url = child_urls[-1] # Pick Last
            else:
                print("No children found.")
                return

            print(f"\n--- Checking Random Child: {target_url} ---")
            sb.open(target_url)
            print("Waiting for load (15s)...")
            sb.sleep(15)
            
            print("Executing JS Count...")
            count = sb.execute_script("return document.getElementsByTagName('loc').length")
            if count == 0:
                count = sb.execute_script("return document.getElementsByTagName('url').length")
                
            print(f"\n--- RESULT ---")
            print(f"File: {target_url}")
            print(f"Total Items: {count}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
