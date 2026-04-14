
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import sys

def main():
    target_url = "https://batdongsan.com.vn/sitemap/detailed-listings-20250728-1.xml"
    try:
        print(f"Counting items (JS Mode) in: {target_url}")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            sb.open(target_url)
            print("Waiting for load (15s)...")
            sb.sleep(15)
            
            # Use JS to count
            # Note: XML usually has tags like <loc>. In DOM they might be namespaced or just tags.
            # Try 'loc' or 'url'.
            # Browser often renders XML inside a specific container or just raw.
            # If standard XML viewer: document.getElementsByTagName('loc').length
            
            print("Executing JS...")
            count = sb.execute_script("return document.getElementsByTagName('loc').length")
            
            if count == 0:
                # Retry with 'url' tag just in case
                count = sb.execute_script("return document.getElementsByTagName('url').length")
                
            print(f"\n--- RESULT ---")
            print(f"Total Items (JS Count): {count}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
