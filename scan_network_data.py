
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import json

def main():
    target_url = "https://batdongsan.com.vn/"
    try:
        print("Starting Network Scan...")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            sb.open(target_url)
            print("Page Loaded. Waiting 15s for APIs...")
            sb.sleep(15)
            
            # Get ALL resources (fetch, xhr)
            js_code = "return window.performance.getEntriesByType('resource').filter(r => r.initiatorType === 'fetch' || r.initiatorType === 'xmlhttprequest').map(r => r.name);"
            resources = sb.execute_script(js_code)
            
            print(f"Found {len(resources)} API/Fetch requests.")
            for r in resources:
                # Filter interesting ones
                if "api" in r or "json" in r or "location" in r or "city" in r or "province" in r:
                    print(f" - {r}")
                    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
