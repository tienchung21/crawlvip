
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import time
import json
import re

def main():
    target_url = "https://batdongsan.com.vn/"
    try:
        print("Starting Interactive Browser...")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            sb.open(target_url)
            print("Browser Opened. Waiting 180s (3 mins) for User Action...")
            
            # Wait for user
            sb.sleep(180)
            
            print("Time's up! Scanning JS Resources...")
            current_url = sb.get_current_url()
            print(f"Current URL: {current_url}")
            
            # 1. Check for Embedded JSON (Next.js / Nuxt)
            content = sb.get_page_source()
            
            # Check for _NEXT_DATA_
            if '"_NEXT_DATA_"' in content or 'id="__NEXT_DATA__"' in content:
                 print("\n[FOUND] Next.js Data detected!")
                 # Extract it
                 match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', content)
                 if match:
                     data = match.group(1)
                     print(f"Data Length: {len(data)} chars")
                     print("Preview: " + data[:200])
            
            # Check for Nuxt
            if 'window.__NUXT__' in content:
                print("\n[FOUND] Nuxt.js Data detected!")
            
            # 2. List Loaded JS Files
            # Use simple JS to get resources
            print("\nScanning Loaded Scripts...")
            try:
                # Simple map return
                js_cmd = "return window.performance.getEntriesByType('resource').filter(r => r.initiatorType === 'script').map(r => r.name);"
                js_files = sb.execute_script(js_cmd)
                
                print(f"Found {len(js_files)} script files loaded.")
                for js in js_files:
                    if ".js" in js:
                        print(f" - {js}")
            except Exception as js_err:
                 print(f"JS Scan Error: {js_err}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
