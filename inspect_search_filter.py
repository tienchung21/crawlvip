
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import json

def main():
    target_url = "https://batdongsan.com.vn/nha-dat-ban"
    
    try:
        print("Inspecting Search Filter logic...")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            sb.open(target_url)
            print("Page Loaded. Waiting 5s...")
            sb.sleep(5)
            
            # 1. Check for standard <select> tags
            # Search filters often use 'select' hidden or 'div' UI
            # We look for "Tỉnh/Thành" placeholder or labels
            
            content = sb.get_page_source()
            
            # Basic snapshot of likely filter elements
            print("\nScanning for Select/Dropdowns...")
            
            # Use JS to find elements with "Hà Nội", "Hồ Chí Minh"
            # and verify if they have IDs attached
            js_code = """
            // Find elements containing 'Hồ Chí Minh'
            let els = Array.from(document.querySelectorAll('*')).filter(el => el.innerText === 'Hồ Chí Minh' && el.tagName !== 'SCRIPT');
            return els.map(el => ({ tag: el.tagName, class: el.className, html: el.outerHTML.substring(0, 200) }));
            """
            matches = sb.execute_script(js_code)
            
            print(f"Found {len(matches)} elements containing 'Hồ Chí Minh'.")
            if matches:
                print("Samples:")
                for m in matches[:3]:
                    print(m)
            
            # 2. Check for "District" List
            # If we find City list, we can check District list
            # Usually District list is loaded AFTER City selection.
            # But the City List usually contains IDs like 'SG', 'HN' or numeric '24', '29'?
            
            # Check for specific class names common in Batdongsan (e.g. re__filter)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
