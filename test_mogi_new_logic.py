
import sys
import os
import re
import time
from seleniumbase import Driver
from lxml import html as lxml_html

# URLs to test
URLS = [
    'https://mogi.vn/quan-binh-tan/mua-dat-tho-cu/ban-dat-khu-ten-lua-mt-duong-so-2-dt-6-x-23m-14-7-ty-id22606069',
    'https://mogi.vn/quan-binh-tan/mua-dat-tho-cu/ban-dat-mat-tien-cho-khu-ten-lua-mt-duong-so-40a-dt-5-x-16m-gia-7-8-id22622065'
]

def test_crawl():
    print("=== TESTING NEW MOGI MAP LOGIC ===")
    driver = Driver(uc=True, headless=True)
    
    try:
        for url in URLS:
            print(f"\nFetching: {url}")
            driver.get(url)
            time.sleep(5)
            
            html = driver.page_source
            tree = lxml_html.fromstring(html)
            
            data = {}
            
            # --- LOGIC COPIED FROM mogi_detail_crawler.py ---
            try:
                # 1. Try iframe src (Old method)
                map_iframes = tree.xpath('//div[contains(@class,"map-content")]//iframe/@src')
                if map_iframes:
                    src = map_iframes[0]
                    print(f"  [Old Method] Found iframe src: {src[:50]}...")
                    coord_match = re.search(r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)', src)
                    if coord_match:
                        data['map'] = f"{coord_match.group(1)},{coord_match.group(2)}"
                
                # 2. If valid map not found, try NEW method (google-maps-link)
                if 'map' not in data:
                    map_links = tree.xpath('//div[contains(@class,"google-maps-link")]//a/@href')
                    if map_links:
                        href = map_links[0]
                        print(f"  [New Method] Found link href: {href[:50]}...")
                        # Regex for ll=lat,long
                        ll_match = re.search(r'[?&]ll=(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
                        if ll_match:
                            data['map'] = f"{ll_match.group(1)},{ll_match.group(2)}"
                        else:
                            # Fallback try q=lat,long inside href
                            q_match = re.search(r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
                            if q_match:
                                data['map'] = f"{q_match.group(1)},{q_match.group(2)}"
            except Exception as e:
                print(f"  Error: {e}")
            # -----------------------------------------------
            
            if 'map' in data:
                print(f"  ✅ SUCCESS: Retrieved Map Coordinates: {data['map']}")
            else:
                print(f"  ❌ FAILED: Could not extract coordinates.")

    except Exception as e:
        print(f"Fatal Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    test_crawl()
