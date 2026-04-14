
from seleniumbase import Driver
import time
import sys

def fetch_html(url):
    driver = Driver(uc=True, headless=True)
    try:
        print(f"Fetching {url}...")
        driver.get(url)
        time.sleep(5) # Wait for JS
        
        html = driver.page_source
        
        # Save to file
        with open("debug_mogi.html", "w", encoding="utf-8") as f:
            f.write(html)
            
        print("Saved to debug_mogi.html")
        
        # Quick check for 'map' or 'lat'
        if "map-content" in html:
            print("Found 'map-content' class.")
        else:
            print("NOT Found 'map-content' class.")
            
        if "google.com/maps" in html:
            print("Found Google Maps URL.")
        else:
            print("NOT Found Google Maps URL.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = "https://mogi.vn/quan-binh-tan/mua-dat-tho-cu/ban-dat-khu-ten-lua-mt-duong-so-2-dt-6-x-23m-14-7-ty-id22606069"
    
    fetch_html(url)
