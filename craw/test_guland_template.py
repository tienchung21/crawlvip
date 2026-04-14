
from curl_cffi import requests
from bs4 import BeautifulSoup
import json
import os

TEMPLATE = {
  "type": "listing",
  "itemSelector": ".c-sdb-card__tle a",
  "nextPageSelector": "#btn-load-more",
  "url": "https://guland.vn/mua-ban-bat-dong-san-quang-binh",
  "createdAt": "2026-01-06T15:19:10.749Z"
}

COOKIE_FILE = "guland_cookies.json"

def load_cookies():
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, 'r') as f:
                return json.load(f)
        except:
            return None
    return None

def test_template():
    url = TEMPLATE['url']
    selector = TEMPLATE['itemSelector']
    print(f"Testing Template on URL: {url}")
    print(f"Selector: {selector}")
    
    # Load cookies if available (optional but good for consistency)
    cookies = load_cookies()
    session = requests.Session()
    if cookies:
        session.cookies.update(cookies)

    try:
        response = session.get(url, impersonate="chrome120", timeout=30)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Select items
            items = soup.select(selector)
            print(f"Found {len(items)} items.")
            
            print("--- First 5 Links Found ---")
            for idx, item in enumerate(items[:5]):
                link = item.get('href')
                text = item.get_text(strip=True)
                print(f"{idx+1}. Text: {text}")
                print(f"   Link: {link}")
                
            if len(items) == 0:
                print("\nWARNING: No items found. Selector might be wrong or content is dynamic/blocked.")
        else:
            print("Failed to fetch page.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_template()
