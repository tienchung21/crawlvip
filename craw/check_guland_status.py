
from curl_cffi import requests


from curl_cffi import requests
import json
import os

COOKIE_FILE = "guland_cookies.json"

def load_cookies():
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, 'r') as f:
                return json.load(f)
        except:
            return None
    return None

def save_cookies(cookies):
    # cookies is a RequestsCookieJar, convert to dict
    with open(COOKIE_FILE, 'w') as f:
        json.dump(cookies, f)
    print(f"-> Saved {len(cookies)} cookies to {COOKIE_FILE}")

def test_crawl():
    url = "https://guland.vn/post/duoi-1-ty-tha-ho-mua-dat-nghi-duong-kim-boi-1904236"
    print(f"Testing URL: {url}")
    
    # 1. Try to load existing cookies
    existing_cookies = load_cookies()
    session = requests.Session()
    
    if existing_cookies:
        print(f"Loaded {len(existing_cookies)} cookies from file. Reusing session...")
        session.cookies.update(existing_cookies)
    else:
        print("No saved cookies found. Starting fresh session...")

    try:
        # Use session to make request
        response = session.get(url, impersonate="chrome120", timeout=30)
        
        print(f"Status Code: {response.status_code}")
        
        # Check Cloudflare
        is_cloudflare = False
        if 'cloudflare' in response.headers.get('server', '').lower() or 'cf-ray' in response.headers:
            is_cloudflare = True
            
        if is_cloudflare:
            print("Result: Cloudflare DETECTED.")
        else:
            print("Result: Cloudflare NOT detected.")

        if response.status_code == 200:
            # 2. Save cookies for next time
            # session.cookies.get_dict() returns a standard python dict
            save_cookies(session.cookies.get_dict())
            print("Success! Page Title:", response.text.split('<title>')[1].split('</title>')[0] if '<title>' in response.text else "No title")
        else:
            print("Failed. Status not 200.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    test_crawl()
