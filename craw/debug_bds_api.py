
from curl_cffi import requests
import json

# ID that returned 500
ID_500 = "45107478"
# ID that returned Date Error
ID_DATE = "45098605"

API_URL = "https://batdongsan.com.vn/api/listing-detail/get-listing-detail"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://batdongsan.com.vn/",
    # Add other headers if known from the main script
}

def test_id(pid):
    url = f"{API_URL}?productId={pid}"
    print(f"--- Testing ID: {pid} ---")
    try:
        resp = requests.get(url, headers=HEADERS, impersonate="chrome120", timeout=30)
        print(f"Status: {resp.status_code}")
        print("Headers:", resp.headers)
        
        try:
            data = resp.json()
            print("Types of data keys:", data.keys())
            
            # Check date fields
            for k, v in data.items():
                if 'date' in k.lower() or 'time' in k.lower():
                    print(f"Date Field [{k}]: {v}")
                    
        except:
            print("Response is not JSON.")
            print("Body Preview:", resp.text[:500])
            
    except Exception as e:
        print(f"Request Error: {e}")

if __name__ == "__main__":
    print("Testing 500 ID...")
    test_id(ID_500)
    print("\nTesting Date Error ID...")
    test_id(ID_DATE)
