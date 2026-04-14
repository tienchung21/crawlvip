
from curl_cffi import requests
import json

def test_api():
    url = "https://batdongsan.com.vn/microservice-architecture-router/Product/ProductDetail/GetMarkerById?productId=41931423"
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Origin': 'https://batdongsan.com.vn',
        'Referer': 'https://batdongsan.com.vn/',
        'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    
    print(f"Requesting: {url}")
    try:
        response = requests.get(
            url,
            headers=headers,
            impersonate="chrome124",
            timeout=30
        )
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            try:
                data = response.json()
                print("Response JSON:")
                print(json.dumps(data, indent=2, ensure_ascii=False))
            except json.JSONDecodeError:
                print("Response is NOT JSON:")
                print(response.text[:500])
        else:
             print("Response Text:")
             print(response.text[:500])
             
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_api()
