
from curl_cffi import requests
import json

def test_api():
    base_url = "https://sellernetapi.batdongsan.com.vn/api/common"
    
    endpoints = [
        # User provided
        f"{base_url}/fetchDistrictList?cityCode=SG",
        # Guesses
        f"{base_url}/fetchCityList",
        f"{base_url}/fetchProvinceList",
        f"{base_url}/GetAllCities",
        "https://sellernetapi.batdongsan.com.vn/api/master/fetchCityList"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://batdongsan.com.vn/"
    }
    
    print("Testing SellerNet APIs...")
    
    for url in endpoints:
        print(f"\nGET {url} ...")
        try:
            r = requests.get(url, headers=headers, impersonate="chrome120", timeout=10)
            print(f"Status: {r.status_code}")
            
            if r.status_code == 200:
                try:
                    data = r.json()
                    # Check if list or error
                    print(f"Response Type: {type(data)}")
                    if isinstance(data, list):
                        print(f"Count: {len(data)}")
                        if len(data) > 0: print(f"Sample: {data[0]}")
                    elif isinstance(data, dict):
                         print(f"Keys: {list(data.keys())}")
                         # seller api often returns {success: true, data: ...}
                except:
                    print(f"Text: {r.text[:200]}")
            else:
                print(f"Error Content: {r.text[:100]}")
                
        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    test_api()
