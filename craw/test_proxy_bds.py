
import requests
import sys

def main():
    proxy_ip = "52.91.52.197"
    # Try common ports if not specified? Or user just gave IP?
    # Let's try to assume it handles traffic on some port.
    # Usually proxies need IP:PORT.
    # If user just gave IP, maybe it's 80, 8080, 3128.
    
    ports = [80, 8080, 3128, 8888]
    target_url = "https://batdongsan.com.vn/"
    # Also Seller API
    api_url = "https://sellernetapi.batdongsan.com.vn/api/common/fetchDistrictList?cityCode=HN"

    print(f"Testing Proxy IP: {proxy_ip}")
    
    for port in ports:
        proxy_url = f"http://{proxy_ip}:{port}"
        print(f"\nTrying Proxy: {proxy_url} ...")
        
        proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        
        try:
            # 1. Test IP/Connectivity
            print("  - Checks IP...")
            r = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=5)
            if r.status_code == 200:
                print(f"    SUCCESS! IP is: {r.json()}")
            else:
                print(f"    Failed IP Check: {r.status_code}")
                continue
                
            # 2. Test Batdongsan
            print("  - Checks Batdongsan API...")
            r = requests.get(api_url, proxies=proxies, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            if r.status_code == 200:
                print("    SUCCESS! Batdongsan API 200 OK.")
                print(f"    Data Length: {len(r.text)}")
                return # Done
            elif r.status_code == 403:
                print("    BLOCKED 403 (Batdongsan blocked this proxy).")
            else:
                print(f"    Result: {r.status_code}")
                
        except Exception as e:
            print(f"    Connection Error: {e}")

    print("\nAll attempts finished.")

if __name__ == "__main__":
    main()
