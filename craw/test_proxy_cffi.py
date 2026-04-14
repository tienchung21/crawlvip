
from curl_cffi import requests

def main():
    proxy_url = "http://52.91.52.197:3128"
    target_url = "https://batdongsan.com.vn/"
    
    print(f"Testing Proxy with curl_cffi (Chrome Impersonation): {proxy_url}")
    
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }
    
    try:
        # 1. Check IP (using httpbin, supports JSON)
        print("1. Checking IP via Proxy...")
        r = requests.get("http://httpbin.org/ip", proxies=proxies, impersonate="chrome124", timeout=10)
        print(f"   Status: {r.status_code}")
        print(f"   Body: {r.text}")
        
        # 2. Check Batdongsan
        print("\n2. Checking Batdongsan via Proxy...")
        r2 = requests.get(target_url, proxies=proxies, impersonate="chrome124", timeout=15)
        print(f"   Status: {r2.status_code}")
        if r2.status_code == 200:
            print("   SUCCESS! Bypass Cloudflare OK.")
        elif r2.status_code == 403:
            print("   BLOCKED (403). Cloudflare detected Datacenter IP.")
        else:
            print(f"   Result: {r2.status_code}")

    except Exception as e:
        print(f"   Error: {e}")

if __name__ == "__main__":
    main()
