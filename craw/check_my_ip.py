
import requests
import os
import sys

def check_ip(proxy=None):
    try:
        url = "http://api.ipify.org?format=json"
        proxies = {}
        if proxy:
            proxies = {"http": proxy, "https": proxy}
            print(f"Testing with Proxy: {proxy}")
        else:
            print("Testing DIRECT connection (No Proxy config passed explicitly)")
        
        # Check env vars
        if os.environ.get("HTTP_PROXY"):
            print(f"Detected HTTP_PROXY env: {os.environ.get('HTTP_PROXY')}")
        if os.environ.get("HTTPS_PROXY"):
            print(f"Detected HTTPS_PROXY env: {os.environ.get('HTTPS_PROXY')}")

        resp = requests.get(url, proxies=proxies, timeout=10)
        if resp.status_code == 200:
            print(f"SUCCESS. Current Public IP: {resp.json().get('ip')}")
        else:
            print(f"FAILED. Status Code: {resp.status_code}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    # 1. Test direct/env based
    print("--- TEST 1: Environment / System Default ---")
    check_ip()
    
    # 2. Check if user passed arguments
    if len(sys.argv) > 1:
        print("\n--- TEST 2: CLI Argument Proxy ---")
        check_ip(sys.argv[1])
