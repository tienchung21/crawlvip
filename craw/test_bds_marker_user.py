
from curl_cffi import requests
import json
import os

# Disable Env Proxy
os.environ.pop("http_proxy", None)
os.environ.pop("https_proxy", None)
os.environ.pop("HTTP_PROXY", None)
os.environ.pop("HTTPS_PROXY", None)

URL = "https://batdongsan.com.vn/microservice-architecture-router/Product/ProductDetail/GetMarkerById?productId=41931423"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://batdongsan.com.vn",
    "Referer": "https://batdongsan.com.vn/",
    # "Sec-Ch-Ua": "\"Chromium\";v=\"144\", \"Not_A Brand\";v=\"8\"",
    # "Sec-Ch-Ua-Mobile": "?0",
    # "Sec-Ch-Ua-Platform": "\"Windows\"",
    # "Sec-Fetch-Dest": "empty",
    # "Sec-Fetch-Mode": "cors",
    # "Sec-Fetch-Site": "same-site",
    # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Let curl_cffi set UA and Sec headers
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def main():
    resp = requests.get(URL, headers=HEADERS, impersonate="chrome124", timeout=30)
    print("status", resp.status_code)
    print("content-type", resp.headers.get("content-type"))

    try:
        data = resp.json()
    except Exception as exc:
        print("json_error", exc)
        with open("api_marker_41931423.raw.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        print("saved api_marker_41931423.raw.html")
        return

    with open("api_marker_41931423.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("saved api_marker_41931423.json")


if __name__ == "__main__":
    main()
