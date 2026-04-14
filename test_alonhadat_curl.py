from curl_cffi import requests

urls = [
    "https://alonhadat.com.vn/can-ban-nha-dat/nghe-an/trang-40",
    "https://alonhadat.com.vn/dat-dep-xom-kim-ngoc-xa-nghi-long-18093210.html"
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    "Referer": "https://alonhadat.com.vn/"
}

print("Đang test curl_cffi với Alonhadat...\n")

for url in urls:
    print(f"URL: {url}")
    try:
        response = requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
        print(f"Status Code: {response.status_code}")
        print(f"Độ dài body : {len(response.text)} ký tự")
        if response.status_code == 200:
            if "<title>" in response.text:
                title = response.text.split("<title>")[1].split("</title>")[0]
                print(f"Title      : {title.strip()}")
            if "ImageCaptcha.ashx" in response.text:
                print("⚠ WARNING   : Bị dính Captcha (yêu cầu giải captcha)!")
            else:
                print("✅ SUCCESS   : Parse nội dung HTML thành công, KHÔNG dính captcha!")
    except Exception as e:
        print(f"❌ Error     : {e}")
    print("-" * 50)
