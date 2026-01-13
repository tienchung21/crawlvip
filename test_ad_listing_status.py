from curl_cffi import requests

# URL bỏ tham số fingerprint để tránh bị khớp sai lệch danh tính
URL = "https://gateway.chotot.com/v1/public/ad-listing?region_v2=13000&area_v2=13107&cg=1000&st=s,k&limit=50&include_expired_ads=true&key_param_included=true&video_count_included=true"

def main():
    # 1. Khai báo Header giả lập iPhone
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9",
        "Referer": "https://www.nhatot.com/",
        "Origin": "https://www.nhatot.com",
    }

    try:
        # 2. Sử dụng impersonate="safari15_5" để giả lập vân tay SSL của thiết bị Apple
        # Nếu muốn giả lập Android, bạn có thể thử impersonate="chrome110" và đổi User-Agent tương ứng
        resp = requests.get(
            URL, 
            headers=headers,
            impersonate="safari15_5", 
            timeout=20
        )
        
        print("Status Code:", resp.status_code)
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"Thành công! Lấy được {len(data.get('ad_list', []))} tin đăng từ 'điện thoại giả lập'.")
            if data.get('ad_list'):
                print("Ví dụ tin đầu tiên:", data['ad_list'][0].get('subject'))
        elif resp.status_code == 429:
            print("Lỗi 429: Cloudflare vẫn nhận ra đây là code hoặc IP của bạn đã bị 'đánh dấu' quá nặng.")
            print("Gợi ý: Hãy thử bật/tắt máy bay trên điện thoại rồi phát 4G lại để đổi IP.")
        else:
            print(f"Lỗi khác ({resp.status_code}):", resp.text[:300])
            
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

if __name__ == "__main__":
    main()