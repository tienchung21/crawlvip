from curl_cffi import requests
from bs4 import BeautifulSoup
import json

def scan_nhatot_structure():
    url = "https://www.nhatot.com/mua-ban-can-ho-chung-cu"
    print(f"🚀 Đang soi chiếu cấu trúc JSON của: {url}")

    try:
        response = requests.get(
            url,
            impersonate="chrome120",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://www.google.com/"
            },
            timeout=15
        )

        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        
        if not script_tag:
            print("❌ Bị chặn rồi bố ơi (Không thấy dữ liệu)")
            return

        data = json.loads(script_tag.string)
        state = data.get('props', {}).get('pageProps', {}).get('initialState', {})

        print("\n--- 1. KIỂM TRA NGĂN TỦ 'stickyAds' ---")
        sticky_data = state.get('stickyAds', {})
        sticky_list = sticky_data.get('sticky_ads', [])
        print(f"   + Có key 'stickyAds' không? -> {'CÓ' if sticky_data else 'KHÔNG'}")
        print(f"   + Số lượng tin trong đó: {len(sticky_list)}")
        
        print("\n--- 2. KIỂM TRA NGĂN TỦ 'adlisting' (Tin thường) ---")
        ads_list = state.get('adlisting', {}).get('data', {}).get('ads', [])
        print(f"   + Số lượng tin lấy được: {len(ads_list)}")
        
        print("\n--- 3. SOI KỸ TRONG 20 TIN THƯỜNG (Tìm gián điệp VIP) ---")
        vip_hidden_count = 0
        for i, item in enumerate(ads_list):
            is_sticky = item.get('is_sticky')
            # In ra trạng thái từng tin
            status = "⭐ VIP (Dính)" if is_sticky else "Thuong"
            if is_sticky: vip_hidden_count += 1
            print(f"   [{i+1}] {status} - {item.get('subject')[:40]}...")
            
        print("\n" + "="*30)
        print(f"🛑 KẾT LUẬN CỦA CON:")
        if len(sticky_list) == 0 and vip_hidden_count == 0:
            print("   👉 Server ĐANG CHẶN QUẢNG CÁO với request này.")
            print("   👉 Nó biết mình là Bot nên nó giấu sạch tin VIP đi rồi.")
        elif vip_hidden_count > 0:
            print(f"   👉 Tin VIP bị trộn vào danh sách thường ({vip_hidden_count} tin).")
        else:
            print("   👉 Cấu trúc bình thường, tin VIP nằm riêng.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    scan_nhatot_structure()                                 