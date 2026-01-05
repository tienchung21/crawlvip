import asyncio
import nodriver as uc
import sys
import json # <--- Bố nhớ thêm thư viện này

# Cấu hình tiết kiệm cho nodriver (chặn ảnh, tắt audio để giảm lag và tiết kiệm bandwidth)
BROWSER_CONFIG_TIET_KIEM = [
    "--blink-settings=imagesEnabled=false", 
    "--disable-images",
    "--mute-audio",
]

# --- CẤU HÌNH CỨNG ---
TARGET_URL = "https://batdongsan.com.vn/nha-dat-ban"
ITEM_SELECTOR = ".js__product-link-for-product-id" 
NEXT_SELECTOR = ".re__pagination-icon > .re__icon-chevron-right--sm" 
MAX_PAGES = 3

async def main():
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding='utf-8')

    print("🚀 Khởi động nodriver (chế độ tiết kiệm - chặn ảnh)...")
    browser = await uc.start(headless=False, browser_args=BROWSER_CONFIG_TIET_KIEM)
    
    print(f"🔗 Đang vào: {TARGET_URL}")
    page = await browser.get(TARGET_URL)
    
    print("⏳ Chờ 5s cho trang load ổn định...")
    await asyncio.sleep(5)

    for current_page in range(1, MAX_PAGES + 1):
        print(f"\n" + "="*50)
        print(f"📄 TRANG SỐ: {current_page}")
        print("="*50)

        # --- BƯỚC 1: LẤY LINK (SỬA LOGIC TẠI ĐÂY) ---
        print(f"🔍 Đang quét với selector: {ITEM_SELECTOR}")
        
        # Dùng JSON.stringify để đóng gói dữ liệu thành chuỗi an toàn
        items_json = await page.evaluate(f"""
            JSON.stringify(
                Array.from(document.querySelectorAll('{ITEM_SELECTOR}'))
                    .map(a => ({{
                        href: a.href,
                        text: a.innerText
                    }}))
                    .filter(item => item.href)
            )
        """)
        
        # Giải nén chuỗi JSON trong Python
        items = json.loads(items_json)

        if items:
            print(f"✅ Tìm thấy {len(items)} tin đăng:")
            for i, item in enumerate(items, 1):
                link = item.get('href') # Dùng .get cho an toàn
                if link and link.startswith('/'):
                    link = "https://batdongsan.com.vn" + link
                print(f"   {i}. {link}")
        else:
            print(f"⚠️ Không tìm thấy tin nào với class '{ITEM_SELECTOR}'")

        # --- BƯỚC 2: CHUYỂN TRANG ---
        if current_page < MAX_PAGES:
            print("\n👉 Đang tìm nút Next...")
            try:
                # Tìm nút next
                next_btn = await page.select(NEXT_SELECTOR, timeout=5)
                
                if next_btn:
                    await next_btn.scroll_into_view()
                    await asyncio.sleep(0.5)
                    await next_btn.click()
                    print(f"➡️ Đã click Next thành công!")
                    
                    print("⏳ Chờ 5s load trang mới...")
                    await asyncio.sleep(5)
                else:
                    print("❌ Không thấy nút Next (Hết trang?). Dừng.")
                    break
            except Exception as e:
                print(f"❌ Lỗi khi Next trang: {e}")
                break
    
    print("\n🏁 Hoàn thành!")
    await asyncio.sleep(5)
    await browser.stop()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    uc.loop().run_until_complete(main())