import asyncio
import os
import sys
from playwright.async_api import async_playwright

# Fix lỗi asyncio trên Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

async def main():
    # 1. Trỏ vào thư mục Profile (như trong log bố gửi)
    profile_path = os.path.join(os.getcwd(), 'crawl4ai_profile')
    
    print(f"📂 Đang nạp Profile từ: {profile_path}")
    print("🚀 Đang mở trình duyệt (Cấu hình Clone 100% từ Crawl4AI)...")

    async with async_playwright() as p:
        # 2. Khởi động với bộ tham số Y HỆT trong Log của bố
        # Để đảm bảo Bot này và Bot Crawl4AI là "hai anh em sinh đôi"
        context = await p.chromium.launch_persistent_context(
            user_data_dir=profile_path,
            headless=False, # Hiện hình để bố soi
            viewport={"width": 1920, "height": 1080},
            
            # --- BỘ THAM SỐ BÍ MẬT (Lấy từ Log) ---
            args=[
                "--disable-gpu",
                "--disable-gpu-compositing",
                "--disable-software-rasterizer",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-infobars",
                "--ignore-certificate-errors",
                "--ignore-certificate-errors-spki-list",
                
                # QUAN TRỌNG NHẤT: Cờ Tàng Hình
                "--disable-blink-features=AutomationControlled", 
                
                "--disable-renderer-backgrounding",
                "--disable-ipc-flooding-protection",
                "--force-color-profile=srgb",
                "--mute-audio",
                "--disable-background-timer-throttling"
            ],
            
            # Xóa các cờ mặc định tố cáo Robot
            ignore_default_args=["--enable-automation"]
        )

        page = await context.new_page()
        
        # Script xóa dấu vết bổ sung (cho chắc ăn)
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        print("\n" + "="*60)
        print("✅ TRÌNH DUYỆT ĐANG TREO VĨNH VIỄN!")
        print("="*60)
        
        # 3. Tự động vào trang check Header để bố xem luôn
        print("🔍 Đang vào httpbin để check User-Agent...")
        await page.goto("https://httpbin.org/headers")
        
        print("\n👉 Bố hãy nhìn màn hình trình duyệt:")
        print("   1. Dòng 'User-Agent' kia chính là cái mà Crawl4AI đang dùng.")
        print("   2. Sau đó bố gõ batdongsan.com.vn lên thanh địa chỉ để soi Cookie.")
        print("\n⛔ Khi nào xong, quay lại đây bấm Ctrl + C để tắt.")
        
        # Giữ máy vĩnh viễn
        await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Đã tắt trình duyệt.")