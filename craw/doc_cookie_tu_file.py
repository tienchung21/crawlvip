import asyncio
import os
import sys
from web_scraper import WebScraper

# Fix lỗi Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

async def main():
    profile_path = os.path.join(os.getcwd(), 'playwright_profile_tab3_detail')
    url_test = "https://batdongsan.com.vn/ban-nha-rieng"
    
    print(f"🚀 Đang khởi động Bot (Chế độ: BẮT BUỘC TẢI LẠI)...")
    
    async with WebScraper(headless=False, verbose=True, keep_open=True, user_data_dir=profile_path) as scraper:
        print(f"🌐 Đang điều khiển trình duyệt vào: {url_test}")
        
        # --- QUAN TRỌNG: Thêm bypass_cache=True ---
        result = await scraper.scrape_simple(url_test, bypass_cache=True)
        
        if result['success']:
            print("\n✅ ĐÃ VÀO TRANG THÀNH CÔNG!")
            print(f"📄 Tiêu đề: {result['title']}")
            print("👀 Bố hãy nhìn màn hình Chrome, giờ nó phải đang ở trang Batdongsan rồi đấy ạ.")
        else:
            print(f"❌ Lỗi: {result['error']}")
            
        print("\n⏳ Giữ trình duyệt 30 giây để bố ngắm...")
        await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass