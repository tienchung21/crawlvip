import nodriver as n
import asyncio

async def main():
    # 1. Khởi động trình duyệt (không bật chế độ expert để dùng được cf_verify)
    # nodriver tự động tìm và sử dụng Chrome/Chromium có sẵn trên máy
    print("--- Đang khởi động trình duyệt ---")
    browser = await n.start()
    
    target_url = "https://2captcha.com/demo/cloudflare-turnstile"
    
    # 2. Truy cập vào trang web
    print(f"--- Đang truy cập: {target_url} ---")
    tab = await browser.get(target_url)

    # 3. Sử dụng tính năng tích hợp sẵn để tìm và check Cloudflare
    # Hàm này sẽ tự động tìm checkbox và click (yêu cầu opencv-python)
    print("--- Đang tự động xử lý Cloudflare Turnstile ---")
    try:
        await tab.cf_verify()
        print(">>> Đã verify thành công! <<<")
    except Exception as e:
        print(f"Có lỗi xảy ra hoặc không tìm thấy Cloudflare: {e}")

    # Đợi một chút để bạn kịp nhìn thấy kết quả
    await asyncio.sleep(5)
    
    # Đóng trình duyệt (tùy chọn)
    # browser.stop() 

if __name__ == "__main__":
    # Chạy vòng lặp sự kiện (Event loop)
    n.loop().run_until_complete(main())