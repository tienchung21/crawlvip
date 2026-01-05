import nodriver as n
import asyncio

async def main():
    print(">>> [START] Khởi động trình duyệt...")
    browser = await n.start(headless=False)
    
    target_url = "https://2captcha.com/demo/cloudflare-turnstile"
    print(f">>> Đang truy cập: {target_url}")
    page = await browser.get(target_url)
    
    # Chờ 5s cho nó load cái khung ra
    await page.sleep(5)
    
    try:
        print("--- Bắt đầu chiến dịch: BẮN VÀO VỎ HỘP ---")
        
        # 1. Tìm cái Wrapper (Cái này log cũ của bố báo là CÓ THẤY)
        # Tìm class bắt đầu bằng 'cf-' hoặc class chứa 'turnstile'
        # Hoặc tìm chính xác cái div mà lần trước bố thấy
        wrapper = await page.select("div.cf-turnstile")
        
        # Nếu không thấy class kia, tìm class rộng hơn
        if not wrapper:
             wrapper = await page.select("div[style*='width'][style*='height']")

        if wrapper:
            print("!!! TÌM THẤY VỎ HỘP (WRAPPER) !!!")
            
            # 2. Lấy tọa độ cái vỏ
            rect = await wrapper.get_rect()
            print(f">>> Vị trí vỏ: X={rect.x}, Y={rect.y}, Rộng={rect.width}, Cao={rect.height}")
            
            # 3. Tính điểm G (Cái nút nằm bên trong cái vỏ này)
            # Thường nút nằm thụt vào 30px so với mép trái vỏ
            # Và nằm chính giữa chiều cao của vỏ
            target_x = rect.x + 30
            target_y = rect.y + (rect.height / 2)
            
            print(f">>> Tính toán điểm bắn: X={target_x}, Y={target_y}")

            # [DEBUG] Vẽ chấm đỏ để bố xem nó định bắn vào đâu
            await page.evaluate(f'''
                var dot = document.createElement('div');
                dot.style.position = 'fixed';
                dot.style.left = '{target_x}px';
                dot.style.top = '{target_y}px';
                dot.style.width = '10px';
                dot.style.height = '10px';
                dot.style.backgroundColor = 'red';
                dot.style.borderRadius = '50%';
                dot.style.zIndex = '999999';
                dot.style.pointerEvents = 'none';
                document.body.appendChild(dot);
            ''')
            
            # 4. Di chuột và Click
            print(">>> Di chuột đến chấm đỏ...")
            await page.mouse.move(target_x, target_y, duration=500)
            await page.mouse.click(target_x, target_y)
            
            print(">>> ĐÃ CLICK! Bố nhìn xem xanh chưa?")
            await asyncio.sleep(5)
            
        else:
            print("??? Vẫn không thấy cả cái vỏ hộp. Bố check lại mạng giúp con.")

    except Exception as e:
        print(f"Lỗi: {e}")

    print(">>> [XONG] Ctrl+C để tắt.")
    await asyncio.sleep(999)

if __name__ == "__main__":
    n.loop().run_until_complete(main())