from seleniumbase import SB
import time
import random
import json

CONFIG = {
  "fields": [
    {"name": "img", "valueType": "src", "selector": ".re__pr-media-slide img"},
    {"name": "title", "valueType": "text", "selector": ".re__pr-title"},
    {"name": "mota", "valueType": "text", "selector": "//div[contains(@class, 're__pr-description')]//div[contains(@class, 're__section-body')]"},
    {"name": "khoanggia", "valueType": "text", "selector": "//span[contains(text(), 'Mức giá') or contains(text(), 'Giá')]/following-sibling::span"},
    {"name": "dientich", "valueType": "text", "selector": "//span[contains(text(), 'Diện tích')]/following-sibling::span"},
    {"name": "sophongngu", "valueType": "text", "selector": "//span[contains(text(), 'Số phòng ngủ')]/following-sibling::span"},
    {"name": "sophongvesinh", "valueType": "text", "selector": "//span[contains(text(), 'Số phòng tắm')]/following-sibling::span"},
    {"name": "huongnha", "valueType": "text", "selector": "//span[contains(text(), 'Hướng nhà')]/following-sibling::span"},
    {"name": "huongbancong", "valueType": "text", "selector": "//span[contains(text(), 'Hướng ban công')]/following-sibling::span"},
    {"name": "mattien", "valueType": "text", "selector": "//span[contains(text(), 'Mặt tiền')]/following-sibling::span"},
    {"name": "duongvao", "valueType": "text", "selector": "//span[contains(text(), 'Đường vào')]/following-sibling::span"},
    {"name": "phaply", "valueType": "text", "selector": "//span[contains(text(), 'Pháp lý')]/following-sibling::span"},
    {"name": "noithat", "valueType": "text", "selector": "//span[contains(text(), 'Nội thất')]/following-sibling::span"},
    {"name": "trangthaiduan", "valueType": "text", "selector": ".re__project-card-info .re__long-text"},
    {"name": "tenmoigioi", "valueType": "text", "selector": ".re__contact-name"},
    {"name": "sodienthoai", "valueType": "text", "selector": ".re__contact-area .re__btn span"},
    {"name": "diachi", "valueType": "text", "selector": ".re__pr-short-description"},
    {"name": "ngaydang", "valueType": "text", "selector": "//span[contains(text(), 'Ngày đăng')]/following-sibling::span"},
    {"name": "matin", "valueType": "text", "selector": "//span[contains(text(), 'Mã tin')]/following-sibling::span"}
  ]
}

URLS = [
    "https://batdongsan.com.vn/ban-nha-biet-thu-lien-ke-duong-n1-phuong-son-ky-prj-the-glen-celadon-city/-tt-36-thang-gia-goc-tu-cdt-vi-tri-dep-chiet-khau-30-tt-22-nhan-nha-pr44861731",
    "https://batdongsan.com.vn/ban-nha-biet-thu-lien-ke-xa-long-hoa-prj-vinhomes-green-paradise/suat-ngoai-giao-can-gio-uu-dai-chiet-khau-18-vi-tri-sieu-dep-lh-chu-dau-tu-pr43991493",
    "https://batdongsan.com.vn/ban-dat-xa-co-dong/ban-son-tay-95-1m-mt-4-4m-duong-nhua-o-to-vao-tan-noi-gia-dau-tu-f0-pr44663404"
]

def crawl_batdongsan_fix():
    # headless2=True để ẩn danh, nhưng bypass tốt
    with SB(uc=True, headless2=True) as sb:
        
        all_results = []
        
        for index, url in enumerate(URLS):
            print(f"\n--- [{index + 1}/{len(URLS)}] Đang vào: {url} ---")
            
            try:
                # Mở link (reconnect=2 để thử lại nhanh nếu lỗi mạng)
                sb.uc_open_with_reconnect(url, reconnect_time=3)
                
                # Click captcha nếu có
                try: sb.uc_gui_click_captcha()
                except: pass

                # Chờ tiêu đề hiện ra tối đa 5s
                try: sb.wait_for_element(".re__pr-title", timeout=5)
                except: print("-> Bỏ qua: Không load được trang này"); continue

                item_data = {"url": url}
                
                # --- PHẦN QUAN TRỌNG ĐÃ SỬA ---
                for field in CONFIG["fields"]:
                    key = field["name"]
                    selector = field["selector"]
                    v_type = field["valueType"]
                    
                    val = None
                    try:
                        # Kiểm tra xem có hiện trên màn hình không (Visible)
                        # Nếu có thì lấy, timeout cực ngắn (1 giây)
                        if sb.is_element_visible(selector):
                            if v_type == "src":
                                val = sb.get_attribute(selector, "src", timeout=1)
                            else:
                                val = sb.get_text(selector, timeout=1)
                    except Exception:
                        pass # Lỗi thì coi như None, không in log rác làm rối mắt
                    
                    item_data[key] = val
                
                # In kết quả NGAY LẬP TỨC để bố kiểm tra luôn
                print(f"-> Đã lấy: {item_data.get('title', 'Không tiêu đề')[:50]}...")
                print(f"-> Giá: {item_data.get('khoanggia')}")
                
                all_results.append(item_data)
                
                # Nghỉ ngơi xíu
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                print(f"Lỗi ở link này: {str(e)}")

        print("\n\n=== TỔNG HỢP KẾT QUẢ JSON (Copy phần dưới này) ===")
        print(json.dumps(all_results, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    crawl_batdongsan_fix()