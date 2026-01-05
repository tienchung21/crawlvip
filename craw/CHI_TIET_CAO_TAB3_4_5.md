# PHÂN TÍCH CHI TIẾT CHỨC NĂNG CÀO (TAB 3, 4, 5 TRONG DASHBOARD)

## 1. Tổng quan các tab
- **Tab 3: Crawl Listing**
  - Cào danh sách link bài viết từ trang listing (danh mục) sử dụng nodriver (undetected-chromedriver).
  - Lưu link vào database (collected_links), hỗ trợ lọc theo domain, loại hình.
  - Cho phép upload template (JSON) để tự động lấy selector.
  - Cho phép cấu hình nâng cao: fake scroll, fake hover, delay, số trang, v.v.
- **Tab 4: Download Images**
  - Tải ảnh từ danh sách URL hoặc từ bảng scraped_detail_images trong database.
  - Lưu ảnh vào thư mục chỉ định, ghi log trạng thái vào database.
  - Hỗ trợ tải theo ID range, phân trang, giới hạn tốc độ tải.
- **Tab 5: Auto Schedule**
  - Quản lý các task tự động: listing -> detail -> image.
  - Cho phép thêm/sửa/xóa task, cấu hình chi tiết từng bước, chọn template, domain, loại hình, lịch chạy.
  - Hiển thị trạng thái, log, lịch sử chạy task.

## 2. File và hàm liên quan

### Tab 3: Crawl Listing
- **dashboard.py**: Toàn bộ UI và logic tab 3 nằm trong file này.
- **listing_simple_core.py**: Hàm `crawl_listing_simple` thực hiện cào link sử dụng nodriver.
- **database.py**: Lưu link vào database, truy vấn link đã thu thập.
- **template/*.json**: Template chứa selector cho từng site.
- **Các hàm chính:**
  - `crawl_listing_simple`: Nhận URL, item_selector, next_selector, số trang, domain, loại hình, v.v. Mở browser, fake scroll, lấy link, lưu vào DB.
  - `check_and_print_cookie`: Kiểm tra cookie cf_clearance để debug anti-bot.
  - UI: Nhận input từ user, upload template, hiển thị kết quả, lọc link theo domain/loại hình.

### Tab 4: Download Images
- **dashboard.py**: UI và logic tab 4.
- **database.py**: Lưu log tải ảnh, truy vấn ảnh đã tải, cập nhật trạng thái.
- **output/images/**: Thư mục lưu ảnh tải về.
- **Các hàm chính:**
  - Đọc danh sách URL ảnh từ textarea hoặc từ DB (scraped_detail_images).
  - Dùng requests tải ảnh, lưu file, log trạng thái (SUCCESS/FAILED) vào DB.
  - Hỗ trợ tải theo ID range, phân trang, hiển thị lịch sử tải.

### Tab 5: Auto Schedule
- **dashboard.py**: UI và logic tab 5.
- **scheduler_service.py**: Chạy nền, thực thi các task theo lịch.
- **database.py**: Lưu task, log, trạng thái, truy vấn task.
- **listing_crawler.py, scraper_core.py, web_scraper.py**: Thực thi từng bước của pipeline (listing -> detail -> image).
- **template/*.json**: Template cho từng bước.
- **Các hàm chính:**
  - UI: Thêm/sửa/xóa task, chọn template, domain, loại hình, cấu hình chi tiết từng bước.
  - `run_scheduler_loop` (scheduler_service.py): Vòng lặp nền, lấy task đến hạn, gọi `run_task`.
  - `run_task`: Gọi lần lượt các bước: crawl_listing -> scrape_pending_links -> download_images.
  - Log trạng thái, cập nhật DB, gửi thông báo Telegram nếu cấu hình.

## 3. Luồng xử lý chi tiết

### Tab 3: Crawl Listing
1. User nhập URL, selector, số trang, domain, loại hình hoặc upload template.
2. Nhấn "Start Crawling" -> gọi `crawl_listing_simple` (listing_simple_core.py):
   - Mở browser với profile riêng, fake scroll, lấy link theo item_selector, chuyển trang bằng next_selector.
   - Lưu link vào DB (collected_links), gán domain, loại hình.
   - Hiển thị kết quả, cho phép lọc, xóa, reset ID, xuất dữ liệu.
3. Có thể tiếp tục cào detail (bằng template) từ các link đã thu thập.

### Tab 4: Download Images
1. User nhập danh sách URL ảnh hoặc chọn tải từ scraped_detail_images (theo trang hoặc ID range).
2. Nhấn "Bắt đầu tải ảnh" -> dùng requests tải từng ảnh, lưu file theo hash, log trạng thái vào DB.
3. Hiển thị lịch sử tải, trạng thái từng ảnh, cho phép phân trang, lọc domain.

### Tab 5: Auto Schedule
1. User thêm task mới: chọn template, URL, domain, loại hình, cấu hình từng bước (listing/detail/image), lịch chạy.
2. Chạy `python scheduler_service.py` để kích hoạt scheduler nền.
3. Scheduler lấy task đến hạn, gọi lần lượt:
   - **crawl_listing** (listing_crawler.py): Cào link theo template, lưu vào DB.
   - **scrape_pending_links** (scheduler_service.py): Cào detail từ link PENDING, lưu kết quả vào DB.
   - **download_images** (scheduler_service.py): Tải ảnh từ scraped_detail_images, cập nhật trạng thái.
4. Log trạng thái từng bước, cập nhật DB, gửi thông báo Telegram nếu cấu hình.
5. UI tab 5 cho phép xem, sửa, xóa, chạy ngay, xem log từng task.

## 4. Tác dụng từng hàm chính
- **crawl_listing_simple**: Cào link từ trang listing, fake scroll, chuyển trang, lưu link vào DB.
- **check_and_print_cookie**: Debug cookie cf_clearance để kiểm tra anti-bot.
- **download_images**: Tải ảnh từ URL, lưu file, log trạng thái vào DB.
- **run_scheduler_loop**: Vòng lặp nền, lấy task đến hạn, gọi run_task.
- **run_task**: Thực thi pipeline: listing -> detail -> image, log trạng thái, cập nhật DB.
- **scrape_pending_links**: Cào detail từ link PENDING, lưu kết quả vào DB, cập nhật trạng thái.

## 5. Kết nối giữa các file
- **dashboard.py**: UI chính, gọi các hàm cào/link/image, thao tác với DB.
- **listing_simple_core.py, listing_crawler.py**: Cào link listing.
- **scraper_core.py, web_scraper.py**: Cào detail, extract dữ liệu.
- **database.py**: Lưu/truy vấn link, ảnh, task, log.
- **scheduler_service.py**: Chạy nền, thực thi pipeline tự động.

## 6. Tham khảo thêm
- Xem chi tiết code trong dashboard.py, scheduler_service.py, listing_simple_core.py, web_scraper.py, database.py để hiểu rõ từng bước xử lý, các hàm và class liên quan.
- Template JSON trong thư mục template/ dùng để định nghĩa selector cho từng site, từng bước (listing/detail).
