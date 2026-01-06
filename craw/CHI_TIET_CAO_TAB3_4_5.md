# CHI TIET TAB 3, TAB 4, TAB 5 (DASHBOARD)

Tài liệu này mô tả chi tiết: file nào đang chạy, hàm nào phụ trách phần nào, và chức năng cụ thể của từng tab.

---

## 1) Tong quan cac tab

### Tab 3 - Crawl Listing + Detail (thu cong)
- Mục tiêu: cào link listing và cào detail theo template ngay trong UI.
- File chính: `craw/dashboard.py`
- Các khối logic chính:
  - Crawl listing bằng nodriver.
  - Cào detail bằng Crawl4AI + Playwright với template JSON.
  - Lưu kết quả detail vào DB và lưu ảnh vào bảng ảnh.

### Tab 4 - Download Images
- Mục tiêu: tải ảnh từ danh sách URL hoặc từ bảng `scraped_detail_images`.
- File chính: `craw/dashboard.py`
- Dùng requests để tải ảnh, lưu vào thư mục chỉ định, ghi log vào DB.

### Tab 5 - Auto Schedule
- Mục tiêu: tạo task tự động listing -> detail -> image theo lịch.
- UI trong `craw/dashboard.py`
- Nền chạy trong `craw/scheduler_service.py`

---

## 2) File va ham dang dung

### 2.1. UI Dashboard (Tab 3/4/5)
**File:** `craw/dashboard.py`

Các hàm/chức năng chính:
- `convert_template_to_schema(...)`: chuyển template JSON thành schema cho Crawl4AI.
- `scrape_url(...)`: cào detail trực tiếp bằng lxml (hỗ trợ XPath + CSS), có bước click hiện số điện thoại.
- `format_extracted_data_fixed(...)`: chuẩn hoá dữ liệu sau extract.
- `scrape_detail_pages(...)` (trong Tab 3): vòng lặp cào detail theo danh sách link, lưu DB.
- Tab 4: block download ảnh dùng requests + `_save_image_bytes(...)`.
- Tab 5: form tạo task, hiển thị logs, trigger scheduler.

Các helper quan trọng:
- `_reveal_phone_before_extract(...)`: click hiện số điện thoại trước khi extract.
- `_get_phone_text_from_page(...)`: đọc số từ DOM (tel:, data-phone, mobile, button nhatot, v.v.).
- `_fetch_cities(...)`, `_fetch_city_children(...)`: lấy tỉnh/xã (transaction_city_merge).

### 2.2. Listing crawl (Tab 3)
**File:** `craw/listing_simple_core.py`

Hàm chính:
- `crawl_listing_simple(...)`: dùng nodriver để mở trang listing, lấy link item, phân trang, lưu vào DB.

Chức năng:
- Lấy item link bằng selector.
- Bấm next page theo selector.
- Lưu vào bảng `collected_links` kèm domain/loaihinh và thông tin tỉnh/xã.

### 2.3. Detail crawl (Tab 3)
**File:** `craw/dashboard.py`

Hàm chính:
- `scrape_url(...)`: chạy cho từng link detail.
- `scrape_detail_pages(...)`: quản lý vòng lặp, delay, fake hover/scroll, lưu DB.

Chức năng:
- Mở trang detail (display_page).
- Click hiện số điện thoại nếu domain là batdongsan/nhatot.
- Extract dữ liệu theo template JSON (CSS/XPath).
- Lưu vào bảng `scraped_details_flat` + `scraped_details`.
- Lưu ảnh vào `scraped_detail_images`.

### 2.4. Scheduler (Tab 5)
**File:** `craw/scheduler_service.py`

Hàm chính:
- `run_scheduler_loop()`: vòng lặp nền đọc task đến hạn, spawn thread.
- `run_task(...)`: chạy pipeline listing -> detail -> image.
- `scrape_pending_links(...)`: cào detail từ các link PENDING.
- `download_images(...)`: tải ảnh từ DB.

Chức năng quan trọng:
- Dùng `cancel_requested` để dừng nhanh khi bấm Cancel.
- Dùng lock `.scheduler_service.lock` để tránh chạy 2 scheduler.
- Reset task treo (`reset_stale_running_tasks()`).

### 2.5. Core crawl detail (Scheduler)
**File:** `craw/scraper_core.py`

Hàm chính:
- `scrape_url(...)`: logic cào detail giống Tab 3, hỗ trợ click hiện số điện thoại.

### 2.6. Crawl4AI wrapper
**File:** `craw/web_scraper.py`

Hàm chính:
- `WebScraper.scrape_simple(...)`: load trang và lấy HTML/markdown.
- `WebScraper.get_active_page(...)`: lấy page để thao tác trực tiếp.

---

## 3) Luong xu ly chi tiet

### 3.1. Tab 3 - Listing (thu cong)
1) User nhập URL + selector hoặc upload template.
2) UI gọi `crawl_listing_simple(...)`.
3) Nodriver mở trang listing -> lấy link -> bấm next -> lặp.
4) Lưu link vào `collected_links` (kèm domain, loaihinh, tỉnh/xã).

### 3.2. Tab 3 - Detail (thu cong)
1) User chọn khoảng ID link để cào.
2) Load template detail JSON.
3) Mỗi link:
   - delay theo cấu hình
   - (nếu bật) fake hover/scroll
   - gọi `scrape_url(...)`
   - click hiện số điện thoại (batdongsan/nhatot)
   - extract theo template
   - lưu DB: `scraped_details_flat`, `scraped_details`, `scraped_detail_images`

### 3.3. Tab 4 - Download Images
1) Nhập list URL hoặc lấy từ `scraped_detail_images`.
2) Dùng requests tải ảnh, lưu file theo hash.
3) Ghi log vào DB (SUCCESS/FAILED).

### 3.4. Tab 5 - Auto Schedule
1) User tạo task (listing/detail/image + lịch).
2) Chạy `python scheduler_service.py`.
3) Scheduler đọc task đến hạn:
   - Listing: `listing_crawler.crawl_listing(...)`
   - Detail: `scrape_pending_links(...)`
   - Image: `download_images(...)`
4) Log vào `scheduler_logs`, cập nhật trạng thái task.

---

## 4) Bang du lieu lien quan

- `collected_links`: lưu link listing (domain, loaihinh, tỉnh/xã cũ & mới).
- `scraped_details`: lưu raw detail JSON.
- `scraped_details_flat`: lưu detail dạng phẳng (các cột rõ ràng).
- `scraped_detail_images`: lưu URL ảnh theo detail_id.
- `scheduler_tasks`: lưu task tự động.
- `scheduler_logs`: log chạy task.

---

## 5) Template JSON

**Thư mục:** `craw/template/`

Mỗi template gồm:
- `name`, `url`, `createdAt`, `fields`
- `fields[]` có `name`, `selector`, `valueType`
- Cả CSS và XPath đều được hỗ trợ.

---

## 6) Tóm tắt nhanh theo file

- `craw/dashboard.py`: UI + logic Tab 3/4/5, extract detail bằng lxml.
- `craw/listing_simple_core.py`: cào link listing bằng nodriver.
- `craw/scheduler_service.py`: chạy task tự động theo lịch.
- `craw/scraper_core.py`: cào detail cho scheduler.
- `craw/web_scraper.py`: wrapper Crawl4AI + Playwright.
- `craw/database.py`: thao tác DB (links, details, images, tasks, logs).
