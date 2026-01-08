# CHI TIET TAB 3, TAB 4, TAB 5 (DASHBOARD)

Tai lieu nay mo ta tong quan va luong xu ly hien tai cho Tab 3/4/5 trong dashboard, cap nhat theo code moi nhat.

---

## 1) Tong quan cac tab

### Tab 3 - Crawl Listing + Detail (thu cong)
- Muc tieu: crawl link listing, sau do crawl detail theo template JSON ngay trong UI.
- File chinh: `craw/dashboard.py`
- Diem nhan:
  - Listing dung nodriver (selector CSS) de lay link.
  - Detail dung Playwright (display_page) + lxml extract (CSS/XPath).
  - Co click hien so dien thoai (batdongsan, nhatot) truoc khi extract.
  - Luu detail vao `scraped_details_flat`, `scraped_details` va anh vao `scraped_detail_images`.

### Tab 4 - Download Images
- Muc tieu: tai anh tu danh sach URL hoac tu bang `scraped_detail_images`.
- File chinh: `craw/dashboard.py`
- Co co che tai lai va ghi log vao `downloaded_images`.

### Tab 5 - Auto Schedule
- Muc tieu: tao task tu dong chay listing -> detail -> image theo lich.
- UI: `craw/dashboard.py`
- Scheduler: `craw/scheduler_service.py`

---

## 2) File va ham dang dung

### 2.1. UI Dashboard (Tab 3/4/5)
**File:** `craw/dashboard.py`

Ham/chuc nang chinh:
- `convert_template_to_schema(...)`: chuyen template JSON thanh schema cho crawl.
- `scrape_url(...)`: extract detail tu HTML (CSS/XPath), co click hien so dien thoai truoc khi extract.
- `format_extracted_data_fixed(...)`: format du lieu tuong thich template.
- Tab 3 detail: load template -> loop link -> luu DB.
- Tab 4: download anh + ghi log.
- Tab 5: form tao task, hien log, cho phep run now/cancel.

Helper quan trong:
- `_reveal_phone_before_extract(...)`: click selector so dien thoai (batdongsan/nhatot) va cho doi 2s.
- `_get_phone_text_from_page(...)`: doc so dien thoai tu DOM (tel:, data-phone, mobile, nhatot button, batdongsan div co mobile).
- `_fetch_cities(...)`, `_fetch_city_children(...)`: lay tinh/xa theo `transaction_city_merge`.

### 2.2. Listing crawl (Tab 3)
**File:** `craw/listing_simple_core.py`
- `crawl_listing_simple(...)`: dung nodriver mo trang listing, lay link item, next page, luu vao `collected_links`.
- Luu kem domain, loaihinh, tinh/xa (cu + moi) vao `collected_links`.

### 2.3. Detail crawl (Tab 3)
**File:** `craw/dashboard.py`
- `scrape_url(...)`: dung display_page (Playwright) neu co, fallback scrape_simple neu khong co page.
- Hanh vi:
  - delay theo config (wait_load min/max, delay min/max).
  - fake hover/scroll neu bat.
  - click hien so dien thoai (batdongsan/nhatot).
  - extract theo template (CSS/XPath) bang lxml.
  - luu detail vao `scraped_details_flat`, raw vao `scraped_details`, anh vao `scraped_detail_images`.

### 2.4. Scheduler (Tab 5)
**File:** `craw/scheduler_service.py`
- `run_scheduler_loop()`: vong lap chay task den han.
- `run_task(...)`: pipeline listing -> detail -> image.
- `scrape_pending_links(...)`: crawl detail tu cac link PENDING.
- `download_images(...)`: tai anh tu DB.

Diem moi:
- Co lock `.scheduler_service.lock` de tranh chay 2 scheduler.
- Co cancelable sleep va cancel_requested.
- Khi detail, chay theo batch 10 link, lap den khi het PENDING.

### 2.5. Core detail (Scheduler)
**File:** `craw/scraper_core.py`
- `scrape_url(...)`: logic tuong tu Tab 3, dung Playwright + lxml extract.
- Ho tro doc so dien thoai tu DOM va cac attribute (tel:, data-phone, mobile).
- Ho tro doc so mogi tu `ng-bind="PhoneFormat('...')"`.

### 2.6. WebScraper wrapper
**File:** `craw/web_scraper.py`
- `WebScraper.scrape_simple(...)`: lay HTML/markdown.
- `WebScraper.get_active_page(...)`: lay page de thao tac truc tiep.

---

## 3) Luong xu ly chi tiet

### 3.1. Tab 3 - Listing (thu cong)
1) Nhap URL + selector hoac upload template.
2) UI goi `crawl_listing_simple(...)`.
3) Nodriver mo trang -> lay link -> next page -> lap.
4) Luu vao `collected_links` (domain, loaihinh, tinh/xa cu + moi).

### 3.2. Tab 3 - Detail (thu cong)
1) Chon khoang ID link.
2) Load template detail JSON.
3) Moi link:
   - delay theo config
   - fake hover/scroll neu bat
   - click hien so (batdongsan/nhatot)
   - extract theo template (CSS/XPath)
   - luu vao DB

### 3.3. Tab 4 - Download Images
1) Nhap list URL hoac chon lay tu `scraped_detail_images`.
2) Tai anh bang requests, luu file.
3) Ghi log vao `downloaded_images`.

### 3.4. Tab 5 - Auto Schedule
1) Tao task (listing/detail/image + lich).
2) Chay `python scheduler_service.py`.
3) Scheduler chay task:
   - Listing: `listing_crawler.crawl_listing(...)`
   - Detail: `scrape_pending_links(...)`
   - Image: `download_images(...)`
4) Log vao `scheduler_logs`, cap nhat trang thai task.

Diem quan trong:
- Lay link detail theo domain + loaihinh.
- Claim link bang `FOR UPDATE SKIP LOCKED` va update status `IN_PROGRESS` de tranh trung.
- Moi batch chi claim 10 link, lap den khi het PENDING.

---

## 4) Bang du lieu lien quan

- `collected_links`: link listing (domain, loaihinh, tinh/xa cu + moi).
- `scraped_details`: raw detail JSON.
- `scraped_details_flat`: detail dang phang (cot ro rang).
- `scraped_detail_images`: URL anh theo detail_id.
- `downloaded_images`: log tai anh.
- `scheduler_tasks`: task tu dong.
- `scheduler_logs`: log chay task.

---

## 5) Template JSON

**Thu muc:** `craw/template/`

Moi template gom:
- `name`, `url`, `createdAt`, `fields`
- `fields[]`: `name`, `selector`, `valueType`
- Ho tro ca CSS va XPath.

---

## 6) Tom tat theo file

- `craw/dashboard.py`: UI + logic Tab 3/4/5, extract detail bang lxml.
- `craw/listing_simple_core.py`: crawl listing bang nodriver.
- `craw/scheduler_service.py`: chay task tu dong theo lich.
- `craw/scraper_core.py`: crawl detail cho scheduler.
- `craw/web_scraper.py`: wrapper Playwright/Crawl4AI.
- `craw/database.py`: thao tac DB (links, details, images, tasks, logs).
