# 🔧 Fix Dashboard Lag & Tab 5 Crawl Issues

## 📋 Tóm tắt vấn đề

### 1. Dashboard siêu lag
**Nguyên nhân:**
- Database có quá nhiều records:
  - `scraped_detail_images`: **6 triệu records** (898 MB data + 1.1 GB index)
  - `collected_links`: **1 triệu records** (219 MB data + 591 MB index)
  - `data_clean_v1`: **1.3 triệu records** (690 MB data)
- MySQL chạy qua XAMPP/LAMPP, không phải system service
- Tables chưa được optimize

**Đã sửa:**
✅ Chạy OPTIMIZE TABLE cho tất cả các table lớn
✅ Chạy ANALYZE TABLE để cập nhật statistics
✅ Xóa cache browser cũ
✅ Tạo script `fix_dashboard_lag.sh` để tự động optimize

### 2. Tab 5 (Auto Schedule) không crawl được
**Nguyên nhân:**
- File `listing_simple_core.py` có scroll logic **QUÁ ĐƠN GIẢN**
  - Chỉ scroll window, không scroll các container có `overflow: scroll`
  - Không có logging để debug
  - `except Exception: pass` → silent fail
- Không có logging chi tiết cho next button
- Scheduler service không chạy

**Đã sửa:**
✅ Upgrade scroll logic trong `listing_simple_core.py` từ `listing_crawler.py`:
  - Thử `page.scroll_down()` native method trước
  - Fallback sang manual `scrollTo()` với random steps
  - Tìm và scroll các scrollable containers nếu window không scroll được
  - Thêm logging chi tiết

✅ Thêm logging cho:
  - Scroll position trước/sau
  - Next button selector và click status
  - Link extraction với traceback
  
✅ Tạo test script `test_tab5_crawl.py` để debug dễ dàng

## 🚀 Cách sử dụng

### Option 1: Chạy script tự động fix
```bash
cd /home/chungnt/crawlvip/craw
bash fix_dashboard_lag.sh
```

### Option 2: Test Tab 5 crawl riêng
```bash
cd /home/chungnt/crawlvip/craw
python test_tab5_crawl.py
```

Script này sẽ:
- Mở browser (headless=False) để bạn xem quá trình crawl
- Crawl 2 pages từ batdongsan.com.vn
- In ra logging chi tiết về scroll, next button, links extracted

### Option 3: Optimize database thủ công
```bash
/opt/lampp/bin/mysql -u root craw_db < optimize_database.sql
```

### Option 4: Khởi động scheduler service
```bash
cd /home/chungnt/crawlvip/craw
nohup python scheduler_service.py > scheduler.log 2>&1 &

# Kiểm tra log
tail -f scheduler.log
```

## 📁 Files đã thay đổi

### 1. `/home/chungnt/crawlvip/craw/listing_simple_core.py`
**Thay đổi:**
- Line 117-133: Upgrade scroll logic (từ 18 lines → 84 lines)
  - Thử native scroll_down() trước
  - Fallback sang manual scrollTo với random steps
  - Tìm scrollable containers và scroll
  - Logging chi tiết scroll position
  
- Line 292-297: Thêm logging cho link extraction
  - Print số lượng links extracted
  - Print traceback nếu có lỗi
  
- Line 320-336: Thêm logging cho next button
  - Print selector đang dùng
  - Print status found/not found
  - Print lỗi nếu click fail

### 2. `/home/chungnt/crawlvip/craw/optimize_database.sql` (NEW)
Script SQL để optimize database

### 3. `/home/chungnt/crawlvip/craw/fix_dashboard_lag.sh` (NEW)
Script bash tự động:
- Check MySQL status
- Optimize database
- Check scheduler service
- Clean browser cache

### 4. `/home/chungnt/crawlvip/craw/test_tab5_crawl.py` (NEW)
Test script để debug Tab 5 crawl

## 🔍 Kiểm tra sau khi fix

### Test 1: Dashboard có còn lag không?
```bash
# Restart dashboard
cd /home/chungnt/crawlvip/craw
streamlit run dashboard.py

# Mở browser: http://localhost:8501
# Test các tab xem load nhanh hơn không
```

### Test 2: Tab 5 có crawl được không?
**Trong dashboard:**
1. Vào Tab 5 (Auto Schedule)
2. Chọn:
   - Listing template: (chọn template batdongsan nếu có)
   - Start URL: https://batdongsan.com.vn/nha-dat-ban
   - Max pages: 2
   - **Listing show browser: TRUE** ← Quan trọng để xem browser
   - Listing fake scroll: TRUE
   - Listing wait load: 5-8 seconds
3. Click "Add task" hoặc chạy listing directly

**Hoặc test riêng:**
```bash
cd /home/chungnt/crawlvip/craw
python test_tab5_crawl.py
```

### Test 3: Check logs
```bash
# Nếu chạy scheduler service
tail -f /home/chungnt/crawlvip/craw/scheduler.log

# Hoặc xem log trong dashboard terminal
```

## 🐛 Nếu vẫn còn vấn đề

### Vấn đề: Browser không scroll
**Debug:**
1. Mở browser (show_browser=True)
2. Xem console log trong browser (F12)
3. Xem terminal log:
   ```
   [Scheduler Listing] Starting fake scroll...
   [Scheduler Listing] Using native scroll_down()  # hoặc manual scrollTo()
   [Scheduler Listing] Final scroll position: 0 -> 1234
   ```

### Vấn đề: Không tìm được next button
**Debug:**
1. Kiểm tra selector trong log:
   ```
   [Scheduler Listing] Looking for next page button with selector: ...
   [Scheduler Listing] Next button not found - stopping at page 1
   ```
2. Inspect element trên trang web để lấy selector mới
3. Update selector trong template hoặc code

### Vấn đề: Dashboard vẫn lag
**Debug:**
```bash
# Check table sizes
/opt/lampp/bin/mysql -u root craw_db -e "
SELECT TABLE_NAME, TABLE_ROWS, 
  ROUND(DATA_LENGTH/1024/1024, 2) AS 'Size_MB'
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'craw_db' 
ORDER BY TABLE_ROWS DESC LIMIT 10;
"

# Xem queries chậm
/opt/lampp/bin/mysql -u root craw_db -e "SHOW PROCESSLIST;"
```

## 📊 Database Statistics (trước khi fix)

| Table | Rows | Data Size | Index Size |
|-------|------|-----------|------------|
| scraped_detail_images | 6,016,178 | 898 MB | 1,111 MB |
| data_clean_v1 | 1,303,428 | 690 MB | 300 MB |
| collected_links | 1,075,724 | 219 MB | 591 MB |
| scraped_details_flat | 990,829 | 1,407 MB | 354 MB |
| ad_listing_detail | 263,395 | 262 MB | 0 MB |
| data_clean | 174,325 | 37 MB | 70 MB |

**Tổng:** Hơn **10 triệu records**, khoảng **5 GB dữ liệu**

## ✅ Checklist hoàn thành

- [x] Fix scroll logic trong listing_simple_core.py
- [x] Thêm logging chi tiết
- [x] Optimize database
- [x] Clean browser cache
- [x] Tạo test script
- [x] Tạo fix script tự động
- [x] Viết documentation

## 📞 Support

Nếu còn vấn đề, check:
1. `/home/chungnt/crawlvip/craw/scheduler.log` - Scheduler service logs
2. Terminal output khi chạy dashboard
3. Browser console (F12) khi show_browser=True
4. MySQL error log: `/opt/lampp/logs/mysql_error.log`

---
*Last updated: 2026-01-27*
*Fixed by: GitHub Copilot*
