# 📋 HƯỚNG DẪN SETUP PROJECT CRAWL-DATA

## 🖥️ YÊU CẦU HỆ THỐNG

### Phần cứng tối thiểu
- **RAM**: 8GB (khuyến nghị 16GB nếu chạy nhiều task song song)
- **CPU**: 4 cores
- **Disk**: 10GB trống (cho browser profiles và data)

### Hệ điều hành
- Windows 10/11 (đã test)
- Ubuntu 20.04+ (cần điều chỉnh một số path)

---

## 🔧 CÀI ĐẶT STEP BY STEP

### 1. Cài đặt Python
```bash
# Tải Python 3.11 hoặc 3.12 từ https://www.python.org/downloads/
# Khi cài, TICK vào "Add Python to PATH"

# Verify
python --version  # Phải hiện Python 3.11.x hoặc 3.12.x
```

### 2. Cài đặt MySQL/MariaDB
```bash
# Option 1: Dùng Laragon (Windows - Khuyến nghị)
# Tải từ https://laragon.org/download/
# Laragon đã bao gồm MySQL, Apache, PHP

# Option 2: Cài MySQL riêng
# Tải từ https://dev.mysql.com/downloads/installer/

# Option 3: Dùng XAMPP
# Tải từ https://www.apachefriends.org/
```

### 3. Tạo Database
```sql
-- Chạy file create_database.sql trong MySQL
-- Hoặc chạy từng lệnh:

CREATE DATABASE IF NOT EXISTS `craw_db` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `craw_db`;

-- Xem file create_database.sql để tạo đầy đủ các bảng
```

### 4. Cài đặt Node.js (cho Playwright)
```bash
# Tải từ https://nodejs.org/
# Chọn bản LTS (ví dụ: 20.x)

# Verify
node --version
npm --version
```

### 5. Clone/Copy Project
```bash
# Copy toàn bộ folder craw vào vị trí mong muốn
# Ví dụ: C:\projects\craw hoặc /home/user/craw
```

### 6. Cài đặt Python Dependencies
```bash
cd path/to/craw

# Tạo virtual environment (khuyến nghị)
python -m venv venv

# Activate venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Cài đặt dependencies
pip install -r requirements.txt
```

### 7. Cài đặt thêm các thư viện bắt buộc
```bash
# Các thư viện chính (phiên bản đã test)
pip install crawl4ai==0.7.8
pip install nodriver==0.48.1
pip install playwright==1.57.0
pip install pymysql==1.1.2
pip install lxml==5.4.0
pip install streamlit==1.52.2
pip install pandas==2.3.3
pip install openpyxl
pip install requests==2.32.5
pip install pillow==12.0.0
pip install beautifulsoup4==4.14.3
pip install python-dotenv==1.2.1
```

### 8. Cài đặt Playwright Browsers
```bash
# QUAN TRỌNG: Phải chạy sau khi cài playwright
playwright install chromium

# Hoặc cài tất cả browsers
playwright install
```

### 9. Setup Crawl4AI (lần đầu)
```bash
# Chạy setup của crawl4ai
crawl4ai-setup

# Hoặc
python -m crawl4ai.setup
```

---

## 📁 CẤU TRÚC THƯ MỤC QUAN TRỌNG

```
craw/
├── app.py                    # (không dùng)
├── dashboard.py              # 🎯 Dashboard Streamlit chính
├── scheduler_service.py      # 🎯 Background scheduler service
├── database.py               # Database operations
├── scraper_core.py           # Core scraping logic
├── web_scraper.py            # WebScraper wrapper cho Crawl4AI
├── listing_crawler.py        # Crawler cho listing pages
├── create_database.sql       # SQL tạo database
├── requirements.txt          # Python dependencies
├── template/                 # Folder chứa template JSON
├── output/                   # Output files
├── playwright_profile*/      # Browser profiles (tự tạo)
└── nodriver_profile*/        # Nodriver profiles (tự tạo)
```

---

## 🚀 CÁCH CHẠY

### Chạy Dashboard (Giao diện quản lý)
```bash
cd path/to/craw
streamlit run dashboard.py

# Mở browser: http://localhost:8501
```

### Chạy Scheduler Service (Background)
```bash
cd path/to/craw
python scheduler_service.py

# Service sẽ chạy liên tục, check database mỗi 2 giây
# Nhấn Ctrl+C 2 lần để dừng
```

### Chạy cả 2 cùng lúc (Production)
```bash
# Terminal 1: Scheduler
python scheduler_service.py

# Terminal 2: Dashboard
streamlit run dashboard.py
```

---

## ⚙️ CẤU HÌNH DATABASE

Mặc định project kết nối MySQL với:
- **Host**: localhost
- **User**: root
- **Password**: (trống)
- **Database**: craw_db

Nếu cần thay đổi, sửa trong các file:
- `database.py` - dòng khởi tạo Database class
- `scheduler_service.py` - dòng `Database(host=..., user=..., password=..., database=...)`
- `dashboard.py` - tương tự

---

## 📦 DANH SÁCH ĐẦY ĐỦ DEPENDENCIES

### requirements.txt đầy đủ
```
# Core crawling
crawl4ai==0.7.8
nodriver==0.48.1
playwright==1.57.0

# Database
pymysql==1.1.2

# HTML parsing
lxml==5.4.0
beautifulsoup4==4.14.3

# Web framework
streamlit==1.52.2

# Data processing
pandas==2.3.3
openpyxl

# HTTP & Images
requests==2.32.5
pillow==12.0.0

# Utils
python-dotenv==1.2.1
```

### Phiên bản đã test hoạt động tốt
```
Python 3.12.x
crawl4ai 0.7.8
nodriver 0.48.1
playwright 1.57.0
streamlit 1.52.2
pandas 2.3.3
lxml 5.4.0
pymysql 1.1.2
pillow 12.0.0
```

---

## 🔐 OPTIONAL: Telegram Notifications

Nếu muốn nhận thông báo qua Telegram:

1. Tạo bot Telegram qua @BotFather
2. Lấy Bot Token và Chat ID
3. Set environment variables:

```bash
# Windows
set TELEGRAM_BOT_TOKEN=your_bot_token
set TELEGRAM_CHAT_ID=your_chat_id

# Linux
export TELEGRAM_BOT_TOKEN=your_bot_token
export TELEGRAM_CHAT_ID=your_chat_id
```

---

## 🐛 TROUBLESHOOTING

### Lỗi "playwright not found"
```bash
pip install playwright
playwright install chromium
```

### Lỗi "nodriver not found"
```bash
pip install nodriver
```

### Lỗi MySQL connection
```bash
# Check MySQL đang chạy
# Laragon: Start All Services
# XAMPP: Start MySQL

# Test connection
python -c "import pymysql; conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db'); print('OK')"
```

### Lỗi encoding trên Windows
```bash
# Set UTF-8 cho terminal
chcp 65001
```

### Lỗi "Target page, context or browser has been closed"
- Thường do Cloudflare block
- Thử lại sau vài phút
- Hoặc giảm số task chạy song song

### Browser profile bị lock
```bash
# Xóa file lock
del playwright_profile_tab3_detail\.in_use.lock
```

---

## 📝 GHI CHÚ QUAN TRỌNG

1. **Browser Profiles**: Các folder `playwright_profile_*` và `nodriver_profile_*` chứa cookies và session. KHÔNG XÓA nếu muốn giữ đăng nhập.

2. **Template JSON**: Các file template trong `template/` định nghĩa cách extract data. Xuất từ Chrome Extension.

3. **Parallel Tasks**: Mỗi task chạy trên profile riêng. Task song song tự động tạo profile mới với suffix `_taskid`.

4. **Signal Handling**: Scheduler bỏ qua SIGINT đơn lẻ (do Chromium gửi). Nhấn Ctrl+C **2 lần trong 3 giây** để dừng.

5. **Database Backup**: Định kỳ backup database `craw_db` để tránh mất dữ liệu.

---

## 🔄 CẬP NHẬT

Khi cần update dependencies:
```bash
pip install --upgrade crawl4ai nodriver playwright streamlit
playwright install chromium
```

---

*Last updated: January 2026*
pip install -r requirements.txt