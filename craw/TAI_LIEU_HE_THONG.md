# 📚 Tài Liệu Hệ Thống - Web Scraper Extension

## 📋 Mục Lục

1. [Tổng Quan](#tổng-quan)
2. [Kiến Trúc Hệ Thống](#kiến-trúc-hệ-thống)
3. [Các Thành Phần Chính](#các-thành-phần-chính)
4. [Chức Năng Chi Tiết](#chức-năng-chi-tiết)
5. [Cài Đặt và Cấu Hình](#cài-đặt-và-cấu-hình)
6. [Hướng Dẫn Sử Dụng](#hướng-dẫn-sử-dụng)
7. [API Documentation](#api-documentation)
8. [Xử Lý Dữ Liệu](#xử-lý-dữ-liệu)
9. [Troubleshooting](#troubleshooting)

---

## 🎯 Tổng Quan 

**Web Scraper Extension** là một hệ thống cào dữ liệu web toàn diện, bao gồm:

- **Browser Extension** (Chrome/Edge): Giao diện người dùng để chọn và cào dữ liệu
- **API Server** (Python): Xử lý logic cào dữ liệu sử dụng Crawl4AI
- **Web Scraper**: Thư viện wrapper cho Crawl4AI với nhiều tính năng nâng cao

### Tính Năng Chính

✅ **Click-to-Select**: Click vào phần tử trên trang để chọn trường cần lấy  
✅ **Smart Selector Generation**: Tự động tạo CSS selector và XPath tối ưu  
✅ **Dual Scraping Mode**: Cào bằng JavaScript (nhanh) hoặc Crawl4AI (mạnh mẽ)  
✅ **Template System**: Lưu và tái sử dụng template cào dữ liệu  
✅ **Lazy Loading Support**: Hỗ trợ lấy ảnh từ `data-src` (lazy loading)  
✅ **Image Filtering**: Tự động loại bỏ SVG, placeholder, và binary data  
✅ **Container Extraction**: Lấy toàn bộ giá trị trong container với `itemprop`  
✅ **Multiple Value Types**: Hỗ trợ text, HTML, attributes (src, href, alt, etc.)  
✅ **Export JSON**: Xuất dữ liệu đã cào thành file JSON  

---

## 💻 Công Nghệ và Phiên Bản

### Ngôn Ngữ Lập Trình

#### Python
- **Phiên bản**: Python 3.10.6
- **Mô tả**: Ngôn ngữ lập trình chính cho backend, API server, và các công cụ scraping
- **Sử dụng cho**: 
  - Streamlit Dashboard (`dashboard.py`)
  - Listing Crawler (`listing_crawler.py`)
  - Web Scraper (`web_scraper.py`)
  - Database Handler (`database.py`)
  - Extension API Server (`extension_api_server.py`)

### Framework và Thư Viện Python

#### 1. Streamlit
- **Phiên bản**: 1.52.1 (yêu cầu >= 1.28.0)
- **Mô tả**: Framework để xây dựng web dashboard tương tác
- **Sử dụng cho**: 
  - Dashboard chính (`dashboard.py`) - giao diện quản lý scraping tasks
  - Hiển thị kết quả scraping
  - Quản lý templates và cấu hình

#### 2. Crawl4AI
- **Phiên bản**: 0.7.7 (yêu cầu >= 0.4.0)
- **Mô tả**: Framework web scraping mạnh mẽ với hỗ trợ JavaScript rendering
- **Sử dụng cho**: 
  - Crawl và extract dữ liệu từ các trang web
  - Hỗ trợ CSS selector và XPath
  - Xử lý lazy loading và dynamic content
  - File: `web_scraper.py`, `listing_crawler.py`

#### 3. Nodriver (undetected-chromedriver)
- **Phiên bản**: 0.48.1
- **Mô tả**: Thư viện browser automation để tránh bot detection
- **Sử dụng cho**: 
  - Crawl listing pages với khả năng tránh phát hiện bot
  - Scroll và lazy load content
  - File: `listing_crawler.py`, `dashboard.py`

#### 4. Playwright
- **Phiên bản**: 1.56.0
- **Mô tả**: Browser automation framework (được sử dụng bởi Crawl4AI)
- **Sử dụng cho**: 
  - Crawl4AI sử dụng Playwright làm engine
  - Hỗ trợ headless browser automation

#### 5. BeautifulSoup4
- **Phiên bản**: 4.14.3 (yêu cầu >= 4.12.0)
- **Mô tả**: Thư viện parsing HTML/XML
- **Sử dụng cho**: 
  - Parse và xử lý HTML content
  - Extract dữ liệu từ DOM

#### 6. Pandas
- **Phiên bản**: 2.3.3 (yêu cầu >= 2.0.0)
- **Mô tả**: Thư viện phân tích và xử lý dữ liệu
- **Sử dụng cho**: 
  - Xử lý và hiển thị dữ liệu scraping trong dashboard
  - Export dữ liệu ra Excel/CSV

#### 7. OpenPyXL
- **Phiên bản**: >= 3.1.0 (trong requirements.txt)
- **Mô tả**: Thư viện đọc/ghi file Excel (.xlsx)
- **Sử dụng cho**: 
  - Export kết quả scraping ra file Excel

#### 8. MySQL Connector
- **Phiên bản**: 
  - `mysql-connector-python`: 9.5.0
  - `pymysql`: (fallback option)
- **Mô tả**: Thư viện kết nối và tương tác với MySQL database
- **Sử dụng cho**: 
  - Lưu trữ collected links trong database
  - Quản lý trạng thái scraping (PENDING, PROCESSED, ERROR)
  - File: `database.py`

#### 9. Python-dotenv
- **Phiên bản**: (trong requirements.txt)
- **Mô tả**: Thư viện quản lý biến môi trường từ file `.env`
- **Sử dụng cho**: 
  - Cấu hình database connection
  - Quản lý API keys và secrets

#### 10. TF-Playwright-Stealth
- **Phiên bản**: 1.2.0
- **Mô tả**: Plugin để tránh bot detection cho Playwright
- **Sử dụng cho**: 
  - Tăng khả năng tránh phát hiện khi scraping

### Database

#### MySQL
- **Phiên bản**: (tùy theo cài đặt Laragon)
- **Mô tả**: Hệ quản trị cơ sở dữ liệu quan hệ
- **Cấu hình mặc định**:
  - Host: `localhost`
  - User: `root`
  - Password: `` (empty)
  - Database: `craw_db`
- **Bảng chính**:
  - `collected_links`: Lưu trữ các link đã thu thập
    - `id`: INT AUTO_INCREMENT PRIMARY KEY
    - `url`: VARCHAR(2000) UNIQUE
    - `status`: VARCHAR(50) DEFAULT 'PENDING'
    - `created_at`: TIMESTAMP

### Browser Extension (Chrome/Edge)

#### Manifest Version
- **Manifest V3**: Phiên bản mới nhất của Chrome Extension API
- **Extension Version**: 1.0.9

#### Công Nghệ Frontend

##### JavaScript (Vanilla)
- **Mô tả**: Không sử dụng framework, pure JavaScript
- **Sử dụng cho**: 
  - `content.js`: Content script chạy trên trang web
  - `sidepanel.js`: Logic UI cho side panel
  - `background.js`: Service worker

##### HTML5
- **Sử dụng cho**: 
  - `sidepanel.html`: Giao diện side panel
  - `manifest.json`: Cấu hình extension

##### CSS3
- **Sử dụng cho**: 
  - `sidepanel.css`: Styling cho side panel
  - `content.css`: Styling cho content script (highlight, overlay)

#### Chrome Extension APIs
- **activeTab**: Truy cập tab hiện tại
- **storage**: Lưu trữ templates và cấu hình
- **scripting**: Inject scripts vào trang
- **tabs**: Quản lý tabs
- **sidePanel**: Hiển thị side panel

### Hệ Điều Hành và Môi Trường

#### Hệ Điều Hành
- **Windows**: 10.0.19045 (Windows 10/11)
- **Shell**: PowerShell

#### Development Environment
- **Laragon**: Local development environment
  - MySQL server
  - PHP (nếu cần)
  - Python environment

### Công Cụ và Utilities

#### URL Parsing
- **urllib.parse**: Module Python chuẩn
  - `urlparse`, `urlunparse`, `parse_qs`, `urlencode`
  - Sử dụng cho: Normalize URLs, parse query parameters

#### Path Handling
- **pathlib**: Module Python chuẩn
  - Sử dụng cho: Quản lý file paths, cross-platform compatibility

#### Async/Await
- **asyncio**: Module Python chuẩn
  - Sử dụng cho: Xử lý asynchronous operations
  - Windows-specific: `WindowsProactorEventLoopPolicy`

#### JSON Processing
- **json**: Module Python chuẩn
  - Sử dụng cho: Serialize/deserialize templates và dữ liệu

#### DateTime
- **datetime**: Module Python chuẩn
  - Sử dụng cho: Timestamp, logging

### Cấu Hình Browser (Nodriver)

#### Performance Optimization
```python
BROWSER_CONFIG_TIET_KIEM = [
    "--blink-settings=imagesEnabled=false", 
    "--disable-images",
    "--mute-audio",
]
```
- **Mục đích**: Giảm lag và tiết kiệm bandwidth
- **Tác dụng**: 
  - Chặn tải ảnh
  - Tắt audio
  - Tăng tốc độ crawl

### Tóm Tắt Phiên Bản

| Công Nghệ | Phiên Bản | Vai Trò và Mục Đích Sử Dụng |
|-----------|-----------|----------------------------|
| **Python** | 3.10.6 | **Ngôn ngữ lập trình chính** - Viết toàn bộ backend, API server, crawler, và dashboard. Hỗ trợ async/await cho xử lý bất đồng bộ. |
| **Streamlit** | 1.52.1 | **Web Dashboard Framework** - Tạo giao diện web tương tác để quản lý scraping tasks, xem kết quả, cấu hình templates. File: `dashboard.py` |
| **Crawl4AI** | 0.7.7 | **Web Scraping Framework chính** - Crawl và extract dữ liệu từ websites với hỗ trợ JavaScript rendering, CSS selector, XPath. File: `web_scraper.py`, `listing_crawler.py` |
| **Nodriver** | 0.48.1 | **Browser Automation với Anti-Detection** - Tránh bot detection khi crawl listing pages, scroll và lazy load content. File: `listing_crawler.py`, `dashboard.py` |
| **Playwright** | 1.56.0 | **Browser Engine** - Được Crawl4AI sử dụng làm engine để điều khiển browser (headless/headful). Tự động cài khi cài Crawl4AI. |
| **BeautifulSoup4** | 4.14.3 | **HTML Parsing Library** - Parse và extract dữ liệu từ HTML đã crawl. Sử dụng trong `extract_batdongsan.py` để parse HTML và tìm các thẻ, attributes, text content. |
| **Pandas** | 2.3.3 | **Data Processing & Display** - Xử lý và hiển thị dữ liệu scraping dưới dạng bảng (DataFrame) trong dashboard. Export ra Excel/CSV. File: `dashboard.py` (hiển thị kết quả, collected links) |
| **MySQL Connector** | 9.5.0 | **Database Connection** - Kết nối và tương tác với MySQL database để lưu collected links, quản lý trạng thái scraping (PENDING, CRAWLED, ERROR). File: `database.py` |
| **Chrome Extension** | Manifest V3 | **Browser Extension Platform** - Nền tảng để xây dựng extension cho Chrome/Edge. Manifest V3 là phiên bản mới nhất của Chrome Extension API với service worker. |
| **Extension Version** | 1.0.9 | **Phiên bản Extension** - Version hiện tại của extension, được khai báo trong `manifest.json`. Dùng để quản lý updates và compatibility. |

### Giải Thích Chi Tiết Vai Trò

#### BeautifulSoup4 (4.14.3) - HTML Parsing
**Vai trò trong dự án:**
- **Parse HTML**: Sau khi Crawl4AI crawl được HTML từ website, BeautifulSoup4 được dùng để parse HTML thành cấu trúc DOM có thể truy vấn
- **Extract dữ liệu**: Tìm và extract các thẻ HTML, attributes, text content dựa trên CSS selector hoặc thẻ HTML
- **File sử dụng**: `extract_batdongsan.py`
  - Parse HTML để tìm title, địa chỉ, giá, mô tả, hình ảnh
  - Tìm các thẻ như `<h1>`, `<span>`, `<strong>` với các class/id cụ thể
  - Extract attributes như `itemprop`, `src`, `href`

**Ví dụ sử dụng:**
```python
soup = BeautifulSoup(html, 'html.parser')
h1 = soup.find('h1')  # Tìm thẻ h1
title = h1.get_text()  # Lấy text content
```

#### Pandas (2.3.3) - Data Processing & Display
**Vai trò trong dự án:**
- **Hiển thị dữ liệu dạng bảng**: Chuyển đổi kết quả scraping (list of dicts) thành DataFrame để hiển thị trong Streamlit dashboard
- **Xử lý dữ liệu**: Filter, sort, group dữ liệu scraping
- **Export dữ liệu**: Xuất kết quả ra file Excel (.xlsx) hoặc CSV
- **File sử dụng**: `dashboard.py`
  - Hiển thị collected links trong Tab 3
  - Hiển thị kết quả scraping trong Tab 2
  - Export results ra Excel/CSV

**Ví dụ sử dụng:**
```python
df = pd.DataFrame(recent_links)  # Chuyển list thành DataFrame
df['created_at'] = pd.to_datetime(df['created_at'])  # Convert datetime
st.dataframe(df)  # Hiển thị trong Streamlit
df.to_excel('results.xlsx')  # Export ra Excel
```

#### Chrome Extension (Manifest V3) - Browser Extension Platform
**Vai trò trong dự án:**
- **Nền tảng extension**: Manifest V3 là phiên bản mới nhất của Chrome Extension API
- **Service Worker**: Thay thế background page bằng service worker (chạy nền)
- **Side Panel API**: Hỗ trợ side panel mới (thay vì popup)
- **Bảo mật**: Tăng cường bảo mật với Content Security Policy (CSP) nghiêm ngặt hơn
- **File**: `extension/manifest.json`

**Tính năng Manifest V3:**
- `activeTab`: Truy cập tab hiện tại khi user click extension
- `storage`: Lưu templates và cấu hình
- `scripting`: Inject content script vào trang web
- `sidePanel`: Hiển thị side panel UI

#### Extension Version (1.0.9) - Version Management
**Vai trò trong dự án:**
- **Version control**: Quản lý phiên bản extension để theo dõi updates
- **Compatibility**: Đảm bảo extension tương thích với các version khác nhau
- **Updates**: Chrome tự động kiểm tra và thông báo khi có version mới
- **File**: `extension/manifest.json` - field `"version": "1.0.9"`

**Cách hoạt động:**
- Khi user cài extension, Chrome lưu version này
- Khi có update, Chrome so sánh version mới với version cũ
- Extension có thể tự động update hoặc yêu cầu user reload

### Yêu Cầu Hệ Thống

#### Python
- Python >= 3.10
- pip (package manager)

#### Database
- MySQL Server (qua Laragon hoặc standalone)
- Quyền tạo database và tables

#### Browser
- Google Chrome hoặc Microsoft Edge (phiên bản mới nhất)
- Để sử dụng extension

#### Network
- Kết nối Internet để crawl websites
- Proxy (tùy chọn) để tránh rate limiting

---

## 🏗️ Kiến Trúc Hệ Thống

```
┌─────────────────────────────────────────────────────────────┐
│                    Browser Extension                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Side Panel  │  │ Content Script│  │ Background   │     │
│  │  (UI)        │  │ (Selection)   │  │ (Service)    │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│         │                  │                  │              │
│         └──────────────────┴──────────────────┘              │
│                            │                                   │
│                            ▼                                   │
│                    HTTP POST Request                           │
└────────────────────────────┼───────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│              Extension API Server (Python)                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ExtensionAPIHandler                                  │   │
│  │  - handle_scrape_with_template()                     │   │
│  │  - handle_scrape_with_fields()                       │   │
│  └──────────────────┬───────────────────────────────────┘   │
│                     │                                         │
│                     ▼                                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  WebScraper (Wrapper)                                │   │
│  │  - scrape_with_schema()                              │   │
│  │  - scrape_simple()                                   │   │
│  └──────────────────┬───────────────────────────────────┘   │
│                     │                                         │
│                     ▼                                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Crawl4AI                                             │   │
│  │  - AsyncWebCrawler                                    │   │
│  │  - JsonCssExtractionStrategy                         │   │
│  └───────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 Các Thành Phần Chính

### 1. Browser Extension (`extension/`)

#### 1.1. Manifest (`manifest.json`)
- **Version**: 1.0.9
- **Permissions**: `activeTab`, `storage`, `scripting`, `tabs`, `sidePanel`
- **Content Scripts**: Chạy trên tất cả các trang web (trừ localhost, streamlit)
- **Side Panel**: Giao diện chính của extension

#### 1.2. Content Script (`content.js`)
**Chức năng:**
- Xử lý click để chọn phần tử trên trang
- Tạo CSS selector và XPath tự động
- Highlight các phần tử đã chọn
- Extract dữ liệu từ DOM (client-side scraping)
- Tìm label/description cho các trường (sibling strategy, uncle strategy)

**Tính năng nổi bật:**
- **Smart Selector Generation**: 
  - Ưu tiên ID, data attributes
  - Sử dụng `itemprop` attribute cho XPath chính xác
  - Tìm label từ sibling hoặc "uncle" element (div.a4ep88f)
  - Fallback về absolute XPath nếu cần
- **Keyboard Shortcut**: Nhấn `X` để toggle giữa CSS và XPath
- **Preview Value**: Hiển thị preview giá trị trước khi crawl

#### 1.3. Side Panel (`sidepanel.js`, `sidepanel.html`, `sidepanel.css`)
**Chức năng:**
- Giao diện quản lý các trường đã chọn
- Bật/tắt chế độ chọn phần tử
- Chọn loại selector (CSS/XPath)
- Chọn loại giá trị (text, html, src, href, all, etc.)
- Preview giá trị đã chọn
- Cào dữ liệu (JavaScript hoặc Crawl4AI)
- Export JSON
- Lưu/Load template

**UI Components:**
- Fields list với min-height 100px
- Action buttons (Cào, Export, Lưu Template)
- Selector type toggle (CSS/XPath)
- Value type dropdown (text, html, src, href, all, etc.)

#### 1.4. Background Script (`background.js`)
- Service worker cho extension
- Xử lý các sự kiện extension
- Quản lý side panel

### 2. API Server (`extension_api_server.py`)

**Port mặc định**: `8765`

**Endpoints:**
- `GET /`: Health check
- `POST /`: Xử lý các action từ extension

**Actions hỗ trợ:**
1. `scrape_with_template`: Cào dữ liệu sử dụng template đã lưu
2. `scrape_with_fields`: Cào dữ liệu với các trường được chọn trực tiếp

**Tính năng xử lý:**
- Chuyển đổi selector từ extension format sang Crawl4AI schema
- Xử lý `valueType: 'all'` (container extraction với `itemprop`)
- Xử lý `valueType: 'src'` (lazy loading images)
- Filter binary data, SVG, placeholder images
- Debug logging chi tiết

### 3. Web Scraper (`web_scraper.py`)

**Class**: `WebScraper`

**Methods:**
- `scrape_simple(url)`: Cào đơn giản, lấy toàn bộ nội dung
- `scrape_with_schema(url, schema)`: Cào với schema định nghĩa các trường
- `scrape_with_llm(url, prompt)`: Cào sử dụng LLM để extract (nếu cấu hình)

**Features:**
- Async/await support
- Context manager (`async with`)
- Browser configuration (headless, viewport)
- Cache mode support

### 4. Template System

**Template Format:**
```json
{
  "name": "Template Name",
  "url": "https://example.com",
  "createdAt": "2024-01-01T00:00:00Z",
  "baseSelector": "body",
  "fields": [
    {
      "name": "Field Name",
      "selector": ".css-selector",
      "xpath": "//xpath/expression",
      "type": "text",
      "valueType": "text",
      "attribute": null
    }
  ]
}
```

**Scripts hỗ trợ:**
- `scrape_with_template.py`: Script CLI để cào với template
- `use_template_example.py`: Ví dụ sử dụng template

---

## ⚙️ Chức Năng Chi Tiết

### 1. Chọn Phần Tử (Element Selection)

**Cách hoạt động:**
1. User click "Bật chế độ chọn" trong side panel
2. Content script bật event listener cho click
3. User click vào phần tử trên trang
4. Content script:
   - Highlight phần tử (border màu xanh)
   - Tạo selector (CSS hoặc XPath)
   - Tìm label/description nếu có
   - Thêm vào danh sách fields

**Selector Generation Strategy:**
1. **ID/Data Attributes**: Ưu tiên cao nhất
2. **Itemprop Attribute**: Cho XPath chính xác
3. **Label Sibling**: Tìm label từ previous sibling
4. **Uncle Strategy**: Tìm label từ div.a4ep88f hoặc span không trong strong
5. **Container Class**: Sử dụng class của container
6. **Absolute XPath**: Fallback cuối cùng

### 2. Value Types

#### 2.1. `text` (Mặc định)
- Lấy text content của phần tử
- Loại bỏ HTML tags
- Trim whitespace

#### 2.2. `html`
- Lấy toàn bộ HTML của phần tử
- Giữ nguyên cấu trúc

#### 2.3. `src`
- Lấy URL từ attribute `src` hoặc `data-src`
- **Lazy Loading Support**: Ưu tiên `data-src` > `data-lazy-src` > `src`
- **Filter**: Loại bỏ SVG, placeholder, binary data
- **Multiple Images**: Sử dụng `type: 'list'` với nested fields

#### 2.4. `href`
- Lấy URL từ attribute `href`
- Thường dùng cho links

#### 2.5. `all` / `container`
- Lấy toàn bộ giá trị trong container
- Tìm tất cả `strong[@itemprop]` trong container
- Trả về dictionary với key là `itemprop` và value là text
- Format: `{"house_type": "Nhà mặt phố", "size": "110 m²", ...}`

#### 2.6. Các attribute khác
- `alt`, `title`, `data-id`, etc.
- Lấy giá trị từ attribute tương ứng

### 3. Scraping Modes

#### 3.1. JavaScript Scraping (Client-side)
- **Nút**: "🚀 Cào dữ liệu (JS)"
- **Cách hoạt động**: Extract trực tiếp từ DOM của trang hiện tại
- **Ưu điểm**: Nhanh, không cần server
- **Nhược điểm**: Chỉ hoạt động trên trang đã mở, không hỗ trợ JavaScript rendering

#### 3.2. Crawl4AI Scraping (Server-side)
- **Nút**: "🤖 Cào với Crawl4AI"
- **Cách hoạt động**: 
  1. Extension gửi request đến API server
  2. API server tạo schema cho Crawl4AI
  3. Crawl4AI crawl trang web với browser automation
  4. Trả về kết quả cho extension
- **Ưu điểm**: 
  - Hỗ trợ JavaScript rendering
  - Có thể crawl bất kỳ URL (không cần mở trang)
  - Xử lý lazy loading, dynamic content
- **Nhược điểm**: Cần API server chạy, chậm hơn JavaScript scraping

### 4. Image Processing

**Lazy Loading Support:**
- Schema lấy cả `data-src`, `data-lazy-src`, và `src`
- Ưu tiên: `data-src` > `data-lazy-src` > `src`
- Lý do: `data-src` thường chứa URL thật, `src` có thể là placeholder

**Filtering:**
- **SVG**: Loại bỏ `.svg`, `data:image/svg+xml`
- **Placeholder**: Loại bỏ `img_empty`, `placeholder`, `empty.jpg`, `no-image`, etc.
- **Binary Data**: Loại bỏ base64 images, JFIF, PNG binary
- **URL Only**: Chỉ giữ lại URLs (http, https, //, /)

### 5. Container Extraction (`valueType: 'all'`)

**Use Case**: Lấy nhiều giá trị từ một container, ví dụ:
```html
<div class="container">
  <strong itemprop="house_type">Nhà mặt phố</strong>
  <strong itemprop="size">110 m²</strong>
  <strong itemprop="rooms">6 phòng</strong>
</div>
```

**Cách hoạt động:**
1. Tìm container selector
2. Tìm tất cả `strong[@itemprop]` trong container
3. Extract text và `itemprop` attribute
4. Trả về dictionary: `{"house_type": "Nhà mặt phố", "size": "110 m²", ...}`

**Schema cho Crawl4AI:**
```json
{
  "name": "field_name",
  "selector": "//container//strong[@itemprop]",
  "type": "list",
  "fields": [
    {"name": "value", "type": "text"},
    {"name": "itemprop", "type": "attribute", "attribute": "itemprop"}
  ]
}
```

---

## 🔧 Cài Đặt và Cấu Hình

### 1. Cài Đặt Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies (chi tiết xem phần [Công Nghệ và Phiên Bản](#-công-nghệ-và-phiên-bản)):**
- `crawl4ai>=0.4.0` (hiện tại: 0.7.7): Web scraping framework
- `python-dotenv`: Environment variables
- `playwright` (hiện tại: 1.56.0): Browser automation (tự động cài với crawl4ai)
- `beautifulsoup4>=4.12.0` (hiện tại: 4.14.3): HTML parsing
- `streamlit>=1.28.0` (hiện tại: 1.52.1): Web dashboard framework
- `pandas>=2.0.0` (hiện tại: 2.3.3): Data processing
- `openpyxl>=3.1.0`: Excel file handling
- `nodriver`: Browser automation với anti-detection (hiện tại: 0.48.1)
- `mysql-connector-python` hoặc `pymysql`: MySQL database connection (hiện tại: 9.5.0)

### 2. Cài Đặt Extension

1. Mở Chrome/Edge: `chrome://extensions/` hoặc `edge://extensions/`
2. Bật "Developer mode"
3. Click "Load unpacked"
4. Chọn thư mục `extension/`

### 3. Tạo Icons (Tùy chọn)

```bash
cd extension
pip install Pillow
python generate_icons.py
```

Hoặc mở `create-icons.html` trong browser và download icons.

### 4. Chạy API Server

```bash
python extension_api_server.py
```

Server sẽ chạy tại `http://localhost:8765`

---

## 📖 Hướng Dẫn Sử Dụng

### 1. Cào Dữ Liệu Cơ Bản

1. **Mở trang web** cần cào
2. **Click icon extension** để mở side panel
3. **Click "Bật chế độ chọn"**
4. **Click vào các phần tử** muốn lấy dữ liệu
5. **Chọn value type** cho mỗi field (text, html, src, etc.)
6. **Click "🤖 Cào với Crawl4AI"** hoặc "🚀 Cào dữ liệu (JS)"
7. **Xem kết quả** trong side panel
8. **Click "💾 Export JSON"** để tải file

### 2. Sử Dụng Template

#### 2.1. Lưu Template
1. Chọn các fields như bình thường
2. Click "📋 Lưu Template"
3. File JSON sẽ được tải về

#### 2.2. Sử Dụng Template với Script

```bash
python scrape_with_template.py template.json https://example.com output.json
```

#### 2.3. Sử Dụng Template với Extension
1. Load template từ file (nếu có chức năng)
2. Hoặc sử dụng template đã lưu trong storage

### 3. Cào Images với Lazy Loading

1. Chọn phần tử `<img>` hoặc container chứa images
2. Chọn `valueType: "src"`
3. Extension sẽ tự động:
   - Lấy từ `data-src` nếu có (lazy loading)
   - Fallback về `src`
   - Filter placeholder và SVG

### 4. Lấy Container Values

1. Chọn container chứa nhiều `strong[@itemprop]`
2. Chọn `valueType: "all"` hoặc `"container"`
3. Kết quả sẽ là dictionary với key là `itemprop` và value là text

---

## 📡 API Documentation

### POST `/`

**Request Body:**
```json
{
  "action": "scrape_with_fields" | "scrape_with_template",
  "url": "https://example.com",
  "fields": [...],  // Cho scrape_with_fields
  "template": {...} // Cho scrape_with_template
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "field1": "value1",
    "field2": ["image1.jpg", "image2.jpg"],
    "field3": {"key1": "value1", "key2": "value2"}
  },
  "url": "https://example.com"
}
```

### Action: `scrape_with_fields`

**Request:**
```json
{
  "action": "scrape_with_fields",
  "url": "https://example.com",
  "fields": [
    {
      "name": "Title",
      "selector": ".title",
      "cssSelector": ".title",
      "xpath": "//div[@class='title']",
      "valueType": "text"
    },
    {
      "name": "Images",
      "selector": ".gallery img",
      "valueType": "src"
    }
  ]
}
```

### Action: `scrape_with_template`

**Request:**
```json
{
  "action": "scrape_with_template",
  "url": "https://example.com",
  "template": {
    "name": "My Template",
    "baseSelector": "body",
    "fields": [...]
  }
}
```

---

## 🔄 Xử Lý Dữ Liệu

### 1. Schema Conversion

Extension format → Crawl4AI schema:

**Text/HTML:**
```json
{
  "name": "field_name",
  "selector": ".selector",
  "type": "text" | "html"
}
```

**Attribute (src, href, etc.):**
```json
{
  "name": "field_name",
  "selector": ".selector img",
  "type": "list",
  "fields": [{
    "name": "url",
    "type": "attribute",
    "attribute": "src"
  }]
}
```

**Container (all):**
```json
{
  "name": "field_name",
  "selector": "//container//strong[@itemprop]",
  "type": "list",
  "fields": [
    {"name": "value", "type": "text"},
    {"name": "itemprop", "type": "attribute", "attribute": "itemprop"}
  ]
}
```

### 2. Data Post-Processing

**Images:**
- Extract từ dict: `data_src` > `data-lazy-src` > `src` > `url`
- Filter SVG, placeholder, binary
- Chỉ giữ URLs

**Container:**
- Convert list of dicts → dictionary
- Key: `itemprop`, Value: `value`

**Text/HTML:**
- Trim whitespace
- Handle None/empty values

---

## 🐛 Troubleshooting

### 1. Extension không hoạt động

**Kiểm tra:**
- Extension đã được load chưa? (`chrome://extensions/`)
- Content script có chạy không? (F12 → Console)
- Có lỗi trong background script không?

**Giải pháp:**
- Reload extension
- Reload trang web
- Kiểm tra console errors

### 2. API Server không kết nối

**Kiểm tra:**
- Server có đang chạy không? (`python extension_api_server.py`)
- Port 8765 có bị chặn không?
- CORS có được cấu hình đúng không?

**Giải pháp:**
- Restart server
- Kiểm tra firewall
- Kiểm tra log terminal

### 3. Selector không đúng

**Vấn đề:**
- Selector quá rộng (match nhiều elements)
- Selector không match element nào
- XPath không hoạt động với Crawl4AI

**Giải pháp:**
- Chọn lại phần tử
- Toggle giữa CSS và XPath (nhấn `X`)
- Chỉnh sửa selector thủ công trong code
- Sử dụng preview để kiểm tra

### 4. Images bị null hoặc placeholder

**Nguyên nhân:**
- Lazy loading: URL thật ở `data-src`, không phải `src`
- Placeholder images chưa được filter

**Giải pháp:**
- Đã được xử lý tự động:
  - Ưu tiên `data-src` > `src`
  - Filter placeholder images
- Nếu vẫn lỗi, kiểm tra log terminal để debug

### 5. Container extraction không hoạt động

**Kiểm tra:**
- `valueType` có phải `"all"` hoặc `"container"` không?
- Container có chứa `strong[@itemprop]` không?
- Selector có đúng container không?

**Giải pháp:**
- Chọn đúng container (div cha)
- Đảm bảo có `itemprop` attributes
- Preview value để kiểm tra

---

## 📝 Ghi Chú Kỹ Thuật

### 1. Selector Generation

**XPath Strategy:**
1. Tìm label từ sibling hoặc uncle
2. Sử dụng `itemprop` nếu có
3. Kết hợp với container class
4. Test selector để đảm bảo match đúng 1 element

**CSS Strategy:**
1. Ưu tiên ID, data attributes
2. Sử dụng class names
3. Kết hợp với parent selectors

### 2. Lazy Loading Images

**Schema:**
```json
{
  "fields": [
    {"name": "data_src", "type": "attribute", "attribute": "data-src"},
    {"name": "src", "type": "attribute", "attribute": "src"}
  ]
}
```

**Processing:**
```python
v = (v.get('data_src') or 
     v.get('data-lazy-src') or
     v.get('src'))
```

### 3. Debug Logging

API server có extensive logging:
- Request received
- Schema generation
- Crawl4AI response
- Data processing
- Final formatted data

Xem log trong terminal khi chạy server.

---

## 📚 Tài Liệu Tham Khảo

- **Crawl4AI**: https://github.com/unclecode/crawl4ai
- **Extension API**: Chrome Extension API documentation
- **XPath**: https://www.w3schools.com/xml/xpath_intro.asp
- **CSS Selectors**: https://www.w3schools.com/cssref/css_selectors.asp

---

## 🔄 Version History

- **v1.0.9**: Current version
  - Lazy loading support
  - Placeholder image filtering
  - Container extraction
  - Improved selector generation
  - Debug logging

---

## 👥 Đóng Góp

Để đóng góp hoặc báo lỗi, vui lòng:
1. Kiểm tra log terminal
2. Mô tả chi tiết vấn đề
3. Cung cấp URL và selector nếu có thể

---

**Tài liệu này được cập nhật lần cuối: 2025-12-11**




@'
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            "playwright_profile",
            headless=False,
            viewport={"width": 1366, "height": 768},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await ctx.new_page()
        await page.goto("https://batdongsan.com.vn/nha-dat-ban", wait_until="domcontentloaded", timeout=60000)
        print("Bấm human/captcha nếu có, rồi đợi (tối đa 120s). Cửa sổ sẽ tự đóng.")
        await page.wait_for_timeout(120000)
        await ctx.close()

asyncio.run(main())
'@ | python -