# PHÂN TÍCH CHI TIẾT EXTENSION (THƯ MỤC extension/)

## 1. Danh sách file và vai trò

- **manifest.json**: File cấu hình chính của extension (Manifest V3). Khai báo quyền, scripts, icon, side panel, content scripts.
- **background.js**: Service worker, xử lý sự kiện cài đặt, khởi động, click vào icon extension để mở side panel.
- **content.js**: Content script chạy trên mọi trang web (trừ localhost/streamlit). Xử lý chọn phần tử, tạo selector, highlight, gửi dữ liệu về side panel.
- **content.css**: CSS cho content script, style highlight phần tử được chọn.
- **sidepanel.html**: Giao diện side panel (UI chính của extension). Chứa các nút, danh sách trường, tab chuyển chế độ, v.v.
- **sidepanel.js**: Logic cho side panel: quản lý trạng thái chọn trường, lưu template, export JSON, gọi API cào dữ liệu, đồng bộ với content script.
- **sidepanel.css**: CSS cho side panel.
- **popup.html**: Giao diện popup nhỏ khi click vào icon extension (không phải UI chính, chỉ để bật/tắt chọn trường nhanh).
- **popup.js**: Logic cho popup: bật/tắt chọn trường, hiển thị trạng thái, danh sách trường đã chọn.
- **styles.css**: CSS dùng chung cho popup và side panel.
- **generate_icons.py, create-icons.html, icons/**: Tạo và chứa icon cho extension.
- **README.md, README_CRAWL4AI.md, HUONG_DAN_SU_DUNG_TEMPLATE.md, RELOAD_INSTRUCTIONS.md**: Tài liệu hướng dẫn sử dụng, reload, template, v.v.

## 2. Chức năng từng file và luồng chạy

### manifest.json
- Khai báo quyền: activeTab, storage, scripting, tabs, sidePanel.
- Định nghĩa content_scripts (content.js, content.css), background (background.js), side_panel (sidepanel.html), icons.
- Chỉ định các file sẽ được Chrome/Edge load khi extension hoạt động.

### background.js
- Đăng ký sự kiện khi extension được cài đặt hoặc khởi động (log ra console).
- Khi user click vào icon extension, mở side panel (chrome.sidePanel.open).
- Là service worker, không giữ trạng thái lâu dài, chỉ xử lý sự kiện nền.

### content.js
- Inject vào mọi trang web (trừ localhost/streamlit).
- Lắng nghe sự kiện click để chọn phần tử, tạo selector (CSS/XPath), highlight phần tử.
- Hỗ trợ phím tắt (X) để chuyển đổi giữa CSS/XPath.
- Tìm label/description cho trường (sibling, parent, uncle strategy).
- Gửi thông tin trường đã chọn về side panel qua chrome.runtime/chrome.storage.
- Xử lý preview giá trị, loại selector, loại giá trị (text, html, src, href, all, ...).

### content.css
- Style cho phần tử được chọn (border, overlay, highlight).
- Đảm bảo overlay không che mất UI gốc của trang.

### sidepanel.html, sidepanel.js, sidepanel.css
- Giao diện chính của extension: hiển thị danh sách trường đã chọn, nút bật/tắt chọn trường, chọn loại selector, loại giá trị, export JSON, lưu template.
- sidepanel.js đồng bộ trạng thái với content script qua chrome.storage/chrome.runtime.
- Cho phép chuyển giữa chế độ detail/listing, tự động gợi ý selector cho các domain phổ biến.
- Gọi API server (extension_api_server.py) để cào dữ liệu với Crawl4AI, nhận kết quả và hiển thị.
- Lưu template ra file JSON, cho phép import lại template.

### popup.html, popup.js, styles.css
- Popup nhỏ khi click vào icon extension (không phải UI chính).
- Cho phép bật/tắt chọn trường nhanh, xem trạng thái, danh sách trường đã chọn.
- styles.css dùng chung cho popup và side panel.

### generate_icons.py, create-icons.html, icons/
- Tạo icon cho extension (dùng Python script hoặc HTML tool).
- icons/ chứa các file icon PNG với nhiều kích thước.

### README.md, README_CRAWL4AI.md, HUONG_DAN_SU_DUNG_TEMPLATE.md, RELOAD_INSTRUCTIONS.md
- Hướng dẫn sử dụng extension, cách tạo/lưu template, cách reload extension khi cập nhật, hướng dẫn cài đặt icon, v.v.

## 3. Luồng hoạt động tổng thể

1. User cài extension, Chrome đọc manifest.json, load các file cần thiết.
2. Khi user vào trang web, content.js và content.css được inject vào trang.
3. User click vào icon extension để mở side panel (background.js gọi chrome.sidePanel.open).
4. User bật chế độ chọn trường trên side panel (sidepanel.js), content.js bắt đầu lắng nghe click trên trang.
5. User click vào phần tử muốn lấy dữ liệu, content.js tạo selector, tìm label, gửi về side panel.
6. User có thể chỉnh sửa selector, loại giá trị, đặt tên trường, lưu template, export JSON.
7. Khi user click "Cào dữ liệu (JS)" hoặc "Cào với Crawl4AI", sidepanel.js gửi request tới API server (extension_api_server.py), nhận kết quả và hiển thị.
8. User có thể lưu template để dùng lại, hoặc export kết quả ra file JSON.

## 4. Các hàm chính và liên kết giữa các file

- **content.js**: handleKeyPress, findLabelSibling, extractText, tạo selector, highlight, gửi message về sidepanel.js.
- **sidepanel.js**: loadState, saveState, updateUI, applyListingDefaults, gọi API cào dữ liệu, lưu template, export JSON.
- **background.js**: onInstalled, onStartup, onClicked (mở side panel).
- **popup.js**: loadState, saveState, updateUI (tương tự sidepanel.js nhưng đơn giản hơn).

## 5. Tài liệu tham khảo
- Xem thêm README.md, README_CRAWL4AI.md, HUONG_DAN_SU_DUNG_TEMPLATE.md trong thư mục extension/ để biết chi tiết từng bước sử dụng, cấu trúc template, các tips khi dùng extension.

