# Hướng dẫn Reload Extension

## Cách 1: Xóa và cài lại (Khuyến nghị)

1. Vào `chrome://extensions/` (hoặc `edge://extensions/`)
2. Tìm extension "Web Scraper - Click to Extract"
3. Click nút **"Xóa"** (Delete) để xóa extension
4. Click **"Load unpacked"** lại
5. Chọn thư mục `extension` trong project
6. Extension sẽ được cài lại với code mới

## Cách 2: Hard Reload

1. Vào `chrome://extensions/`
2. Tìm extension "Web Scraper"
3. Click nút **Reload** (mũi tên tròn)
4. **Đóng tất cả popup extension** nếu đang mở
5. **Mở lại popup** để test

## Cách 3: Clear Cache và Reload

1. Vào `chrome://extensions/`
2. Bật "Developer mode"
3. Click "Service Worker" hoặc "Inspect views: popup" để mở DevTools
4. Trong DevTools, click chuột phải vào nút Reload
5. Chọn **"Empty Cache and Hard Reload"**
6. Đóng DevTools và test lại

## Kiểm tra code đã update chưa

1. Vào `chrome://extensions/`
2. Tìm extension "Web Scraper"
3. Click "Inspect views: popup" để mở DevTools
4. Trong tab Sources, tìm file `popup.js`
5. Kiểm tra dòng 192 - phải có `async function exportJSON()`
6. Nếu vẫn thấy `function exportJSON()` (không có async) → extension chưa reload

