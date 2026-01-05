# Hướng dẫn sử dụng Extension với Crawl4AI

## 🚀 Cài đặt và chạy

### Bước 1: Chạy API Server

Mở terminal và chạy:

```bash
cd C:\laragon\www\craw
python extension_api_server.py
```

Server sẽ chạy tại `http://localhost:8765`

### Bước 2: Sử dụng Extension

1. **Mở Extension**: Click vào icon extension để mở side panel
2. **Chọn các trường**: 
   - Click "Bật chế độ chọn"
   - Click vào các phần tử trên trang web để chọn
3. **Cào dữ liệu với Crawl4AI**:
   - Click nút "🤖 Cào với Crawl4AI"
   - Extension sẽ gửi request đến API server
   - Crawl4AI sẽ cào dữ liệu và trả về kết quả

## 📋 Tính năng

### 1. Cào với JavaScript (nhanh, từ trang hiện tại)
- Click nút "🚀 Cào dữ liệu (JS)"
- Cào dữ liệu trực tiếp từ DOM của trang hiện tại
- Nhanh nhưng chỉ hoạt động trên trang đã mở

### 2. Cào với Crawl4AI (mạnh mẽ, có thể cào bất kỳ URL)
- Click nút "🤖 Cào với Crawl4AI"
- Sử dụng Crawl4AI để cào dữ liệu
- Có thể cào bất kỳ URL nào (không cần mở trang)
- Hỗ trợ JavaScript rendering, lazy loading, etc.

### 3. Export JSON
- Click nút "💾 Export JSON"
- Xuất dữ liệu đã cào thành file JSON

### 4. Lưu Template
- Click nút "📋 Lưu Template"
- Lưu template để sử dụng với Crawl4AI sau này
- Template chứa CSS selector và XPath

## 🔧 Cấu trúc dữ liệu

### Request gửi đến API Server:
```json
{
  "action": "scrape_with_fields",
  "url": "https://example.com",
  "fields": [
    {
      "name": "Tiêu đề",
      "selector": ".title",
      "cssSelector": ".title",
      "valueType": "text"
    }
  ]
}
```

### Response từ API Server:
```json
{
  "success": true,
  "data": {
    "Tiêu đề": "Nội dung tiêu đề",
    "Giá": "1.5 tỷ"
  },
  "url": "https://example.com"
}
```

## ⚠️ Lưu ý

1. **API Server phải chạy**: Extension cần API server đang chạy để gọi Crawl4AI
2. **CSS Selector**: Crawl4AI chỉ hỗ trợ CSS selector, không hỗ trợ XPath
3. **Tên field**: Tên field trong extension sẽ được giữ nguyên trong kết quả
4. **Value Type**: 
   - `text`: Lấy text content
   - `html`: Lấy HTML
   - `src`, `href`, `alt`, `title`, `data-id`: Lấy attribute tương ứng

## 🐛 Troubleshooting

### Lỗi "Không thể kết nối đến API server"
- Kiểm tra API server có đang chạy không
- Kiểm tra port 8765 có bị chặn không
- Thử restart API server

### Lỗi "No valid CSS selectors found"
- Đảm bảo các field có CSS selector (không phải XPath)
- Kiểm tra selector có đúng không

### Kết quả rỗng
- Kiểm tra selector có đúng không
- Thử preview value trong extension trước
- Kiểm tra URL có đúng không

