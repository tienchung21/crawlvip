# Web Scraper Extension

Extension trình duyệt đơn giản để click chọn các trường và cào dữ liệu từ website bất kỳ.

## Tính năng

- ✅ Click vào phần tử trên trang để chọn trường cần lấy
- ✅ Highlight các phần tử đã chọn
- ✅ Tự động tạo selector cho mỗi phần tử
- ✅ Cào dữ liệu từ các trường đã chọn
- ✅ Export kết quả dưới dạng JSON

## Cài đặt

### Bước 1: Tạo icon (tùy chọn)

**Cách 1: Dùng Python script (khuyến nghị)**
```bash
cd extension
pip install Pillow
python generate_icons.py
```

**Cách 2: Dùng HTML**
1. Mở file `create-icons.html` trong trình duyệt
2. Click "Download Icons" để tải các file icon
3. Di chuyển các file icon vào thư mục `extension/icons/`

**Lưu ý:** Nếu không tạo icon, extension vẫn hoạt động nhưng sẽ hiển thị icon mặc định hoặc có thể báo lỗi khi load.

### Bước 2: Cài đặt extension
1. Mở Chrome/Edge và vào `chrome://extensions/` (hoặc `edge://extensions/`)
2. Bật "Developer mode" (góc trên bên phải)
3. Click "Load unpacked"
4. Chọn thư mục `extension` trong project này
5. Extension sẽ xuất hiện trong thanh công cụ

## Cách sử dụng

1. **Vào website cần cào dữ liệu**
2. **Click vào icon extension** trên thanh công cụ
3. **Click "Bật chế độ chọn"** để bật chế độ chọn phần tử
4. **Click vào các phần tử** trên trang mà bạn muốn lấy dữ liệu
   - Mỗi lần click sẽ highlight phần tử
   - Click lại lần nữa để bỏ chọn
5. **Xem danh sách trường đã chọn** trong popup extension
6. **Click "Cào dữ liệu"** để xem kết quả
7. **Click "Export JSON"** để tải file JSON chứa dữ liệu đã cào

## Cấu trúc dữ liệu export

```json
{
  "url": "https://example.com",
  "scrapedAt": "2024-01-01T00:00:00.000Z",
  "fields": [
    {
      "name": "Tên trường",
      "selector": ".class-name",
      "fullSelector": "div > .class-name",
      "tagName": "div"
    }
  ],
  "data": {
    "Tên trường": [
      {
        "text": "Nội dung text",
        "html": "<div>Nội dung HTML</div>",
        "selector": ".class-name",
        "href": "https://...",
        "src": "https://..."
      }
    ]
  }
}
```

## Lưu ý

- Extension hoạt động trên tất cả các website
- Selector được tạo tự động, có thể cần chỉnh sửa thủ công nếu cần
- Dữ liệu được lưu trong storage của extension
- Có thể xóa từng trường hoặc xóa tất cả

## Troubleshooting

- Nếu không click được phần tử: Thử tắt và bật lại chế độ chọn
- Nếu selector không đúng: Có thể chỉnh sửa selector trong code hoặc chọn lại phần tử
- Nếu extension không hoạt động: Kiểm tra console (F12) để xem lỗi

