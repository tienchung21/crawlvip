# 📋 Hướng dẫn sử dụng Template đã lưu

## 🎯 Cách lưu Template

1. Mở extension và chọn các trường cần cào
2. Click nút **"📋 Lưu Template"**
3. File JSON sẽ được tải xuống (ví dụ: `crawl4ai_template_1733995200000.json`)

## 🚀 Cách sử dụng Template

### Cách 1: Dùng script Python (Đơn giản nhất)

```bash
python scrape_with_template.py <template_file> <url> [output_file]
```

**Ví dụ:**
```bash
# Cào 1 URL
python scrape_with_template.py crawl4ai_template_1733995200000.json https://batdongsan.com.vn/ban-nha-...

# Cào và lưu vào file cụ thể
python scrape_with_template.py template.json https://example.com output.json
```

### Cách 2: Dùng trong code Python

```python
import asyncio
from scrape_with_template import scrape_with_template

async def main():
    data = await scrape_with_template(
        template_path="crawl4ai_template_1733995200000.json",
        url="https://batdongsan.com.vn/ban-nha-...",
        output_file="result.json"
    )
    print(data)

asyncio.run(main())
```

### Cách 3: Cào nhiều URL cùng lúc

Tạo file `scrape_multiple.py`:

```python
import asyncio
from scrape_with_template import scrape_with_template

urls = [
    "https://batdongsan.com.vn/ban-nha-1",
    "https://batdongsan.com.vn/ban-nha-2",
    "https://batdongsan.com.vn/ban-nha-3",
]

async def main():
    template = "crawl4ai_template_1733995200000.json"
    
    for i, url in enumerate(urls, 1):
        print(f"\n{'='*60}")
        print(f"📄 Đang cào URL {i}/{len(urls)}: {url}")
        print(f"{'='*60}\n")
        
        output_file = f"output/result_{i}.json"
        await scrape_with_template(template, url, output_file)
        
        # Đợi một chút giữa các request
        await asyncio.sleep(2)

asyncio.run(main())
```

## 📁 Cấu trúc Template

Template JSON có cấu trúc:

```json
{
  "name": "Template_2025-12-12",
  "description": "Template được tạo từ extension tại https://...",
  "url": "https://batdongsan.com.vn/...",
  "createdAt": "2025-12-12T09:00:00.000Z",
  "baseSelector": "body",
  "fields": [
    {
      "name": "ten_tindang",
      "selector": ".re__pr-title",
      "type": "text",
      "selectorType": "css",
      "xpath": "//h1[contains(@class, 're__pr-title')]"
    },
    {
      "name": "khoanggia",
      "selector": "div#product-detail-web > div > div > span",
      "type": "html",
      "selectorType": "css"
    }
  ]
}
```

## 💡 Lưu ý

1. **Template có thể dùng cho nhiều URL**: Template lưu selector, có thể dùng để cào nhiều trang cùng cấu trúc
2. **URL có thể khác**: URL trong template chỉ là URL gốc, bạn có thể dùng template để cào URL khác
3. **Selector tự động đơn giản hóa**: Script sẽ tự động bỏ `nth-of-type` để tương thích với Crawl4AI
4. **Kết quả lưu vào thư mục `output/`**: Mặc định kết quả sẽ được lưu vào thư mục `output/`

## 🔧 Troubleshooting

### Template không tìm thấy field
- Kiểm tra selector có đúng không
- Thử preview value trong extension trước khi lưu template
- Có thể cần chỉnh selector trong template file

### Kết quả khác với extension
- Extension dùng JavaScript trên trang hiện tại
- Template dùng Crawl4AI (có thể khác một chút)
- Nếu cần chính xác 100%, dùng extension trực tiếp

### Cần chỉnh selector
- Mở file template JSON
- Tìm field cần chỉnh
- Sửa `selector` trong field đó
- Lưu và chạy lại

