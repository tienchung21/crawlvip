"""
Script đơn giản để cào dữ liệu sử dụng template đã lưu từ extension

Cách sử dụng:
    python scrape_with_template.py <template_file> <url> [output_file]

Ví dụ:
    python scrape_with_template.py crawl4ai_template_1234567890.json https://batdongsan.com.vn/... output.json
"""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from web_scraper import WebScraper


async def scrape_with_template(template_path: str, url: str, output_file: str = None):
    """
    Cào dữ liệu sử dụng template từ extension
    
    Args:
        template_path: Đường dẫn đến file template JSON
        url: URL cần cào
        output_file: File để lưu kết quả (nếu None sẽ tự động tạo tên)
    """
    # Kiểm tra file template
    if not Path(template_path).exists():
        print(f"❌ Không tìm thấy file template: {template_path}")
        return None
    
    # Đọc template
    print(f"📋 Đang đọc template: {template_path}")
    with open(template_path, 'r', encoding='utf-8') as f:
        template = json.load(f)
    
    print(f"📄 Template: {template.get('name', 'Unknown')}")
    print(f"📅 Tạo lúc: {template.get('createdAt', 'Unknown')}")
    print(f"🔢 Số trường: {len(template.get('fields', []))}")
    print(f"🌐 URL gốc: {template.get('url', 'Unknown')}")
    print(f"🎯 URL cần cào: {url}\n")
    
    # Tạo schema cho Crawl4AI
    schema = {
        "name": template.get("name", "ExtractedData"),
        "baseSelector": template.get("baseSelector") or "body",
        "fields": []
    }
    
    # Chuyển đổi fields từ template sang format Crawl4AI
    for field in template.get("fields", []):
        field_config = {
            "name": field["name"],
            "selector": field["selector"],
            "type": field.get("type", "text")
        }
        
        # Thêm attribute nếu có
        if field.get("attribute"):
            field_config["attribute"] = field["attribute"]
        
        schema["fields"].append(field_config)
    
    print("📊 Schema đã tạo:")
    print(json.dumps(schema, indent=2, ensure_ascii=False))
    print("\n" + "="*60 + "\n")
    
    # Cào dữ liệu với Crawl4AI
    print("🚀 Đang cào dữ liệu với Crawl4AI...")
    async with WebScraper(headless=True, verbose=False) as scraper:
        result = await scraper.scrape_with_schema(url, schema, bypass_cache=True)
        
        if result["success"]:
            print("✅ Cào thành công!\n")
            
            # Xử lý extracted_data
            extracted_data = result.get("extracted_data", {})
            if isinstance(extracted_data, list) and len(extracted_data) > 0:
                extracted_data = extracted_data[0]
            
            print("📊 Dữ liệu đã extract:")
            print(json.dumps(extracted_data, indent=2, ensure_ascii=False))
            
            # Lưu kết quả
            if output_file is None:
                # Tạo tên file tự động
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"output/scraped_{timestamp}.json"
            
            # Tạo thư mục output nếu chưa có
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Lưu file
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "url": url,
                    "template": template_path,
                    "template_name": template.get("name"),
                    "scraped_at": datetime.now().isoformat(),
                    "data": extracted_data
                }, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Đã lưu kết quả vào: {output_file}")
            return extracted_data
        else:
            print(f"❌ Lỗi: {result.get('error', 'Unknown error')}")
            return None


async def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("📖 Cách sử dụng:")
        print(f"   python {sys.argv[0]} <template_file> <url> [output_file]")
        print("\n💡 Ví dụ:")
        print(f"   python {sys.argv[0]} crawl4ai_template_1234567890.json https://batdongsan.com.vn/...")
        print(f"   python {sys.argv[0]} template.json https://example.com output.json")
        sys.exit(1)
    
    template_path = sys.argv[1]
    url = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    await scrape_with_template(template_path, url, output_file)


if __name__ == "__main__":
    asyncio.run(main())

