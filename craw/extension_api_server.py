"""
API Server cho Extension - Gọi Crawl4AI để cào dữ liệu
Version đơn giản - bỏ hết các kiểm tra ngoại lệ dài dòng
"""

import asyncio
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from web_scraper import WebScraper


class ExtensionAPIHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.end_headers()
    
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode('utf-8'))
        
        action = data.get('action', '')
        if action == 'scrape_with_template':
            result = asyncio.run(self.handle_scrape_with_template(data))
        elif action == 'scrape_with_fields':
            result = asyncio.run(self.handle_scrape_with_fields(data))
        else:
            result = {'success': False, 'error': f'Unknown action: {action}'}
        
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({
            'status': 'ok',
            'message': 'Extension API Server is running'
        }, ensure_ascii=False).encode('utf-8'))
    
    async def handle_scrape_with_template(self, data):
        print("\n" + "="*80)
        print("🚀 HANDLE_SCRAPE_WITH_TEMPLATE - REQUEST RECEIVED")
        print("="*80)
        print(f"📥 URL: {data.get('url')}")
        print(f"📋 Template name: {data.get('template', {}).get('name', 'N/A')}")
        print(f"📋 Template fields count: {len(data.get('template', {}).get('fields', []))}")
        print("\n📋 Template fields:")
        for i, field in enumerate(data.get('template', {}).get('fields', [])):
            print(f"  [{i}] {field.get('name', 'N/A')}:")
            print(f"      selector: {field.get('selector') or field.get('xpath') or field.get('cssSelector', 'N/A')}")
            print(f"      valueType: {field.get('valueType') or field.get('type', 'text')}")
            print(f"      textContent: {field.get('textContent', '')[:100] if field.get('textContent') else 'N/A'}")
        print("="*80 + "\n")
        
        url = data.get('url')
        template = data.get('template')
        
        base_selector = 'body'
        schema = {
            'name': template.get('name', 'ExtractedData'),
            'baseSelector': base_selector,
            'fields': []
        }
        
        for field in template.get('fields', []):
            raw_selector = field.get('xpath') or field.get('selector') or field.get('cssSelector')
            if not raw_selector:
                continue
            
            selector_type = 'xpath' if raw_selector.startswith('//') else 'css'
            value_type = field.get('valueType') or field.get('type', 'text')
            
            field_config = {
                'name': field['name'],
                'selector': raw_selector,
                'type': 'text'
            }
            
            if value_type in ['src', 'href', 'alt', 'title', 'data-id']:
                if value_type == 'src':
                    # Đảm bảo selector trỏ đến <img> tags
                    # Nếu selector chưa có "img", thêm " img" để tìm tất cả img tags bên trong
                    selector_lower = raw_selector.lower()
                    
                    img_selector = raw_selector
                    if 'img' not in selector_lower and not raw_selector.startswith('//'):
                        # CSS selector: thêm " img" để tìm tất cả img tags trong container
                        img_selector = f"{raw_selector} img"
                    elif raw_selector.startswith('//'):
                        # XPath: thêm "//img" để tìm tất cả img tags trong container
                        img_selector = f"{raw_selector}//img"
                    
                    # Crawl4AI không hỗ trợ multiple: true cho attribute
                    # Phải dùng type: "list" với nested field để lấy nhiều images
                    # Lấy cả data-src (lazy loading) và src (fallback)
                    field_config = {
                        'name': field['name'],
                        'selector': img_selector,
                        'type': 'list',  # Dùng list để lấy nhiều elements
                        'fields': [{
                            'name': 'data_src',
                            'type': 'attribute',
                            'attribute': 'data-src'
                        }, {
                            'name': 'src',
                            'type': 'attribute',
                            'attribute': 'src'
                        }, {
                            'name': 'data_lazy_src',
                            'type': 'attribute',
                            'attribute': 'data-lazy-src'
                        }]
                    }
                else:
                    # Các attribute khác (href, alt, title, data-id) - giữ nguyên
                    field_config['type'] = 'attribute'
                    field_config['attribute'] = value_type
            elif value_type == 'html':
                field_config['type'] = 'html'
            elif value_type == 'all' or value_type == 'container':
                # Lấy toàn bộ giá trị trong container
                if raw_selector.startswith('//'):
                    # XPath: tìm tất cả strong[@itemprop] trong container
                    container_selector = raw_selector
                    if 'strong' not in raw_selector.lower():
                        container_selector = f"{raw_selector}//strong[@itemprop]"
                    else:
                        if not raw_selector.endswith(']') or '@itemprop' not in raw_selector:
                            container_selector = f"{raw_selector}[@itemprop]"
                    
                    field_config = {
                        'name': field['name'],
                        'selector': container_selector,
                        'type': 'list',
                        'fields': [{
                            'name': 'value',
                            'type': 'text'
                        }, {
                            'name': 'itemprop',
                            'type': 'attribute',
                            'attribute': 'itemprop'
                        }]
                    }
                else:
                    # CSS selector: tìm tất cả strong[itemprop] trong container
                    container_selector = raw_selector
                    if 'strong' not in raw_selector.lower():
                        container_selector = f"{raw_selector} strong[itemprop]"
                    
                    field_config = {
                        'name': field['name'],
                        'selector': container_selector,
                        'type': 'list',
                        'fields': [{
                            'name': 'value',
                            'type': 'text'
                        }, {
                            'name': 'itemprop',
                            'type': 'attribute',
                            'attribute': 'itemprop'
                        }]
                    }
            
            schema['fields'].append(field_config)
        
        # Debug: Log schema được gửi cho Crawl4AI
        print("\n" + "="*80)
        print("📤 SCHEMA SENT TO CRAWL4AI (TEMPLATE):")
        print("="*80)
        print(json.dumps(schema, indent=2, ensure_ascii=False))
        print("="*80 + "\n")
        
        async with WebScraper(headless=True, verbose=False) as scraper:
            print("🔄 Calling Crawl4AI...")
            result = await scraper.scrape_with_schema(url, schema, bypass_cache=True)
            
            print("\n" + "="*80)
            print("📥 CRAWL4AI RESPONSE (TEMPLATE):")
            print("="*80)
            print(f"✅ Success: {result.get('success')}")
            if not result.get('success'):
                print(f"❌ Error: {result.get('error', 'Unknown error')}")
            print("="*80 + "\n")
            
            if result['success']:
                extracted_data = result.get('extracted_data', {})
                if isinstance(extracted_data, list) and len(extracted_data) > 0:
                    extracted_data = extracted_data[0]
                elif isinstance(extracted_data, list):
                    extracted_data = {}
                
                print("\n" + "="*80)
                print("📊 EXTRACTED DATA FROM CRAWL4AI (TEMPLATE):")
                print("="*80)
                print(json.dumps(extracted_data, indent=2, ensure_ascii=False)[:2000])
                if len(json.dumps(extracted_data, ensure_ascii=False)) > 2000:
                    print("... (truncated)")
                print("="*80 + "\n")
                
                formatted_data = {}
                if isinstance(extracted_data, dict):
                    for field in template.get('fields', []):
                        field_name = field.get('name', '')
                        text_content = field.get('textContent', '')
                        value_type = field.get('valueType') or field.get('type', 'text')
                        
                        # Check if textContent is binary data - nếu là binary thì bỏ qua
                        text_content_is_binary = False
                        if text_content:
                            text_lower = text_content.lower()
                            if 'jfif' in text_lower[:500] or 'png' in text_lower[:500] or '\xff\xd8' in text_content[:20] or len(text_content) > 5000:
                                text_content_is_binary = True
                                text_content = None  # Bỏ qua textContent nếu là binary
                        
                        if field_name in extracted_data:
                            value = extracted_data[field_name]
                            
                            # Xử lý trường hợp 'all' hoặc 'container' - lấy toàn bộ giá trị
                            if value_type == 'all' or value_type == 'container':
                                if isinstance(value, list):
                                    # Format thành dictionary với key là itemprop và value là text
                                    container_dict = {}
                                    for item in value:
                                        if isinstance(item, dict):
                                            itemprop = item.get('itemprop', '')
                                            text_value = item.get('value', '')
                                            if itemprop and text_value:
                                                container_dict[itemprop] = text_value.strip()
                                    if container_dict:
                                        formatted_data[field_name] = container_dict
                                        continue
                                # Nếu không phải list hoặc không có dữ liệu, thử dùng textContent
                                if text_content and not text_content_is_binary and text_content.strip():
                                    formatted_data[field_name] = text_content.strip()
                                    continue
                                formatted_data[field_name] = value if value else None
                                continue
                            
                            if value_type in ['src', 'href']:
                                # Filter out binary data (JFIF, PNG, etc.)
                                is_binary = False
                                
                                # Check if value is bytes
                                if isinstance(value, bytes):
                                    is_binary = True
                                    value = None
                                elif isinstance(value, str):
                                    # Check if it's binary data - multiple checks
                                    value_preview = value[:500].lower() if len(value) > 500 else value.lower()
                                    value_start = value[:20] if len(value) > 20 else value
                                    
                                    # Check for binary markers
                                    if ('jfif' in value_preview or 
                                        'png' in value_preview or
                                        '\xff\xd8' in value_start or
                                        value_start.startswith('\x89PNG') or
                                        '\x10JFIF' in value_start or
                                        len(value) > 5000 or
                                        (not value.startswith('http') and not value.startswith('//') and not value.startswith('/') and len(value) > 200)):
                                        is_binary = True
                                        value = None
                                
                                # If value is list, filter out binary data
                                if isinstance(value, list):
                                    print(f"  📋 DEBUG: value is list with {len(value)} items")
                                    if len(value) > 0:
                                        print(f"  📋 DEBUG: First item type: {type(value[0]).__name__}, value: {repr(value[0])[:200]}")
                                    filtered_values = []
                                    print(f"  📋 Processing list with {len(value)} items")
                                    for i, v in enumerate(value):
                                        original_v = v
                                        # Nếu v là dict (từ list type với nested fields), ưu tiên data-src cho lazy loading
                                        if isinstance(v, dict):
                                            original_dict = v
                                            # Ưu tiên: data-src > data-lazy-src > src > url > href > field_name
                                            # (data-src thường chứa URL thật, src có thể là placeholder)
                                            v = (v.get('data_src') or 
                                                 v.get('data-src') or
                                                 v.get('data_lazy_src') or 
                                                 v.get('data-lazy-src') or
                                                 v.get('src') or 
                                                 v.get('url') or
                                                 v.get('href') or 
                                                 v.get(field_name))
                                            # Nếu vẫn None, thử lấy giá trị đầu tiên trong dict
                                            if v is None and len(original_dict) > 0:
                                                # Lấy giá trị đầu tiên không phải None
                                                for key, val in original_dict.items():
                                                    if val is not None and isinstance(val, str):
                                                        v = val
                                                        break
                                            print(f"    [{i}]: dict -> extracted value: {repr(str(v)[:100]) if v else 'None'}")
                                        
                                        if v is None:
                                            print(f"    [{i}]: Skipping None value")
                                            continue
                                        
                                        if isinstance(v, bytes):
                                            print(f"    [{i}]: Skipping bytes")
                                            continue  # Skip bytes
                                        elif isinstance(v, str):
                                            v_preview = v[:500].lower() if len(v) > 500 else v.lower()
                                            v_start = v[:20] if len(v) > 20 else v
                                            
                                            # Skip binary data
                                            if ('jfif' in v_preview or 
                                                'png' in v_preview or
                                                '\xff\xd8' in v_start or
                                                v_start.startswith('\x89PNG') or
                                                '\x10JFIF' in v_start or
                                                len(v) > 5000):
                                                print(f"    [{i}]: Skipping binary data")
                                                continue
                                            # Skip SVG files
                                            v_lower = v.lower()
                                            if ('.svg' in v_lower or 
                                                v.startswith('data:image/svg+xml') or
                                                v.endswith('.svg')):
                                                print(f"    [{i}]: Skipping SVG: {v[:80]}")
                                                continue
                                            
                                            # Skip placeholder/empty images
                                            if ('img_empty' in v_lower or 
                                                '/user/assets/img/img_empty' in v_lower or
                                                'placeholder' in v_lower or
                                                'empty.jpg' in v_lower or
                                                'empty.png' in v_lower or
                                                'no-image' in v_lower or
                                                'noimage' in v_lower or
                                                'default-image' in v_lower):
                                                print(f"    [{i}]: ⚠️ Skipping placeholder image: {v[:80]}")
                                                continue
                                            
                                            # Only keep URLs
                                            if v.startswith('http') or v.startswith('//') or v.startswith('/'):
                                                filtered_values.append(v)
                                                print(f"    [{i}]: ✅ Added URL: {v[:80]}")
                                            else:
                                                print(f"    [{i}]: Skipping non-URL: {v[:80]}")
                                        else:
                                            print(f"    [{i}]: Skipping non-string value: {type(v).__name__}")
                                    
                                    print(f"  📊 Filtered {len(filtered_values)} valid URLs from {len(value)} items")
                                    if filtered_values:
                                        formatted_data[field_name] = filtered_values
                                        print(f"  ✅ Added {len(filtered_values)} URLs to result")
                                        continue
                                    else:
                                        print(f"  ⚠️  No valid URLs found, will try markdown fallback")
                                        value = None
                                        is_binary = True
                                
                                # If binary or no valid URL, try to extract from markdown
                                if is_binary or not value or (isinstance(value, str) and not value.startswith('http') and not value.startswith('//') and not value.startswith('/')):
                                    import re
                                    markdown = result.get('markdown', '')
                                    if markdown:
                                        img_pattern = r'!\[.*?\]\((https?://[^\s\)]+)'
                                        all_image_urls = re.findall(img_pattern, markdown)
                                        unique_images = list(dict.fromkeys(all_image_urls))
                                        # Filter out SVG files
                                        unique_images = [url for url in unique_images 
                                                        if '.svg' not in url.lower() 
                                                        and not url.lower().endswith('.svg')]
                                        if unique_images:
                                            formatted_data[field_name] = unique_images
                                            continue
                                
                                # Skip if still binary or invalid
                                if is_binary or not value:
                                    continue
                            
                            if text_content and text_content.strip():
                                text_normalized = text_content.strip().lower()
                                value_normalized = str(value).strip().lower() if value else ''
                                
                                if text_normalized != value_normalized:
                                    import re
                                    is_markdown = bool(re.search(r'\[([^\]]+)\]\([^\)]+\)', str(value))) or any(p in str(value) for p in ['](http', '- Ảnh', '!['])
                                    text_len = len(text_content.strip())
                                    value_len = len(str(value).strip()) if value else 0
                                    
                                    if is_markdown or (text_len > value_len * 2 and value_len < 100) or (text_len > 200 and value_len < 50):
                                        formatted_data[field_name] = text_content.strip()
                                        continue
                            
                            if not value or (isinstance(value, str) and not value.strip()):
                                if text_content and text_content.strip():
                                    formatted_data[field_name] = text_content.strip()
                                    continue
                            
                            # Final check: Don't add binary data even if it passed previous checks
                            if value_type in ['src', 'href']:
                                if isinstance(value, str):
                                    value_lower = value.lower()
                                    if 'jfif' in value_lower[:1000] or 'png' in value_lower[:1000] or len(value) > 5000:
                                        print(f"  ⚠️  Final check: BINARY DATA detected, skipping...")
                                        # Skip binary, use textContent or markdown fallback
                                        if text_content and text_content.strip():
                                            formatted_data[field_name] = text_content.strip()
                                            print(f"  ✅ Using textContent fallback: {text_content[:100]}")
                                        else:
                                            import re
                                            markdown = result.get('markdown', '')
                                            if markdown:
                                                img_pattern = r'!\[.*?\]\((https?://[^\s\)]+)'
                                                all_image_urls = re.findall(img_pattern, markdown)
                                                unique_images = list(dict.fromkeys(all_image_urls))
                                                # Filter out SVG files
                                                unique_images = [url for url in unique_images 
                                                                if '.svg' not in url.lower() 
                                                                and not url.lower().endswith('.svg')]
                                                if unique_images:
                                                    formatted_data[field_name] = unique_images
                                                    print(f"  ✅ Using markdown fallback: {len(unique_images)} URLs found")
                                        continue
                            
                            print(f"  ✅ Final value: {repr(str(value)[:200]) if value else 'None'}")
                            formatted_data[field_name] = value
                        else:
                            print(f"  ⚠️  Field '{field_name}' NOT FOUND in extracted_data")
                            # Chỉ dùng textContent nếu không phải binary data
                            # text_content_is_binary đã được định nghĩa ở trên trong cùng scope
                            if text_content and text_content.strip() and not text_content_is_binary:
                                formatted_data[field_name] = text_content.strip()
                            else:
                                formatted_data[field_name] = None
                
                # Debug: Log formatted_data cuối cùng
                print("\n" + "="*80)
                print("✅ FINAL FORMATTED DATA (TEMPLATE):")
                print("="*80)
                print(json.dumps(formatted_data, indent=2, ensure_ascii=False))
                print("="*80 + "\n")
                
                return {
                    'success': True,
                    'data': formatted_data,
                    'url': url
                }
            else:
                print(f"\n⚠️  DEBUG: Crawl4AI failed: {result.get('error', 'Unknown error')}\n")
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error')
                }
    
    async def handle_scrape_with_fields(self, data):
        print("\n" + "="*80)
        print("🚀 HANDLE_SCRAPE_WITH_FIELDS - REQUEST RECEIVED")
        print("="*80)
        print(f"📥 URL: {data.get('url')}")
        print(f"📋 Fields count: {len(data.get('fields', []))}")
        print("\n📋 Fields:")
        for i, field in enumerate(data.get('fields', [])):
            print(f"  [{i}] {field.get('name', 'N/A')}:")
            print(f"      selector: {field.get('customSelector') or field.get('selector') or field.get('cssSelector') or field.get('xpath', 'N/A')}")
            print(f"      valueType: {field.get('valueType', 'text')}")
            print(f"      textContent: {field.get('textContent', '')[:100] if field.get('textContent') else 'N/A'}")
        print("="*80 + "\n")
        
        url = data.get('url')
        fields = data.get('fields', [])
        
        if not url or not fields:
            return {'success': False, 'error': 'Missing url or fields'}
        
        schema = {
            'name': 'ExtensionScrapedData',
            'baseSelector': 'body',
            'fields': []
        }
        
        for field in fields:
            # Ưu tiên customSelector, sau đó selector, cuối cùng cssSelector
            raw_selector = field.get('customSelector') or field.get('selector') or field.get('cssSelector') or field.get('xpath')
            if not raw_selector:
                continue
            
            value_type = field.get('valueType', 'text')
            
            # Truyền thẳng selector cho Crawl4AI, không chuyển đổi
            field_config = {
                'name': field.get('name', ''),
                'selector': raw_selector,
                'type': 'text'
            }
            
            if value_type in ['src', 'href', 'alt', 'title', 'data-id']:
                if value_type == 'src':
                    # Đảm bảo selector trỏ đến <img> tags
                    # Nếu selector chưa có "img", thêm " img" để tìm tất cả img tags bên trong
                    selector_lower = raw_selector.lower()
                    
                    img_selector = raw_selector
                    if 'img' not in selector_lower and not raw_selector.startswith('//'):
                        # CSS selector: thêm " img" để tìm tất cả img tags trong container
                        img_selector = f"{raw_selector} img"
                    elif raw_selector.startswith('//'):
                        # XPath: thêm "//img" để tìm tất cả img tags trong container
                        img_selector = f"{raw_selector}//img"
                    
                    # Crawl4AI không hỗ trợ multiple: true cho attribute
                    # Phải dùng type: "list" với nested field để lấy nhiều images
                    # Lấy cả data-src (lazy loading) và src (fallback)
                    field_config = {
                        'name': field.get('name', ''),
                        'selector': img_selector,
                        'type': 'list',  # Dùng list để lấy nhiều elements
                        'fields': [{
                            'name': 'data_src',
                            'type': 'attribute',
                            'attribute': 'data-src'
                        }, {
                            'name': 'src',
                            'type': 'attribute',
                            'attribute': 'src'
                        }, {
                            'name': 'data_lazy_src',
                            'type': 'attribute',
                            'attribute': 'data-lazy-src'
                        }]
                    }
                else:
                    # Các attribute khác (href, alt, title, data-id) - giữ nguyên
                    field_config['type'] = 'attribute'
                    field_config['attribute'] = value_type
            elif value_type == 'html':
                field_config['type'] = 'html'
            elif value_type == 'all' or value_type == 'container':
                # Lấy toàn bộ giá trị trong container
                # Nếu là XPath, tìm tất cả strong[@itemprop] hoặc các element có text
                if raw_selector.startswith('//'):
                    # XPath: tìm tất cả strong[@itemprop] trong container
                    container_selector = raw_selector
                    if 'strong' not in raw_selector.lower():
                        # Nếu selector chưa có strong, thêm //strong[@itemprop] để lấy tất cả
                        container_selector = f"{raw_selector}//strong[@itemprop]"
                    else:
                        # Nếu đã có strong, đảm bảo lấy tất cả
                        if not raw_selector.endswith(']') or '@itemprop' not in raw_selector:
                            container_selector = f"{raw_selector}[@itemprop]"
                    
                    field_config = {
                        'name': field.get('name', ''),
                        'selector': container_selector,
                        'type': 'list',
                        'fields': [{
                            'name': 'value',
                            'type': 'text'
                        }, {
                            'name': 'itemprop',
                            'type': 'attribute',
                            'attribute': 'itemprop'
                        }]
                    }
                else:
                    # CSS selector: tìm tất cả strong[itemprop] trong container
                    container_selector = raw_selector
                    if 'strong' not in raw_selector.lower():
                        container_selector = f"{raw_selector} strong[itemprop]"
                    
                    field_config = {
                        'name': field.get('name', ''),
                        'selector': container_selector,
                        'type': 'list',
                        'fields': [{
                            'name': 'value',
                            'type': 'text'
                        }, {
                            'name': 'itemprop',
                            'type': 'attribute',
                            'attribute': 'itemprop'
                        }]
                    }
            
            schema['fields'].append(field_config)
        
        # Debug: Log schema được gửi cho Crawl4AI
        print("\n" + "="*80)
        print("📤 SCHEMA SENT TO CRAWL4AI (FIELDS):")
        print("="*80)
        print(json.dumps(schema, indent=2, ensure_ascii=False))
        print("="*80 + "\n")
        
        async with WebScraper(headless=True, verbose=False) as scraper:
            print("🔄 Calling Crawl4AI...")
            result = await scraper.scrape_with_schema(url, schema, bypass_cache=True)
            
            print("\n" + "="*80)
            print("📥 CRAWL4AI RESPONSE (FIELDS):")
            print("="*80)
            print(f"✅ Success: {result.get('success')}")
            if not result.get('success'):
                print(f"❌ Error: {result.get('error', 'Unknown error')}")
            print("="*80 + "\n")
            
            if result['success']:
                extracted_data = result.get('extracted_data', {})
                if isinstance(extracted_data, list) and len(extracted_data) > 0:
                    extracted_data = extracted_data[0]
                elif isinstance(extracted_data, list):
                    extracted_data = {}
                
                print("\n" + "="*80)
                print("📊 EXTRACTED DATA FROM CRAWL4AI (FIELDS):")
                print("="*80)
                print(f"Type: {type(extracted_data)}")
                if isinstance(extracted_data, dict):
                    print(f"Keys: {list(extracted_data.keys())}")
                    for key, val in extracted_data.items():
                        val_type = type(val).__name__
                        if isinstance(val, str):
                            val_len = len(val)
                            val_preview = repr(val[:200]) if val_len > 200 else repr(val)
                            print(f"  {key}:")
                            print(f"    Type: {val_type}")
                            print(f"    Length: {val_len}")
                            print(f"    Preview: {val_preview}")
                            # Check for binary markers
                            if 'jfif' in val.lower()[:500] or 'png' in val.lower()[:500] or '\xff\xd8' in val[:20]:
                                print(f"    ⚠️  BINARY DATA DETECTED!")
                        elif isinstance(val, list):
                            print(f"  {key}:")
                            print(f"    Type: {val_type} (length: {len(val)})")
                            for i, item in enumerate(val[:3]):  # Show first 3 items
                                if isinstance(item, str):
                                    print(f"      [{i}]: type=str, len={len(item)}, preview={repr(item[:100])}")
                                else:
                                    print(f"      [{i}]: type={type(item).__name__}, value={repr(item)[:100]}")
                        else:
                            print(f"  {key}: type={val_type}, value={repr(val)[:200]}")
                else:
                    print(f"Value: {repr(extracted_data)[:500]}")
                print("="*80 + "\n")
                
                formatted_data = {}
                if isinstance(extracted_data, dict):
                    for field in fields:
                        field_name = field.get('name', '')
                        text_content = field.get('textContent', '')
                        value_type = field.get('valueType', 'text')
                        
                        # Debug: Log từng field
                        print(f"DEBUG Field '{field_name}':")
                        print(f"  valueType: {value_type}")
                        
                        # Check if textContent is binary data - nếu là binary thì bỏ qua
                        text_content_is_binary = False
                        if text_content:
                            text_lower = text_content.lower()
                            if 'jfif' in text_lower[:500] or 'png' in text_lower[:500] or '\xff\xd8' in text_content[:20] or len(text_content) > 5000:
                                text_content_is_binary = True
                                print(f"  ⚠️  textContent is BINARY DATA, will ignore it")
                                text_content = None  # Bỏ qua textContent nếu là binary
                            else:
                                print(f"  textContent: {text_content[:100] if text_content else 'None'}")
                        else:
                            print(f"  textContent: None")
                        
                        if field_name in extracted_data:
                            value = extracted_data[field_name]
                            print(f"  Raw value from Crawl4AI: type={type(value).__name__}, len={len(str(value)) if value else 0}")
                            if isinstance(value, str) and len(value) > 0:
                                print(f"  Raw value preview: {repr(value[:200])}")
                            elif isinstance(value, list):
                                print(f"  Raw value is LIST with {len(value)} items")
                                for i, item in enumerate(value[:3]):
                                    print(f"    [{i}]: {repr(item)[:100]}")
                            
                            # Xử lý trường hợp 'all' hoặc 'container' - lấy toàn bộ giá trị
                            if value_type == 'all' or value_type == 'container':
                                if isinstance(value, list):
                                    # Format thành dictionary với key là itemprop và value là text
                                    container_dict = {}
                                    for item in value:
                                        if isinstance(item, dict):
                                            itemprop = item.get('itemprop', '')
                                            text_value = item.get('value', '')
                                            if itemprop and text_value:
                                                container_dict[itemprop] = text_value.strip()
                                    if container_dict:
                                        formatted_data[field_name] = container_dict
                                        print(f"  ✅ Extracted {len(container_dict)} items from container")
                                        continue
                                # Nếu không phải list hoặc không có dữ liệu, thử dùng textContent
                                if text_content and not text_content_is_binary and text_content.strip():
                                    formatted_data[field_name] = text_content.strip()
                                    continue
                                formatted_data[field_name] = value if value else None
                                continue
                            
                            if value_type in ['src', 'href']:
                                # Filter out binary data (JFIF, PNG, etc.)
                                is_binary = False
                                
                                # Check if value is bytes
                                if isinstance(value, bytes):
                                    is_binary = True
                                    value = None
                                elif isinstance(value, str):
                                    # Check if it's binary data - multiple checks
                                    value_preview = value[:500].lower() if len(value) > 500 else value.lower()
                                    value_start = value[:20] if len(value) > 20 else value
                                    
                                    # Check for binary markers
                                    if ('jfif' in value_preview or 
                                        'png' in value_preview or
                                        '\xff\xd8' in value_start or
                                        value_start.startswith('\x89PNG') or
                                        '\x10JFIF' in value_start or
                                        len(value) > 5000 or
                                        (not value.startswith('http') and not value.startswith('//') and not value.startswith('/') and len(value) > 200)):
                                        is_binary = True
                                        value = None
                                
                                # If value is list, filter out binary data
                                if isinstance(value, list):
                                    print(f"  📋 DEBUG: value is list with {len(value)} items")
                                    if len(value) > 0:
                                        print(f"  📋 DEBUG: First item type: {type(value[0]).__name__}, value: {repr(value[0])[:200]}")
                                    filtered_values = []
                                    for v in value:
                                        original_v = v
                                        # Nếu v là dict (từ list type với nested fields), ưu tiên data-src cho lazy loading
                                        if isinstance(v, dict):
                                            original_dict = v
                                            # Ưu tiên: data-src > data-lazy-src > src > url > href > field_name
                                            # (data-src thường chứa URL thật, src có thể là placeholder)
                                            v = (v.get('data_src') or 
                                                 v.get('data-src') or
                                                 v.get('data_lazy_src') or 
                                                 v.get('data-lazy-src') or
                                                 v.get('src') or 
                                                 v.get('url') or
                                                 v.get('href') or 
                                                 v.get(field_name))
                                            # Nếu vẫn None, thử lấy giá trị đầu tiên trong dict
                                            if v is None and len(original_dict) > 0:
                                                # Lấy giá trị đầu tiên không phải None
                                                for key, val in original_dict.items():
                                                    if val is not None and isinstance(val, str):
                                                        v = val
                                                        break
                                            print(f"    Dict -> extracted: {repr(str(v)[:100]) if v else 'None'}")
                                        
                                        if isinstance(v, bytes):
                                            continue  # Skip bytes
                                        elif isinstance(v, str):
                                            v_preview = v[:500].lower() if len(v) > 500 else v.lower()
                                            v_start = v[:20] if len(v) > 20 else v
                                            
                                            # Skip binary data
                                            if ('jfif' in v_preview or 
                                                'png' in v_preview or
                                                '\xff\xd8' in v_start or
                                                v_start.startswith('\x89PNG') or
                                                '\x10JFIF' in v_start or
                                                len(v) > 5000):
                                                continue
                                            # Skip SVG files
                                            v_lower = v.lower()
                                            if ('.svg' in v_lower or 
                                                v.startswith('data:image/svg+xml') or
                                                v.endswith('.svg')):
                                                continue
                                            
                                            # Skip placeholder/empty images
                                            if ('img_empty' in v_lower or 
                                                '/user/assets/img/img_empty' in v_lower or
                                                'placeholder' in v_lower or
                                                'empty.jpg' in v_lower or
                                                'empty.png' in v_lower or
                                                'no-image' in v_lower or
                                                'noimage' in v_lower or
                                                'default-image' in v_lower):
                                                print(f"    ⚠️ Skipping placeholder image: {v[:80]}")
                                                continue
                                            
                                            # Only keep URLs
                                            if v.startswith('http') or v.startswith('//') or v.startswith('/'):
                                                filtered_values.append(v)
                                                print(f"    ✅ Added valid URL: {v[:80]}")
                                            else:
                                                print(f"    ⚠️ Skipping non-URL: {v[:80]}")
                                    if filtered_values:
                                        formatted_data[field_name] = filtered_values
                                        print(f"  ✅ Extracted {len(filtered_values)} URLs from list")
                                        continue
                                    else:
                                        value = None
                                        is_binary = True
                                
                                # If binary or no valid URL, try to extract from markdown
                                if is_binary or not value or (isinstance(value, str) and not value.startswith('http') and not value.startswith('//') and not value.startswith('/')):
                                    import re
                                    markdown = result.get('markdown', '')
                                    if markdown:
                                        img_pattern = r'!\[.*?\]\((https?://[^\s\)]+)'
                                        all_image_urls = re.findall(img_pattern, markdown)
                                        unique_images = list(dict.fromkeys(all_image_urls))
                                        # Filter out SVG files
                                        unique_images = [url for url in unique_images 
                                                        if '.svg' not in url.lower() 
                                                        and not url.lower().endswith('.svg')]
                                        if unique_images:
                                            formatted_data[field_name] = unique_images
                                            continue
                                
                                # Skip if still binary or invalid
                                if is_binary or not value:
                                    continue
                            
                            if text_content and text_content.strip():
                                text_normalized = text_content.strip().lower()
                                value_normalized = str(value).strip().lower() if value else ''
                                
                                if text_normalized != value_normalized:
                                    import re
                                    is_markdown = bool(re.search(r'\[([^\]]+)\]\([^\)]+\)', str(value))) or any(p in str(value) for p in ['](http', '- Ảnh', '!['])
                                    text_len = len(text_content.strip())
                                    value_len = len(str(value).strip()) if value else 0
                                    
                                    if is_markdown or (text_len > value_len * 2 and value_len < 100) or (text_len > 200 and value_len < 50):
                                        formatted_data[field_name] = text_content.strip()
                                        continue
                            
                            if not value or (isinstance(value, str) and not value.strip()):
                                # Chỉ dùng textContent nếu không phải binary data
                                if text_content and not text_content_is_binary and text_content.strip():
                                    formatted_data[field_name] = text_content.strip()
                                    continue
                            
                            # Final check: Don't add binary data even if it passed previous checks
                            if isinstance(value, str) and value_type in ['src', 'href']:
                                value_lower = value.lower()
                                if 'jfif' in value_lower[:1000] or 'png' in value_lower[:1000] or len(value) > 5000:
                                    # Skip binary, use textContent or markdown fallback
                                    if text_content and not text_content_is_binary and text_content.strip():
                                        formatted_data[field_name] = text_content.strip()
                                    else:
                                        import re
                                        markdown = result.get('markdown', '')
                                        if markdown:
                                            img_pattern = r'!\[.*?\]\((https?://[^\s\)]+)'
                                            all_image_urls = re.findall(img_pattern, markdown)
                                            unique_images = list(dict.fromkeys(all_image_urls))
                                            # Filter out SVG files
                                            unique_images = [url for url in unique_images 
                                                            if '.svg' not in url.lower() 
                                                            and not url.lower().endswith('.svg')]
                                            if unique_images:
                                                formatted_data[field_name] = unique_images
                                    continue
                            
                            formatted_data[field_name] = value
                        else:
                            # Chỉ dùng textContent nếu không phải binary data
                            if text_content and not text_content_is_binary and text_content.strip():
                                formatted_data[field_name] = text_content.strip()
                            else:
                                formatted_data[field_name] = None
                
                # Debug: Log formatted_data cuối cùng
                print("\n" + "="*80)
                print("✅ FINAL FORMATTED DATA (FIELDS):")
                print("="*80)
                print(json.dumps(formatted_data, indent=2, ensure_ascii=False))
                print("="*80 + "\n")
                
                return {
                    'success': True,
                    'data': formatted_data,
                    'url': url
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error')
                }


def run_server(port=8765):
    server_address = ('localhost', port)
    httpd = HTTPServer(server_address, ExtensionAPIHandler)
    print(f"🚀 Extension API Server đang chạy tại http://localhost:{port}")
    print("📋 Sẵn sàng nhận requests từ extension...")
    print("💡 Nhấn Ctrl+C để dừng server\n")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n🛑 Đang dừng server...")
        httpd.shutdown()
        print("✅ Server đã dừng")


if __name__ == '__main__':
    import sys
    
    os.environ['HOME'] = str(Path(__file__).parent)
    os.environ['USERPROFILE'] = str(Path(__file__).parent)
    os.environ['CRAWL4_AI_BASE_DIRECTORY'] = str(Path(__file__).parent / '.crawl4ai')
    
    crawl4ai_dir = Path(__file__).parent / '.crawl4ai'
    crawl4ai_dir.mkdir(exist_ok=True)
    
    port = 8765
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print(f"⚠️  Port không hợp lệ, dùng port mặc định: {port}")
    
    run_server(port)
