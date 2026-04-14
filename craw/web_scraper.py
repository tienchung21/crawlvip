"""
Tool cào dữ liệu từ trang web sử dụng Crawl4AI
Hỗ trợ nhiều tính năng: cào đơn giản, extract theo schema, extract với LLM
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime

# Force UTF-8 to avoid Windows console encoding errors (rich logger)
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
# Patch: Bypass proxy for localhost
os.environ['no_proxy'] = '127.0.0.1,localhost'

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="ignore")
        except Exception:
            pass

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from crawl4ai import JsonCssExtractionStrategy, LLMExtractionStrategy, RegexExtractionStrategy
from crawl4ai import LLMConfig
from crawl4ai import PruningContentFilter, BM25ContentFilter


class WebScraper:
    """Tool cào dữ liệu từ trang web"""
    
    # Counter để tạo unique ID cho mỗi instance
    _instance_counter = 0
    
    # Sửa trong file web_scraper.py
    def __init__(
        self,
        headless: bool = True,
        verbose: bool = False,
        keep_open: bool = False,
        user_data_dir: str = None,
        use_managed_browser: bool = True,
    ):
        """
        Khởi tạo WebScraper
        Args:
            user_data_dir: Đường dẫn thư mục lưu Profile (Cookie, Cache...)
        """
        # Tạo unique instance ID để đảm bảo không bị share browser
        WebScraper._instance_counter += 1
        self._instance_id = WebScraper._instance_counter
        import time
        self._unique_id = f"{self._instance_id}_{int(time.time() * 1000)}"
        print(f"[WebScraper] Creating instance #{self._instance_id} with unique_id={self._unique_id}")
        
        self.keep_open = keep_open

        stealth_args = [
            "--disable-blink-features=AutomationControlled", # Quan trọng nhất: Tắt dấu hiệu Robot
            "--no-first-run",
            "--disable-infobars",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--ignore-certificate-errors",
        ]
        extra_args = list(stealth_args)
        if not headless:
            extra_args.extend([
                "--start-maximized",
                "--window-position=0,0",
                "--disable-features=CalculateNativeWinOcclusion",
                "--disable-backgrounding-occluded-windows",
                "--disable-session-crashed-bubble",
            ])
        
        # Thêm unique window name để tránh browser bị share
        extra_args.append(f"--class=crawl4ai-instance-{self._unique_id}")
        
        # Tự động phát hiện OS để set User-Agent phù hợp tránh lệch pha Fingerprint
        if sys.platform == "linux" or sys.platform == "linux2":
            ua_xin = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        else:
            ua_xin = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        
        # Xác định có dùng persistent context hay không
        # Chỉ bật persistent context khi có user_data_dir
        use_persistent = bool(user_data_dir)
        
        # Tính debugging port unique dựa trên instance ID để tránh conflict
        # Base port 9222, mỗi instance +1
        unique_debug_port = 9222 + (self._instance_id % 100)
        
        self.browser_config = BrowserConfig(
            headless=headless,
            verbose=verbose,
            browser_mode="dedicated",  # Dùng dedicated browser, không chia sẻ
            debugging_port=unique_debug_port,  # Unique port cho mỗi instance
            viewport_width=1920,
            viewport_height=1040,
            user_data_dir=user_data_dir,
            use_managed_browser=use_managed_browser,
            use_persistent_context=use_persistent,  # Chỉ bật khi có user_data_dir
            extra_args=extra_args,
            user_agent=ua_xin
        )
        print(f"[WebScraper] Instance #{self._instance_id}: debug_port={unique_debug_port}, user_data_dir={user_data_dir}")
        self.crawler = None
    
    async def __aenter__(self):
        """Context manager entry with retry for CDP connection"""
        import asyncio
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(1, max_retries + 1):
            try:
                self.crawler = AsyncWebCrawler(config=self.browser_config)
                await self.crawler.__aenter__()
                print(f"[WebScraper #{self._instance_id}] Crawler initialized successfully on attempt {attempt}")
                return self
            except Exception as e:
                error_msg = str(e)
                if "CDP endpoint" in error_msg or "not ready" in error_msg.lower():
                    print(f"[WebScraper #{self._instance_id}] CDP connection failed (attempt {attempt}/{max_retries}): {error_msg}")
                    if attempt < max_retries:
                        print(f"[WebScraper #{self._instance_id}] Waiting {retry_delay}s before retry...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print(f"[WebScraper #{self._instance_id}] All {max_retries} attempts failed, raising error")
                        raise
                else:
                    # Lỗi khác không phải CDP, raise ngay
                    raise
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        # Giữ browser mở nếu được yêu cầu (dùng cho quan sát thủ công)
        if self.crawler and not self.keep_open:
            await self.crawler.__aexit__(exc_type, exc_val, exc_tb)
    
    async def close(self):
        """Đóng browser thủ công khi keep_open=True"""
        if self.crawler:
            await self.crawler.__aexit__(None, None, None)

    async def get_active_page(self):
        """
        Lấy page đang active từ Crawl4AI's browser context.
        Dùng cho việc hiển thị và cào trực tiếp.
        QUAN TRỌNG: Không tạo page mới khi context đã đóng - trả về None để caller xử lý.
        """
        try:
            ctx = getattr(self.crawler, "crawler_strategy", None)
            if not ctx:
                print(f"[WebScraper #{self._instance_id}] get_active_page: No crawler_strategy")
                return None
            bm = getattr(ctx, "browser_manager", None)
            if not bm:
                print(f"[WebScraper #{self._instance_id}] get_active_page: No browser_manager")
                return None
            
            # Debug: Log browser manager và context để xác định có bị share không
            bm_id = id(bm)
            crawler_id = id(self.crawler)
            user_data = getattr(self.browser_config, 'user_data_dir', 'N/A')
            print(f"[WebScraper #{self._instance_id}] get_active_page: crawler_id={crawler_id}, browser_manager_id={bm_id}, user_data_dir={user_data}")
            
            context = getattr(bm, "default_context", None)
            if not context:
                print(f"[WebScraper #{self._instance_id}] get_active_page: No default_context")
                return None
            
            ctx_id = id(context)
            print(f"[WebScraper #{self._instance_id}] get_active_page: context_id={ctx_id}")
            
            # Kiểm tra context còn sống không trước khi truy cập pages
            try:
                # Thử truy cập thuộc tính của context để kiểm tra còn sống
                if hasattr(context, '_closed') and context._closed:
                    print("[WebScraper] Context is closed, returning None")
                    return None
            except Exception:
                pass
            
            pages = []
            try:
                pages = list(context.pages) if hasattr(context, 'pages') else []
            except Exception as e:
                # Context có thể đã bị đóng
                print(f"[WebScraper] Cannot get pages from context: {e}")
                return None
            
            page = None
            if pages:
                # Trả về page cuối cùng (thường là page đang active)
                # Kiểm tra page còn sống không
                for p in reversed(pages):
                    try:
                        # Kiểm tra page còn sống bằng cách truy cập url
                        _ = p.url
                        page = p
                        break
                    except Exception:
                        continue
                        
            # KHÔNG tạo page mới nếu không có page nào - để caller quyết định
            # Việc tạo page mới khi context đóng sẽ gây lỗi "Target page, context or browser has been closed"
            if not page and pages:
                # Có pages nhưng tất cả đều đã đóng
                print("[WebScraper] All pages are closed")
                return None
            elif not page and not pages:
                # Không có page nào, thử tạo mới NẾU context còn sống
                try:
                    page = await context.new_page()
                except Exception as e:
                    print(f"[WebScraper] Cannot create new page: {e}")
                    return None
            
            # Set viewport to match window size (full screen)
            if page:
                try:
                    # Lấy kích thước màn hình thực tế và set viewport
                    await page.set_viewport_size({"width": 1920, "height": 1040})
                except Exception as e:
                    print(f"[WebScraper] set_viewport_size error: {e}")
            
            return page
        except Exception as e:
            print(f"[WebScraper] get_active_page error: {e}")
            return None

    async def navigate_and_get_html(self, url: str, wait_networkidle: bool = True, timeout: int = 30000) -> dict:
        """
        Navigate page hiển thị đến URL và lấy HTML.
        Dùng display_page nếu có, không thì lấy active page từ Crawl4AI.
        """
        page = getattr(self, 'display_page', None)
        if not page:
            page = await self.get_active_page()
        if not page:
            return {"success": False, "error": "No page available", "html": ""}
        
        try:
            print(f"[WebScraper] Navigating to: {url[:100]}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            if wait_networkidle:
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass
            try:
                await page.bring_to_front()
            except Exception:
                pass
            html = await page.content()
            current_url = page.url or url
            print(f"[WebScraper] Got HTML, length={len(html)}, url={current_url[:80]}")
            self.display_page = page
            return {"success": True, "html": html, "url": current_url, "page": page}
        except Exception as e:
            print(f"[WebScraper] navigate_and_get_html error: {e}")
            return {"success": False, "error": str(e), "html": ""}
    
    async def scrape_simple(self, url: str, bypass_cache: bool = False) -> Dict[str, Any]:
        """
        Cào dữ liệu đơn giản - lấy markdown và HTML
        
        Args:
            url: URL cần cào
            bypass_cache: Bỏ qua cache
            
        Returns:
            Dict chứa markdown, HTML và metadata
        """
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS if bypass_cache else CacheMode.ENABLED
        )
        
        result = await self.crawler.arun(url=url, config=config)
        
        return {
            "success": result.success,
            "url": url,
            "markdown": result.markdown.raw_markdown if result.success else None,
            "html": result.html if result.success else None,  # <--- Sửa ở đây: Lấy HTML gốc
            "title": result.metadata.get("title", "") if result.success else "",
            "description": result.metadata.get("description", "") if result.success else "",
            "error": result.error_message if not result.success else None,
            "timestamp": datetime.now().isoformat()
        }
    
    async def scrape_with_js(self, url: str, js_code: List[str], bypass_cache: bool = False) -> Dict[str, Any]:
        """
        Cào dữ liệu với JavaScript code để thao tác trang
        
        Args:
            url: URL cần cào
            js_code: List các dòng JavaScript code
            bypass_cache: Bỏ qua cache
            
        Returns:
            Dict chứa markdown, HTML và metadata
        """
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS if bypass_cache else CacheMode.ENABLED,
            js_code=js_code
        )
        
        result = await self.crawler.arun(url=url, config=config)
        
        return {
            "success": result.success,
            "url": url,
            "markdown": result.markdown.raw_markdown if result.success else None,
            "html": result.html if result.success else None,  # <--- Sửa ở đây: Lấy HTML gốc
            "title": result.metadata.get("title", "") if result.success else "",
            "description": result.metadata.get("description", "") if result.success else "",
            "error": result.error_message if not result.success else None,
            "timestamp": datetime.now().isoformat()
        }
    
    async def scrape_with_schema(self, url: str, schema: Dict, bypass_cache: bool = False) -> Dict[str, Any]:
        """
        Cào dữ liệu với schema CSS selector
        
        Args:
            url: URL cần cào
            schema: Schema định nghĩa cấu trúc dữ liệu cần extract
            bypass_cache: Bỏ qua cache
            
        Example schema:
            {
                "name": "Articles",
                "baseSelector": "article.post",
                "fields": [
                    {"name": "title", "selector": "h2", "type": "text"},
                    {"name": "url", "selector": "a", "type": "attribute", "attribute": "href"}
                ]
            }
        """
        # Đảm bảo schema có baseSelector (Crawl4AI yêu cầu bắt buộc)
        if 'baseSelector' not in schema or not schema.get('baseSelector'):
            schema['baseSelector'] = 'body'
        
        # Đảm bảo schema có name
        if 'name' not in schema:
            schema['name'] = 'ExtractedData'
        
        # Đảm bảo schema có fields
        if 'fields' not in schema:
            schema['fields'] = []
        
        # Debug: Log schema trước khi tạo strategy
        print(f"\n[WebScraper] Schema trước khi tạo JsonCssExtractionStrategy:")
        print(f"  name: {schema.get('name')}")
        print(f"  baseSelector: {schema.get('baseSelector')}")
        for i, field in enumerate(schema.get('fields', [])):
            field_selector = field.get('selector', 'N/A')
            is_xpath = field_selector.startswith('//') if field_selector != 'N/A' else False
            print(f"  Field {i+1}: name='{field.get('name')}', selector='{field_selector[:80]}...', is_xpath={is_xpath}")
        
        extraction_strategy = JsonCssExtractionStrategy(schema)
        
        config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            cache_mode=CacheMode.BYPASS if bypass_cache else CacheMode.ENABLED,
            word_count_threshold=10
        )
        
        print(f"[WebScraper] Đang scrape URL: {url[:80]}...")
        result = await self.crawler.arun(url=url, config=config)
        
        # Debug: Log kết quả
        print(f"[WebScraper] Kết quả scrape:")
        print(f"  success: {result.success}")
        if result.success:
            print(f"  extracted_content type: {type(result.extracted_content)}")
            if result.extracted_content:
                print(f"  extracted_content preview: {str(result.extracted_content)[:200]}...")
        else:
            print(f"  error_message: {result.error_message}")
        
        extracted_data = None
        if result.success and result.extracted_content:
            try:
                extracted_data = json.loads(result.extracted_content)
            except json.JSONDecodeError:
                extracted_data = result.extracted_content
        
        return {
            "success": result.success,
            "url": url,
            "extracted_data": extracted_data,
            "markdown": result.markdown.raw_markdown if result.success else None,
            "error": result.error_message if not result.success else None,
            "timestamp": datetime.now().isoformat()
        }
    
    async def scrape_with_regex(self, url: str, patterns: Optional[Dict[str, str]] = None, 
                                builtin_patterns: Optional[int] = None, bypass_cache: bool = False) -> Dict[str, Any]:
        """
        Cào dữ liệu sử dụng regex patterns
        
        Args:
            url: URL cần cào
            patterns: Dict custom patterns {label: regex}
            builtin_patterns: Bit flags cho built-in patterns (Email, Phone, etc.)
            bypass_cache: Bỏ qua cache
        """
        extraction_strategy = RegexExtractionStrategy(
            pattern=builtin_patterns or RegexExtractionStrategy.All,
            custom=patterns
        )
        
        config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            cache_mode=CacheMode.BYPASS if bypass_cache else CacheMode.ENABLED
        )
        
        result = await self.crawler.arun(url=url, config=config)
        
        extracted_data = None
        if result.success and result.extracted_content:
            try:
                extracted_data = json.loads(result.extracted_content)
            except json.JSONDecodeError:
                extracted_data = result.extracted_content
        
        return {
            "success": result.success,
            "url": url,
            "extracted_data": extracted_data,
            "markdown": result.markdown.raw_markdown if result.success else None,
            "error": result.error_message if not result.success else None,
            "timestamp": datetime.now().isoformat()
        }
    
    async def scrape_with_llm(self, url: str, instruction: str, 
                             llm_config: Optional[LLMConfig] = None, 
                             bypass_cache: bool = False) -> Dict[str, Any]:
        """
        Cào dữ liệu sử dụng LLM để extract thông minh
        
        Args:
            url: URL cần cào
            instruction: Hướng dẫn cho LLM về dữ liệu cần extract
            llm_config: Config cho LLM (nếu None sẽ dùng OpenAI từ env)
            bypass_cache: Bỏ qua cache
        """
        # Nếu không có llm_config, thử lấy từ env
        if llm_config is None:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                llm_config = LLMConfig(
                    provider="openai/gpt-4o-mini",
                    api_token=api_key
                )
            else:
                return {
                    "success": False,
                    "url": url,
                    "error": "Cần cung cấp LLM config hoặc set OPENAI_API_KEY trong env",
                    "timestamp": datetime.now().isoformat()
                }
        
        extraction_strategy = LLMExtractionStrategy(
            llm_config=llm_config,
            extraction_type="schema",
            instruction=instruction
        )
        
        config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            cache_mode=CacheMode.BYPASS if bypass_cache else CacheMode.ENABLED,
            word_count_threshold=50
        )
        
        result = await self.crawler.arun(url=url, config=config)
        
        extracted_data = None
        if result.success and result.extracted_content:
            try:
                extracted_data = json.loads(result.extracted_content)
            except json.JSONDecodeError:
                extracted_data = result.extracted_content
        
        return {
            "success": result.success,
            "url": url,
            "extracted_data": extracted_data,
            "markdown": result.markdown.raw_markdown if result.success else None,
            "error": result.error_message if not result.success else None,
            "timestamp": datetime.now().isoformat()
        }
    
    async def scrape_multiple(self, urls: List[str], bypass_cache: bool = False) -> List[Dict[str, Any]]:
        """
        Cào nhiều URLs cùng lúc
        
        Args:
            urls: List các URLs cần cào
            bypass_cache: Bỏ qua cache
            
        Returns:
            List các kết quả
        """
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS if bypass_cache else CacheMode.ENABLED
        )
        
        results = await self.crawler.arun_many(urls=urls, config=config)
        
        output = []
        for result in results:
            output.append({
                "success": result.success,
                "url": result.url if hasattr(result, 'url') else "",
                "markdown": result.markdown.raw_markdown if result.success else None,
                "html": result.html if result.success else None,  # <--- Sửa ở đây: Lấy HTML gốc
                "error": result.error_message if not result.success else None,
                "timestamp": datetime.now().isoformat()
            })
        
        return output
    
    def save_to_file(self, data: Dict[str, Any], filename: Optional[str] = None, 
                    output_dir: str = "output") -> str:
        """
        Lưu kết quả vào file JSON
        
        Args:
            data: Dữ liệu cần lưu
            filename: Tên file (nếu None sẽ tự động generate)
            output_dir: Thư mục output
            
        Returns:
            Đường dẫn file đã lưu
        """
        Path(output_dir).mkdir(exist_ok=True)
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"scrape_{timestamp}.json"
        
        filepath = Path(output_dir) / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return str(filepath)


# Hàm helper để sử dụng dễ dàng
async def quick_scrape(url: str, output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Hàm helper để cào nhanh một URL
    
    Args:
        url: URL cần cào
        output_file: File để lưu kết quả (optional)
    
    Returns:
        Dict chứa kết quả
    """
    async with WebScraper() as scraper:
        result = await scraper.scrape_simple(url)
        
        if output_file:
            scraper.save_to_file(result, output_file)
            print(f"✅ Đã lưu kết quả vào: {output_file}")
        
        return result


if __name__ == "__main__":
    # Example usage
    async def main():
        async with WebScraper(headless=True, verbose=True) as scraper:
            # Test với một URL đơn giản
            print(" Đang cào dữ liệu từ example.com...")
            result = await scraper.scrape_simple("https://example.com")
            
            if result["success"]:
                print(f"✅ Thành công!")
                print(f"📄 Markdown length: {len(result['markdown'])} chars")
                print(f"📝 Title: {result['title']}")
                print(f"\n--- Markdown preview ---\n{result['markdown'][:500]}")
                
                # Lưu kết quả
                filepath = scraper.save_to_file(result)
                print(f"\n💾 Đã lưu vào: {filepath}")
            else:
                print(f"❌ Lỗi: {result['error']}")
    
    asyncio.run(main())
