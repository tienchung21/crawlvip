
import asyncio
import os
import nodriver as uc

# IMPORTANT: Bypass proxy for localhost to prevent ERR_TUNNEL_CONNECTION_FAILED (websocket)
# But ALLOW proxy for external sites (if system has http_proxy set)
os.environ['no_proxy'] = '127.0.0.1,localhost'

BROWSER_CONFIG = [
    "--blink-settings=imagesEnabled=true", 
    "--disable-blink-features=AutomationControlled",
]

async def main():
    target_url = "https://alonhadat.com.vn/xac-thuc-nguoi-dung.html?url=/thu-mua-dat-dinh-quy-hoach-cac-phan-khu-tai-do-thi-moi-cam-lam-18134234.html"
    
    print(f"Starting test on {target_url}")
    
    try:
        browser = await uc.start(
            headless=True, # Headless for snapshot
            browser_args=BROWSER_CONFIG,
            sandbox=True
        )
        
        page = await browser.get(target_url)
        await asyncio.sleep(5) # Wait for load
        
        # 1. Save HTML
        try:
            content = await page.evaluate("document.documentElement.outerHTML")
            with open("captcha_source.html", "w", encoding="utf-8") as f:
                f.write(content)
            print("Saved captcha_source.html")
        except Exception as e:
            print(f"Failed to get content: {e}")
        
        # 2. Take Screenshot
        try:
            await page.save_screenshot("captcha_screenshot.jpg")
            print("Saved captcha_screenshot.jpg")
        except Exception as e:
            print(f"Screenshot failed: {e}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # browser.stop() # nodriver sometimes hangs on stop
        pass

if __name__ == "__main__":
    asyncio.run(main())
