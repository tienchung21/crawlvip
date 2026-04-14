
import asyncio
import nodriver as n
import sys

async def main():
    try:
        print("Starting Nodriver V2...")
        # minimal args
        args = [
            "--no-sandbox", 
            "--disable-gpu", 
            "--disable-dev-shm-usage",
            "--no-proxy-server", # Bypass Squid
            "--disable-extensions"
        ]
        
        # sandbox=False => adds --no-sandbox
        browser = await n.start(
            headless=False, 
            sandbox=False, 
            browser_args=args
        )
        
        url = "https://batdongsan.com.vn/sitemap/detailed-listings.xml"
        print(f"Opening {url}...")
        page = await browser.get(url)
        
        print("Waiting 15s for Cloudflare...")
        await asyncio.sleep(15)
        
        content = await page.get_content()
        print("\n--- CONTENT V2 ---")
        print(content[:2000])
        
        browser.stop()
        
    except Exception as e:
        print(f"Error V2: {e}")
        # import traceback
        # traceback.print_exc()

if __name__ == "__main__":
    n.loop().run_until_complete(main())
