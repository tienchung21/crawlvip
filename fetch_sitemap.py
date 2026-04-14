
import asyncio
import nodriver as n
import sys

async def main():
    try:
        print("Starting browser...")
        # Use no_sandbox=True to avoid permission issues
        browser = await n.start(headless=True, sandbox=False, browser_args=["--no-sandbox", "--disable-gpu"])
        
        url = "https://batdongsan.com.vn/sitemap/detailed-listings.xml"
        print(f"Navigating to {url}...")
        page = await browser.get(url)
        
        # Wait for potential cloudflare challenge
        print("Waiting for page load/challenge...")
        await asyncio.sleep(15) 
        
        # Check if we got XML or HTML
        content = await page.get_content()
        
        print("\n--- CONTENT PREVIEW ---")
        print(content[:2000])
        print("\n--- END PREVIEW ---")
        
        # Save to file for further analysis if needed
        with open("sitemap_dump.xml", "w", encoding="utf-8") as f:
            f.write(content)
            
        browser.stop()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    n.loop().run_until_complete(main())
