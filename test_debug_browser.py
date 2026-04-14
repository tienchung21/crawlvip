import asyncio
import nodriver as uc
import sys

async def main():
    print("[TEST] Starting nodriver in HEADED mode (headless=False)...")
    try:
        # Minimal arguments
        browser = await uc.start(
            host="127.0.0.1",
            port=9222
        )
        print("[TEST] SUCCESS: Browser connected!")
        
        page = await browser.get("https://google.com")
        print(f"[TEST] Navigation success: {page}")
        
        await asyncio.sleep(2)
        print("[TEST] Stopping browser...")
        browser.stop()
        print("[TEST] Browser stopped.")
        
    except Exception as e:
        print(f"[TEST] FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
