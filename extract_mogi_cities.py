
from playwright.sync_api import sync_playwright
import json
import time

def extract_cities():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print("Navigating to https://mogi.vn...")
        page.goto("https://mogi.vn")
        
        # Wait for potential scripts to load
        time.sleep(5)
        
        try:
            # Try to get mogiCities
            # Using evaluate to get the JSON object
            data = page.evaluate("() => { return window.mogiCities; }")
            
            if data:
                print("SUCCESS: Found mogiCities!")
                with open("mogi_cities.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print("Saved to mogi_cities.json")
            else:
                print("FAILED: mogiCities is undefined or null.")
                
        except Exception as e:
            print(f"Error evaluating mogiCities: {e}")
            
        browser.close()

if __name__ == "__main__":
    extract_cities()
