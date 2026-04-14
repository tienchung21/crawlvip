from seleniumbase import Driver
import time
import re

def extract_phone_from_html(html: str):
    # 1. Try AgentMobile in pageData
    agent_mobile = re.search(r'"AgentMobile"\s*:\s*"(\d+)"', html)
    if agent_mobile:
        print(f"Found AgentMobile: {agent_mobile.group(1)}")
    else:
        print("AgentMobile not found")
        
    # 2. Try PhoneFormat binding
    phone_format = re.search(r"PhoneFormat\('(\d+)'\)", html)
    if phone_format:
        print(f"Found PhoneFormat: {phone_format.group(1)}")
    else:
        print("PhoneFormat not found")
        
    # 3. Check what IS visible
    binding = re.search(r'ng-bind="PhoneFormat\([^\)]+\)"', html)
    if binding:
        print(f"Found binding snippet: {binding.group(0)}")

url = "https://mogi.vn/quan-dong-da/mua-duong-noi-bo/phan-lo-truong-chinh-55m-4tm-o-to-tranh-nhanh-thoi-id22718169"
print(f"Visiting {url}...")

driver = Driver(uc=True, headless=True)
try:
    driver.get(url)
    time.sleep(5)
    page_source = driver.page_source
    print(f"Page source length: {len(page_source)}")
    
    extract_phone_from_html(page_source)
    
finally:
    driver.quit()
