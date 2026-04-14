import requests
import json
from lxml import html
from lxml.cssselect import CSSSelector

# Load template
with open('/home/chungnt/crawlvip/craw/template/mogidetails.json', 'r') as f:
    template = json.load(f)

url = "https://mogi.vn/quan-thanh-xuan/mua-can-ho-tap-the-cu-xa/ban-can-ho-tap-the-thanh-xuan-bac-gia-1-3-ty-62m2-noi-that-co-ban-id21294540"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

print(f"Fetching: {url}")
response = requests.get(url, headers=headers)
# Decode response properly
html_content = response.content.decode('utf-8')
tree = html.fromstring(html_content)

print(f"{'FIELD':<20} | {'SELECTOR':<50} | {'VALUE'}")
print("-" * 100)

for field in template['fields']:
    name = field['name']
    selector = field.get('selector', '')
    value = "N/A"
    
    if not selector: 
        continue
        
    try:
        if selector.startswith('//'):
            elements = tree.xpath(selector)
        else:
            sel = CSSSelector(selector)
            elements = sel(tree)
            
        if elements:
            el = elements[0]
            if field.get('valueType') == 'src':
                value = el.get('src')
            elif field.get('valueType') == 'regex':
                 value = "(Regex fields skipped in simple debug)"
            else:
                value = el.text_content().strip()
        else:
            value = "NOT FOUND"
            
    except Exception as e:
        value = f"ERROR: {e}"
        
    print(f"{name:<20} | {selector:<50} | {value}")
