
import sys
import os
import re
from lxml import html as lxml_html

def test_extract():
    # Load debug_mogi.html
    try:
        with open("debug_mogi.html", "r", encoding="utf-8") as f:
            html = f.read()
    except:
        print("debug_mogi.html not found.")
        return

    tree = lxml_html.fromstring(html)
    data = {}
    
    # Simulate logic
    print("Testing Extraction Logic:")
    
    map_iframes = tree.xpath('//div[contains(@class,"map-content")]//iframe/@src')
    if map_iframes:
        src = map_iframes[0]
        print(f"Found iframe src: {src}")
        
        # New Regex
        coord_match = re.search(r'[?&]q=(-?\d+\.?\d*),(-?\d+\.?\d*)', src)
        if coord_match:
            data['map'] = f"{coord_match.group(1)},{coord_match.group(2)}"
            print(f"✅ Extracted map: {data['map']}")
        else:
            print("❌ Regex failed to match.")
    else:
        print("❌ No map iframe found.")

if __name__ == "__main__":
    test_extract()
