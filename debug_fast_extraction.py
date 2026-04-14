
import sys
import os
import requests
from lxml import html as lxml_html
import re
import json

# Setup path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from craw.mogi_fast_crawler import scrape_page, create_session, load_template

def test_single(url):
    print(f"Testing URL: {url}")
    session = create_session()
    template = load_template()
    
    data = scrape_page(session, url, template)
    
    # Save HTML for debug
    try:
        resp = session.get(url)
        with open('debug_failed.html', 'w', encoding='utf-8') as f:
            f.write(resp.text)
        print("Saved HTML to debug_failed.html")
    except: pass

    
    print("\n--- EXTRACTED DATA ---")
    keys_to_check = ['map', 'thuocduan', 'title', 'sodienthoai', '_error']
    for k in keys_to_check:
        print(f"{k}: {data.get(k)}")
        
    print("\n--- FULL DATA KEYS ---")
    print(list(data.keys()))

    if 'map' in data:
        print(f"✅ MAP FOUND: {data['map']}")
    else:
        print("❌ MAP MISSING")

if __name__ == "__main__":
    url = "https://mogi.vn/quan-12/thue-can-ho-dich-vu/can-ho-dich-vu-cao-cap-dep-moi-id22660735"
    if len(sys.argv) > 1:
        url = sys.argv[1]
    test_single(url)
