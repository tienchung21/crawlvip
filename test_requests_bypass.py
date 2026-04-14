
from curl_cffi import requests
import textwrap

def test_bypass():
    url = "https://batdongsan.com.vn/sitemap/detailed-listings.xml"
    print(f"Testing curl_cffi on {url}...")
    
    try:
        # Impersonate chrome to bypass TLS fingerprinting
        r = requests.get(url, impersonate="chrome120", timeout=30)
        
        print(f"Status Code: {r.status_code}")
        print(f"Content-Type: {r.headers.get('content-type', '')}")
        
        content = r.text
        if "<sitemapindex" in content:
            print("\nSUCCESS! Got XML Sitemap Index.")
            print(f"Content Length: {len(content)}")
            print("Preview:")
            print(textwrap.shorten(content, width=200))
        elif "Just a moment" in content:
            print("\nFAILED. Cloudflare Challenge detected.")
        else:
            print(f"\nUnknown Response. Preview: {content[:200]}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_bypass()
