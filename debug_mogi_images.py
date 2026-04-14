import requests
from lxml import html

url = "https://mogi.vn/quan-2/mua-nha-biet-thu-lien-ke/ban-biet-thu-khu-thoi-bao-kinh-te-an-khanh-quan-2-dt-119m2-gia-32-ty-id22682684"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

print(f"Fetching: {url}")
response = requests.get(url, headers=headers)
html_content = response.content.decode('utf-8')
tree = html.fromstring(html_content)

# 1. Test current selector
current_imgs = tree.xpath('//div[@id="gallery"]//img/@src')
print(f"Current Selector (//div[@id='gallery']//img/@src) found: {len(current_imgs)} images")
for img in current_imgs:
    print(f" - {img}")

# 2. Inspect gallery structure
gallery = tree.xpath('//div[@id="gallery"]')
if gallery:
    print("\nGallery div content (first 500 chars):")
    print(html.tostring(gallery[0], encoding='unicode')[:1000])
else:
    print("\nDiv #gallery NOT FOUND")

# 3. Look for other potential image containers
media_imgs = tree.xpath('//div[contains(@class, "media-item")]//img/@src')
print(f"\nAlternative Selector (//div[contains(@class, 'media-item')]//img/@src) found: {len(media_imgs)} images")
for img in media_imgs:
    print(f" - {img}")
    
# 4. Check for lazy loaded images (data-src)
lazy_imgs = tree.xpath('//div[@id="gallery"]//img/@data-src')
print(f"\nLazy Load Selector (//div[@id='gallery']//img/@data-src) found: {len(lazy_imgs)} images")
for img in lazy_imgs:
    print(f" - {img}")
