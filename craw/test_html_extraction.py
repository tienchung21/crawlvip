
import sys
import os
from lxml import html as lxml_html

# Add path to import headers
sys.path.append(os.getcwd())
# Import the function we modified (using mogi_detail_crawler as source)
from craw.mogi_detail_crawler import extract_value_by_selector

html_snippet = """
<html>
<body>
<div class="info-content-body">
- Bán lô d?t 65m2 - h?m vào 3m, ngay ch? d?u m?i nông s?n Hóc Môn.<br><br>
- Ðu?ng Nguy?n Th? Sóc vào 100m, xã Bà Ði?m, Huy?n Hóc Môn.<br>
- Khu dân cu dông dúc, ti?n ích xung quanh d?y d?.<br>
- H?m vào 3m, xe t?i nh? vô tho?i mái.<br><br>
- Di?n tích d?t: Ngang 5,5m x Dài 12m. T?ng di?n tích 65m2.<br>
- Pháp lý: Mua bán công ch?ng vi b?ng (gi?y t? tay ngu?n g?c rõ ràng).<br><br>
- Giá bán: 860 tri?u.<br>
- Hu?ng Ðông Nam.<br><br>
Hotline: 0779. 938. 978 anh BON.
</div>
</body>
</html>
"""

def test():
    tree = lxml_html.fromstring(html_snippet)
    
    # Selector targeting the div
    selector = '//div[contains(@class, "info-content-body")]'
    
    print("--- Testing valueType='text' (Previous behavior) ---")
    result_text = extract_value_by_selector(tree, selector, 'text')
    print(f"RESULT:\n{result_text}\n")
    
    print("--- Testing valueType='html' (New behavior) ---")
    result_html = extract_value_by_selector(tree, selector, 'html')
    print(f"RESULT:\n{result_html}\n")

if __name__ == "__main__":
    test()
