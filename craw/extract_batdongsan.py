"""
Tool extract thông tin bất động sản từ batdongsan.com.vn
Kết hợp CSS Schema + HTML parsing + AI (tùy chọn)

Cách sử dụng:
  python extract_batdongsan.py <URL> [output_file] [--no-ai]
  
Ví dụ:


  python extract_batdongsan.py ""
"""

import asyncio
import os
import json
import re
import sys
from datetime import datetime
from bs4 import BeautifulSoup
from web_scraper import WebScraper
from crawl4ai import LLMConfig, JsonCssExtractionStrategy

# Set Groq API key
GROQ_API_KEY = "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"
os.environ["GROQ_API_KEY"] = GROQ_API_KEY

# Decrypt phone config - import từ file riêng
try:
    from decrypt_config import DECRYPT_ENABLED, COOKIES, USER_AGENT
    DECRYPT_PHONE_ENABLED = DECRYPT_ENABLED
    COOKIES_FOR_DECRYPT = COOKIES
    USER_AGENT_FOR_DECRYPT = USER_AGENT
except ImportError:
    DECRYPT_PHONE_ENABLED = False
    COOKIES_FOR_DECRYPT = ""
    USER_AGENT_FOR_DECRYPT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def decrypt_phone_number(encrypted_phone: str) -> str:
    """
    Decrypt phone number using API
    Requires: pip install curl_cffi
    """
    if not DECRYPT_PHONE_ENABLED or not COOKIES_FOR_DECRYPT or not encrypted_phone:
        return None
    
    try:
        from curl_cffi import requests
        
        # Parse cookies
        cookies = {}
        for cookie in COOKIES_FOR_DECRYPT.strip().split('; '):
            if '=' in cookie:
                key, value = cookie.split('=', 1)
                cookies[key.strip()] = value.strip()
        
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://batdongsan.com.vn',
            'Referer': 'https://batdongsan.com.vn/',
            'User-Agent': USER_AGENT_FOR_DECRYPT.strip(),
            'X-Requested-With': 'XMLHttpRequest',
        }
        
        data = {
            'PhoneNumber': encrypted_phone,
            'createLead[mobile]': '4883611',
            'createLead[sellerId]': '4491058',
            'createLead[productId]': '44503982',
            'createLead[leadSourcePage]': 'BDS_LISTING_DETAILS_PAGE',
            'createLead[leadSourceAction]': 'PHONE_REVEAL',
            'createLead[fromLeadType]': 'AGENT_LISTING'
        }
        
        response = requests.post(
            "https://batdongsan.com.vn/Product/ProductDetail/DecryptPhone",
            headers=headers,
            cookies=cookies,
            data=data,
            timeout=10,
            impersonate="chrome120"
        )
        
        if response.status_code == 200:
            phone = response.text.strip()
            if phone and len(phone) >= 9:
                return phone
        
        return None
        
    except Exception as e:
        print(f"⚠️  Decrypt failed: {e}")
        return None


def clean_text(text: str) -> str:
    """Làm sạch text, loại bỏ markdown, link, etc."""
    if not text:
        return ""
    
    # Loại bỏ markdown links
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
    # Loại bỏ markdown images
    text = re.sub(r'!\[([^\]]*)\]\([^\)]+\)', '', text)
    # Loại bỏ HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Loại bỏ multiple spaces
    text = re.sub(r'\s+', ' ', text)
    # Trim
    text = text.strip()
    
    return text


def extract_from_html(html: str, raw_html: str = None) -> dict:
    """
    Extract thông tin từ HTML bằng BeautifulSoup
    
    Args:
        html: Cleaned HTML để parse
        raw_html: Raw HTML (có iframe) để extract tọa độ
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    # Nếu không có raw_html, dùng html
    if raw_html is None:
        raw_html = html
    data = {}
    
    # Title - tìm trong h1 (sau breadcrumb)
    h1 = soup.find('h1')
    if h1:
        title_text = clean_text(h1.get_text())
        # Loại bỏ phần logo nếu có
        if 'Nền tảng bất động sản' not in title_text and len(title_text) > 5:
            data['title'] = title_text
    
    # Fallback: tìm h1 với class cụ thể
    if not data.get('title'):
        h1_with_class = soup.select_one('h1.re__pr-title, h1[class*="title"]')
        if h1_with_class:
            title_text = clean_text(h1_with_class.get_text())
            if len(title_text) > 10:
                data['title'] = title_text
    
    # Địa chỉ - tìm span ngay sau h1
    if h1:
        next_span = h1.find_next_sibling('span')
        if next_span:
            address = clean_text(next_span.get_text())
            if address and len(address) > 5:
                data['dia_chi'] = address
    
    # Giá - tìm span có text "Khoảng giá" rồi lấy span kế tiếp
    price_label = soup.find('span', string=re.compile(r'Khoảng giá', re.I))
    if price_label:
        price_value = price_label.find_next_sibling('span')
        if price_value:
            data['khoang_gia'] = clean_text(price_value.get_text())
    
    # Fallback: tìm bất kỳ element nào có text "tỷ" hoặc "triệu"
    if not data.get('khoang_gia'):
        price_elems = soup.find_all(string=re.compile(r'\d+[\.,]?\d*\s*(tỷ|triệu|tr|nghìn)', re.I))
        for elem in price_elems:
            price_text = clean_text(elem)
            # Kiểm tra có số và đơn vị tiền tệ
            if re.search(r'\d+[\.,]?\d*\s*(tỷ|triệu)', price_text, re.I):
                # Không lấy nếu nằm trong mô tả dài
                if len(price_text) < 100:
                    data['khoang_gia'] = price_text
                    break
    
    # Diện tích - tìm span có text "Diện tích" rồi lấy span kế tiếp
    # Tìm tất cả các span "Diện tích" và lấy cái đầu tiên hợp lệ
    area_labels = soup.find_all('span', string=re.compile(r'Diện tích', re.I))
    for area_label in area_labels:
        # Kiểm tra xem có nằm trong phần môi giới không (có link guru trong parent)
        parent_div = area_label.find_parent('div')
        if parent_div:
            # Nếu parent có link guru thì bỏ qua (đây là phần môi giới)
            if parent_div.find('a', href=re.compile(r'guru\.batdongsan\.com\.vn', re.I)):
                continue
        
        area_value = area_label.find_next_sibling('span')
        if area_value:
            area_text = clean_text(area_value.get_text())
            # Chỉ lấy nếu có m² hoặc m2 và có số trong đó
            if ('m²' in area_text or 'm2' in area_text.lower()):
                # Phải bắt đầu bằng số (không phải tên)
                if re.match(r'^\d+', area_text):
                    # Không chứa "Xem thêm", "tin khác", "Bđs" (tên công ty)
                    if not any(word in area_text for word in ['Xem thêm', 'tin khác', 'Bđs', 'BDS']):
                        # Phải có độ dài hợp lý (không quá dài)
                        if len(area_text) < 20:
                            data['dien_tich'] = area_text
                            break
    
    # Fallback: tìm trong phần "Đặc điểm bất động sản" (nếu có)
    if not data.get('dien_tich'):
        # Tìm span "Diện tích" trong phần đặc điểm
        dac_diem_section = soup.find('div', string=re.compile(r'Đặc điểm', re.I))
        if dac_diem_section:
            parent = dac_diem_section.find_parent()
            if parent:
                area_spans = parent.find_all('span', string=re.compile(r'Diện tích', re.I))
                for span in area_spans:
                    next_span = span.find_next_sibling('span')
                    if next_span:
                        area_val = clean_text(next_span.get_text())
                        if re.match(r'^\d+.*m[²2]', area_val) and len(area_val) < 20:
                            data['dien_tich'] = area_val
                            break
    

    
    # Đặc điểm bất động sản (khoảng giá, diện tích, hướng nhà, mặt tiền, đường vào, pháp lý)
    dac_diem = {}
    
    # Khoảng giá (lấy từ đã extract ở trên)
    if data.get('khoang_gia'):
        dac_diem['khoang_gia'] = data['khoang_gia']
    
    # Diện tích (lấy từ đã extract ở trên)
    if data.get('dien_tich'):
        dac_diem['dien_tich'] = data['dien_tich']
    
    # Hướng nhà
    huong_label = soup.find('span', string=re.compile(r'Hướng nhà', re.I))
    if huong_label:
        huong_value = huong_label.find_next_sibling('span')
        if huong_value:
            huong_text = clean_text(huong_value.get_text())
            if huong_text and len(huong_text) < 50:
                dac_diem['huong_nha'] = huong_text
    
    # Mặt tiền
    mat_tien_label = soup.find('span', string=re.compile(r'Mặt tiền', re.I))
    if mat_tien_label:
        mat_tien_value = mat_tien_label.find_next_sibling('span')
        if mat_tien_value:
            mat_tien_text = clean_text(mat_tien_value.get_text())
            if mat_tien_text and len(mat_tien_text) < 50 and re.search(r'\d', mat_tien_text):
                dac_diem['mat_tien'] = mat_tien_text
    
    # Đường vào
    duong_label = soup.find('span', string=re.compile(r'Đường vào', re.I))
    if duong_label:
        duong_value = duong_label.find_next_sibling('span')
        if duong_value:
            duong_text = clean_text(duong_value.get_text())
            if duong_text and len(duong_text) < 50:
                dac_diem['duong_vao'] = duong_text
    
    # Pháp lý
    phap_ly_label = soup.find('span', string=re.compile(r'Pháp lý', re.I))
    if phap_ly_label:
        phap_ly_value = phap_ly_label.find_next_sibling('span')
        if phap_ly_value:
            phap_ly_text = clean_text(phap_ly_value.get_text())
            if phap_ly_text and len(phap_ly_text) < 100:
                dac_diem['phap_ly'] = phap_ly_text
    
    if dac_diem:
        data['dac_diem'] = dac_diem
    
    # Mô tả - tìm div sau span "Thông tin mô tả"
    desc_label = soup.find('span', string=re.compile(r'Thông tin mô tả', re.I))
    if desc_label:
        desc_div = desc_label.find_next('div')
        if desc_div:
            desc = clean_text(desc_div.get_text())
            if desc and len(desc) > 20:
                data['mo_ta'] = desc
    
    # Tên dự án - tìm trong phần "Thông tin dự án"
    du_an_label = soup.find('div', string=re.compile(r'Thông tin dự án', re.I))
    if du_an_label:
        # Tìm div chứa tên dự án
        parent = du_an_label.find_parent()
        if parent:
            # Tìm tất cả div và span con
            name_elems = parent.find_all(['div', 'span', 'a'], recursive=True)
            for elem in name_elems:
                elem_text = clean_text(elem.get_text())
                # Tìm text ngắn (10-100 ký tự) có chữ in hoa
                if 10 < len(elem_text) < 100:
                    # Loại bỏ các text không phải tên dự án
                    if 'Xem' not in elem_text and 'tin đăng' not in elem_text.lower() and 'Đang cập nhật' not in elem_text:
                        # Kiểm tra xem có phải tên dự án không (không phải link, không phải số)
                        if not elem_text.startswith('http') and not re.match(r'^\d+$', elem_text) and not elem_text.startswith('·'):
                            # Phải có ít nhất 1 chữ in hoa hoặc có dấu gạch ngang
                            if re.search(r'[A-ZĐ]', elem_text) or '-' in elem_text:
                                # Tên dự án thường ngắn (2-10 từ)
                                word_count = len(elem_text.split())
                                if 2 <= word_count <= 10:
                                    data['du_an'] = {'ten': elem_text}
                                    break
            
            # Tìm link
            du_an_link = parent.find('a', href=re.compile(r'du-an|the-paris|vinhomes', re.I))
            if du_an_link:
                link = du_an_link.get('href', '')
                if link:
                    if not link.startswith('http'):
                        link = 'https://batdongsan.com.vn' + link
                    if not data.get('du_an'):
                        data['du_an'] = {}
                    data['du_an']['link'] = link
    
    # Ngày đăng - tìm span "Ngày đăng" rồi lấy span kế tiếp
    ngay_dang_label = soup.find('span', string=re.compile(r'Ngày đăng', re.I))
    if ngay_dang_label:
        ngay_dang_value = ngay_dang_label.find_next_sibling('span')
        if ngay_dang_value:
            data['ngay_dang'] = clean_text(ngay_dang_value.get_text())
    
    # Ngày hết hạn
    ngay_het_han_label = soup.find('span', string=re.compile(r'Ngày hết hạn', re.I))
    if ngay_het_han_label:
        ngay_het_han_value = ngay_het_han_label.find_next_sibling('span')
        if ngay_het_han_value:
            data['ngay_het_han'] = clean_text(ngay_het_han_value.get_text())
    
    # Loại tin - tìm span "Loại tin" rồi lấy span kế tiếp
    loai_tin_label = soup.find('span', string=re.compile(r'Loại tin', re.I))
    if loai_tin_label:
        loai_tin_value = loai_tin_label.find_next_sibling('span')
        if loai_tin_value:
            data['loai_tin'] = clean_text(loai_tin_value.get_text())
    
    # Mã tin - tìm span "Mã tin" rồi lấy span kế tiếp
    ma_tin_label = soup.find('span', string=re.compile(r'Mã tin', re.I))
    if ma_tin_label:
        ma_tin_value = ma_tin_label.find_next_sibling('span')
        if ma_tin_value:
            data['ma_tin'] = clean_text(ma_tin_value.get_text())
    
    # Môi giới - tìm trong thẻ a có href chứa "guru.batdongsan.com.vn"
    moi_gioi = {}
    
    # Tìm tất cả thẻ a có href chứa "guru.batdongsan.com.vn"
    guru_links = soup.find_all('a', href=re.compile(r'guru\.batdongsan\.com\.vn', re.I))
    for link in guru_links:
        # Tìm tên môi giới - text trong thẻ a hoặc span con
        name_text = clean_text(link.get_text())
        # Loại bỏ các text không phải tên
        if name_text and 'Xem thêm' not in name_text and 'Xem trang' not in name_text and 'Chat' not in name_text and len(name_text) > 2:
            # Kiểm tra xem có phải tên không (2-4 từ, mỗi từ bắt đầu bằng chữ hoa)
            # Pattern mềm hơn: chấp nhận cả tên Latin và Việt có dấu
            # "Don Văn Dũng", "Nguyễn Văn A", "John Smith"
            words = name_text.split()
            if 2 <= len(words) <= 4:
                # Mỗi từ phải bắt đầu bằng chữ hoa
                if all(word[0].isupper() or word[0] in 'ĐĂÂÊÔƠƯÉÈẺẼẸẾỀỂỄỆẤẦẨẪẬỐỒỔỖỘỚỜỞỠỢỨỪỬỮỰ' for word in words if word):
                    # Không phải là cụm từ action (Cho Thu, Xem Chi Tiet)
                    if name_text not in ['Cho Thu', 'Cho Thue', 'Xem Chi Tiet', 'Xem Them']:
                        moi_gioi['ten'] = name_text
                        break
    
    # Tìm số điện thoại
    # 1. Tìm encrypted phone trong raw_html (attribute "raw")
    encrypted_phone = None
    if raw_html:
        raw_soup = BeautifulSoup(raw_html, 'html.parser')
        for element in raw_soup.find_all(attrs={'raw': True}):
            raw_attr = element.get('raw')
            if raw_attr and len(raw_attr) > 20:  # Encrypted string dài > 20 ký tự
                encrypted_phone = raw_attr
                moi_gioi['so_dien_thoai_ma_hoa'] = raw_attr
                break
    
    # 1b. Decrypt phone nếu bật
    if encrypted_phone and DECRYPT_PHONE_ENABLED:
        decrypted = decrypt_phone_number(encrypted_phone)
        if decrypted:
            moi_gioi['so_dien_thoai_giai_ma'] = decrypted
            print(f"   ✅ Decrypted: {decrypted}")
    
    # 2. Tìm số điện thoại hiển thị (có thể bị ẩn một phần)
    # Pattern linh hoạt hơn: 0xxx xxx ***, 09xx xxx xxx, etc.
    phone_spans = soup.find_all('span', string=re.compile(r'\d{3,4}\s?\d{3}\s?[\d\*]{3,4}', re.I))
    for span in phone_spans:
        phone_text = clean_text(span.get_text())
        # Tìm pattern số điện thoại (có thể bị ẩn một phần)
        phone_match = re.search(r'(\d{3,4}[\s\-]?\d{3}[\s\-]?[\d\*]{3,4})', phone_text)
        if phone_match:
            phone = phone_match.group(1).strip()
            # Chuẩn hóa format
            phone = re.sub(r'[\s\-]+', ' ', phone)
            moi_gioi['so_dien_thoai'] = phone
            break
    
    # Hình môi giới - tìm img trong phần có link guru
    if guru_links:
        for link in guru_links:
            img = link.find('img')
            if img and img.get('src'):
                src = img.get('src')
                if src and not any(word in src.lower() for word in ['logo', 'banner', 'app-store', 'google-play']):
                    if not src.startswith('http'):
                        src = 'https://batdongsan.com.vn' + src
                    moi_gioi['link_hinh'] = src
                    break
    
    if moi_gioi:
        data['moi_gioi'] = moi_gioi
    
    # Images - tìm tất cả img có src chứa "file4.batdongsan.com.vn/resize"
    images = []
    img_tags = soup.find_all('img', src=re.compile(r'file4\.batdongsan\.com\.vn/resize', re.I))
    for img in img_tags:
        src = img.get('src', '')
        if src:
            # Chỉ lấy hình lớn (1275x717) hoặc thumbnail (200x200, 255x180)
            if 'resize/1275x717' in src or 'resize/200x200' in src or 'resize/255x180' in src:
                # Loại bỏ logo, banner
                if not any(word in src.lower() for word in ['logo', 'banner', 'app-store', 'google-play', 'footer', 'crop']):
                    if src not in images:
                        images.append(src)
    
    # Sắp xếp: hình lớn trước
    images.sort(key=lambda x: '1275x717' in x, reverse=True)
    
    if images:
        data['images'] = {
            'album': images,
            'main_image': images[0] if images else None
        }
    
    # Tọa độ - tìm trong nhiều nguồn
    toa_do = {}
    
    # 1. Tìm trong iframe Google Maps bằng regex trực tiếp trên HTML raw
    # Pattern: data-src="https://www.google.com/maps/embed/v1/place?q=10.786270701503849,106.7317658241369
    iframe_match = re.search(r'(?:data-src|src)="[^"]*google\.com/maps/[^"]*[?&]q=([\d.]+),([\d.]+)', raw_html, re.I)
    if iframe_match:
        toa_do['lat'] = iframe_match.group(1)
        toa_do['lng'] = iframe_match.group(2)
    
    # 2. Backup: tìm trong iframe tags parsed (nếu chưa có)
    if not toa_do:
        iframes = soup.find_all('iframe')
        for iframe in iframes:
            src = iframe.get('data-src', '') or iframe.get('src', '')
            if 'google.com/maps' in src:
                # Pattern: q=20.995635503329915,105.93618149734492
                coord_match = re.search(r'q=([\d.]+),([\d.]+)', src)
                if coord_match:
                    toa_do['lat'] = coord_match.group(1)
                    toa_do['lng'] = coord_match.group(2)
                    break
    
    # 2. Tìm trong script hoặc data attribute
    if not toa_do:
        # Tìm trong script tags có chứa tọa độ
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string or ''
            # Pattern: lat: 20.995635, lng: 105.93618
            coord_match = re.search(r'lat["\s:]+([0-9.]+).*?lng["\s:]+([0-9.]+)', script_text, re.I | re.DOTALL)
            if coord_match:
                toa_do['lat'] = coord_match.group(1)
                toa_do['lng'] = coord_match.group(2)
                break
            # Pattern khác: [20.995635, 105.93618]
            coord_match = re.search(r'\[([0-9.]{5,}),\s*([0-9.]{5,})\]', script_text)
            if coord_match:
                toa_do['lat'] = coord_match.group(1)
                toa_do['lng'] = coord_match.group(2)
                break
    
    # Fallback 1: Tìm trong div.place-name
    if not toa_do:
        place_name_elem = soup.select_one('div.place-name')
        if place_name_elem:
            coord_text = clean_text(place_name_elem.get_text())
            # Parse tọa độ dạng "20°59'44.0"N 105°56'10.5"E"
            coord_match = re.search(r'(\d+)°(\d+)\'([\d.]+)"([NS])\s+(\d+)°(\d+)\'([\d.]+)"([EW])', coord_text)
            if coord_match:
                lat_deg = float(coord_match.group(1))
                lat_min = float(coord_match.group(2))
                lat_sec = float(coord_match.group(3))
                lat_dir = coord_match.group(4)
                
                lng_deg = float(coord_match.group(5))
                lng_min = float(coord_match.group(6))
                lng_sec = float(coord_match.group(7))
                lng_dir = coord_match.group(8)
                
                # Convert sang decimal degrees
                lat = lat_deg + lat_min/60 + lat_sec/3600
                if lat_dir == 'S':
                    lat = -lat
                
                lng = lng_deg + lng_min/60 + lng_sec/3600
                if lng_dir == 'W':
                    lng = -lng
                
                toa_do['lat'] = str(lat)
                toa_do['lng'] = str(lng)
                toa_do['raw'] = coord_text
    
    # Fallback 2: Tìm bất kể text nào có pattern tọa độ
    if not toa_do:
        coord_elems = soup.find_all(string=re.compile(r'\d+°\d+\'[\d.]+"[NS]', re.I))
        for elem in coord_elems:
            coord_text = clean_text(elem)
            coord_match = re.search(r'(\d+)°(\d+)\'([\d.]+)"([NS])\s+(\d+)°(\d+)\'([\d.]+)"([EW])', coord_text)
            if coord_match:
                lat_deg = float(coord_match.group(1))
                lat_min = float(coord_match.group(2))
                lat_sec = float(coord_match.group(3))
                lat_dir = coord_match.group(4)
                
                lng_deg = float(coord_match.group(5))
                lng_min = float(coord_match.group(6))
                lng_sec = float(coord_match.group(7))
                lng_dir = coord_match.group(8)
                
                lat = lat_deg + lat_min/60 + lat_sec/3600
                if lat_dir == 'S':
                    lat = -lat
                
                lng = lng_deg + lng_min/60 + lng_sec/3600
                if lng_dir == 'W':
                    lng = -lng
                
                toa_do['lat'] = str(lat)
                toa_do['lng'] = str(lng)
                toa_do['raw'] = coord_text
                break
    
    if toa_do:
        data['toa_do'] = toa_do
    
    return data


async def extract_batdongsan(url: str, output_file: str = None, use_ai: bool = False):
    """
    Extract thông tin bất động sản với độ chính xác cao
    """
    
    print("=" * 60)
    print("EXTRACT THÔNG TIN BẤT ĐỘNG SẢN")
    print("=" * 60)
    print(f"📄 URL: {url}")
    print(f"🤖 AI: {'BẬT' if use_ai else 'TẮT (mặc định)'}")
    print("=" * 60 + "\n")
    
    async with WebScraper(headless=True, verbose=False) as scraper:
        # Bước 1: Crawl để lấy HTML với js_code để trigger lazy load iframe
        print("📥 Bước 1: Đang crawl trang web...")
        
        # JS code để scroll và trigger lazy load iframe
        js_code = [
            "window.scrollTo(0, document.body.scrollHeight);",
            "await new Promise(resolve => setTimeout(resolve, 2000));",
            "const iframes = document.querySelectorAll('iframe[data-src]');",
            "iframes.forEach(iframe => { if (iframe.dataset.src && iframe.dataset.src.includes('google.com/maps')) { iframe.src = iframe.dataset.src; } });",
            "await new Promise(resolve => setTimeout(resolve, 1000));"
        ]
        
        # Retry logic với timeout ngắn hơn
        from crawl4ai import CrawlerRunConfig, CacheMode
        max_retries = 3
        retry_count = 0
        raw_result = None
        
        while retry_count < max_retries:
            try:
                config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    js_code=js_code,
                    page_timeout=30000,  # 30s timeout
                    wait_until="domcontentloaded"  # Không đợi load hết, chỉ DOM ready
                )
                raw_result = await scraper.crawler.arun(url=url, config=config)
                
                if raw_result.success:
                    break
                    
                retry_count += 1
                if retry_count < max_retries:
                    print(f"⚠️  Thử lại lần {retry_count + 1}/{max_retries}...")
                    await asyncio.sleep(2)
                    
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"⚠️  Lỗi: {str(e)[:100]}. Thử lại lần {retry_count + 1}/{max_retries}...")
                    await asyncio.sleep(2)
                else:
                    return {
                        "success": False,
                        "error": f"Connection reset sau {max_retries} lần thử: {str(e)[:200]}"
                    }
        
        if not raw_result or not raw_result.success:
            return {
                "success": False,
                "error": raw_result.error_message if raw_result else "Không crawl được sau 3 lần thử"
            }
        
        raw_html = raw_result.html  # Raw HTML có iframe
        html = raw_result.cleaned_html or raw_html  # Cleaned HTML để parse
        markdown = raw_result.markdown.raw_markdown if raw_result.markdown else ""
        print("✅ Đã crawl xong\n")
        
        # Bước 2: Extract từ HTML bằng BeautifulSoup (dùng cleaned HTML)
        print("📊 Bước 2: Đang extract từ HTML...")
        html_data = extract_from_html(html, raw_html=raw_html)  # Pass raw_html để extract iframe
        print("✅ HTML extract xong\n")
        
        # Bước 3: Extract với CSS Schema
        print("📊 Bước 3: Đang extract với CSS Schema...")
        schema = {
            "name": "BatDongSan",
            "baseSelector": "body",
            "fields": [
                {"name": "title", "selector": "h1.re__pr-title, .re__pr-title, h1", "type": "text"},
                {"name": "price", "selector": ".re__pr-short-info-item-price, .pr-price", "type": "text"},
                {"name": "area", "selector": "[class*='area']", "type": "text"},
                {"name": "address", "selector": ".re__pr-short-info-item-address, .pr-address", "type": "text"},
                {"name": "description", "selector": ".re__section-body, .pr-description", "type": "text"},
            ]
        }
        
        schema_result = await scraper.scrape_with_schema(url, schema, bypass_cache=True)
        schema_data = {}
        if schema_result.get("extracted_data"):
            if isinstance(schema_result["extracted_data"], list) and len(schema_result["extracted_data"]) > 0:
                schema_data = schema_result["extracted_data"][0]
            elif isinstance(schema_result["extracted_data"], dict):
                schema_data = schema_result["extracted_data"]
        
        # Merge schema data vào html_data
        if schema_data.get("title"):
            html_data["title"] = clean_text(schema_data["title"])
        if schema_data.get("price"):
            html_data["khoang_gia"] = clean_text(schema_data["price"])
        if schema_data.get("area"):
            html_data["dien_tich"] = clean_text(schema_data["area"])
        if schema_data.get("address"):
            html_data["dia_chi"] = clean_text(schema_data["address"])
        if schema_data.get("description"):
            html_data["mo_ta"] = clean_text(schema_data["description"])
        
        print("✅ CSS Schema extract xong\n")
        
        # Bước 4: Extract với AI để bổ sung (nếu bật)
        ai_data = {}
        
        if use_ai:
            print("🤖 Bước 4: Đang extract với AI để bổ sung...")
            api_key = os.getenv("GROQ_API_KEY")
            if api_key:
                try:
                    llm_config = LLMConfig(
                        provider="groq/llama-3.3-70b-versatile",
                        api_token=api_key
                    )
                    
                    instruction = """
                    Bổ sung các thông tin còn thiếu hoặc chưa chính xác:
                    - Tên dự án đầy đủ (ví dụ: "The Paris - Vinhomes Ocean Park", không phải slug)
                    - Địa chỉ đầy đủ (không có markdown link)
                    - Đặc điểm bất động sản (hướng ban công, đường vào, pháp lý)
                    - Thông tin dự án
                    - Tọa độ nếu có
                    - Tên môi giới chính xác (KHÔNG phải "Cho Thu")
                    - Số điện thoại (có thể bị ẩn một phần)
                    
                    Trả về JSON chỉ với các thông tin BỔ SUNG hoặc SỬA LẠI.
                    """
                    
                    ai_result = await scraper.scrape_with_llm(url, instruction, llm_config, bypass_cache=True)
                    
                    if ai_result.get("success") and ai_result.get("extracted_data"):
                        ai_data_raw = ai_result["extracted_data"]
                        if isinstance(ai_data_raw, list) and len(ai_data_raw) > 0:
                            ai_data = ai_data_raw[0]
                        elif isinstance(ai_data_raw, dict):
                            ai_data = ai_data_raw
                    
                    print("✅ AI extract xong\n")
                except Exception as e:
                    print(f"⚠️ AI lỗi: {e}, bỏ qua\n")
        else:
            print("⏭️ Bỏ qua AI extraction\n")
        
        # Bước 5: Merge tất cả dữ liệu (ưu tiên HTML > Schema > AI)
        final_data = {
            "title": clean_text(html_data.get("title") or ai_data.get("title", "")),
            "images": html_data.get("images") or ai_data.get("images", {"album": [], "main_image": None}),
            "du_an": html_data.get("du_an") or ai_data.get("du_an", {}),
            "dia_chi": html_data.get("dia_chi") or ai_data.get("dia_chi", ""),
            "mo_ta": html_data.get("mo_ta") or ai_data.get("mo_ta", ""),
            "dac_diem": html_data.get("dac_diem") or ai_data.get("dac_diem", {}),
            "thong_tin_du_an": ai_data.get("thong_tin_du_an", {}),
            "toa_do": html_data.get("toa_do") or ai_data.get("toa_do", {}),
            "ngay_dang": html_data.get("ngay_dang") or ai_data.get("ngay_dang", ""),
            "ngay_het_han": html_data.get("ngay_het_han") or ai_data.get("ngay_het_han", ""),
            "loai_tin": html_data.get("loai_tin") or ai_data.get("loai_tin", ""),
            "ma_tin": html_data.get("ma_tin") or "",
            "duan_id": ai_data.get("duan_id", ""),
            "moi_gioi": html_data.get("moi_gioi") or ai_data.get("moi_gioi", {})
        }
        
        # Extract mã tin từ URL (nếu chưa có từ HTML)
        if not final_data.get("ma_tin"):
            ma_tin_match = re.search(r'pr(\d+)', url)
            if ma_tin_match:
                final_data["ma_tin"] = ma_tin_match.group(1)
        
        # Clean title nếu vẫn có markdown
        if final_data["title"] and ('[' in final_data["title"] or '](' in final_data["title"]):
            final_data["title"] = clean_text(final_data["title"])
        
        # Clean địa chỉ nếu vẫn có markdown
        if final_data["dia_chi"] and '](' in final_data["dia_chi"]:
            final_data["dia_chi"] = clean_text(final_data["dia_chi"])
        
        # Clean tên dự án
        if final_data.get("du_an", {}).get("ten"):
            ten_du_an = final_data["du_an"]["ten"]
            # Kiểm tra xem có phải slug không (ví dụ: "the-paris-vinhomes-ocean-park")
            if '-' in ten_du_an and ten_du_an.islower():
                # Là slug, tìm tên thật từ AI
                if ai_data.get("du_an", {}).get("ten"):
                    final_data["du_an"]["ten"] = ai_data["du_an"]["ten"]
                else:
                    # Convert slug thành tên dự án (capitalize mỗi từ)
                    final_data["du_an"]["ten"] = ' '.join(word.capitalize() for word in ten_du_an.split('-'))
            elif ')' in ten_du_an and '(' not in ten_du_an:
                # Có thể là slug khác, tìm tên thật từ AI
                if ai_data.get("du_an", {}).get("ten"):
                    final_data["du_an"]["ten"] = ai_data["du_an"]["ten"]
            final_data["du_an"]["ten"] = clean_text(final_data["du_an"]["ten"])
        
        # Validate và clean môi giới
        if final_data.get("moi_gioi", {}).get("ten"):
            ten_mg = final_data["moi_gioi"]["ten"]
            # Loại bỏ các tên không hợp lệ
            invalid_names = ['Cho Thu', 'Cho Thue', 'Xem Chi Tiet', 'Xem Them', 'Chat Ngay']
            if ten_mg in invalid_names:
                # Thử lấy từ AI
                if ai_data.get("moi_gioi", {}).get("ten"):
                    final_data["moi_gioi"]["ten"] = ai_data["moi_gioi"]["ten"]
                else:
                    # Xóa tên không hợp lệ
                    del final_data["moi_gioi"]["ten"]
        
        result = {
            "success": True,
            "url": url,
            "extracted_at": datetime.now().isoformat(),
            "data": final_data
        }
        
        # Hiển thị kết quả JSON ngay
        print("\n" + "=" * 60)
        print("📊 JSON OUTPUT")
        print("=" * 60)
        print(json.dumps(final_data, indent=2, ensure_ascii=False))
        print("=" * 60 + "\n")
        
        # Lưu file
        if output_file:
            # Nếu output_file không có path, thêm vào output/
            if '/' not in output_file and '\\' not in output_file:
                output_file = f"output/{output_file}"
            # Nếu đã có output/ rồi thì không thêm nữa
            elif output_file.startswith('output/'):
                pass
            scraper.save_to_file(result, output_file, output_dir="")
            print(f"💾 Đã lưu vào: {output_file}")
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batdongsan_{timestamp}.json"
            scraper.save_to_file(result, filename)
            print(f"💾 Đã lưu vào: output/{filename}")
        
        return result


async def main():
    if len(sys.argv) < 2:
        print("=" * 60)
        print(" TOOL EXTRACT THÔNG TIN BẤT ĐỘNG SẢN")
        print("=" * 60)
        print("\nCách sử dụng:")
        print("  python extract_batdongsan.py <URL> [output_file] [--ai]")
        print("\nVí dụ:")
        print("  python extract_batdongsan.py <URL>                    # Không AI (mặc định)")
        print("  python extract_batdongsan.py <URL> --ai               # Có AI")
        print("  python extract_batdongsan.py <URL> result.json        # Không AI + file output")
        print("  python extract_batdongsan.py <URL> result.json --ai   # Có AI + file output")
        print("\n" + "=" * 60)
        return
    
    url = sys.argv[1]
    output_file = None
    use_ai = False
    
    # Parse arguments
    for arg in sys.argv[2:]:
        if arg == "--ai":
            use_ai = True
        elif not arg.startswith("--"):
            output_file = arg
    
    result = await extract_batdongsan(url, output_file, use_ai)
    
    if result["success"]:
        print("\n" + "=" * 60)
        print("✅ HOÀN THÀNH!")
        print("=" * 60)
    else:
        print(f"\n❌ Lỗi: {result.get('error')}")


if __name__ == "__main__":
    # Fix encoding cho Windows console
    import sys
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    asyncio.run(main())

