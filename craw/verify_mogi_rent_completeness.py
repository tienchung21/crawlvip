
import requests
import sys
import os
import re
from datetime import datetime
import lxml.html

# Setup database import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database

# Sourcing .bashrc env vars manually if needed, or just hardcode proxy
PROXY = "http://100.53.5.135:3128"
PROXIES = {"http": PROXY, "https": PROXY}

MOGI_CATEGORIES = {
    "Thuê": {
        "Nhà mặt tiền phố": "https://mogi.vn/thue-nha-mat-tien-pho",
        "Nhà biệt thự, liền kề": "https://mogi.vn/thue-nha-biet-thu-lien-ke",
        "Đường nội bộ": "https://mogi.vn/thue-duong-noi-bo",
        "Nhà hẻm ngõ": "https://mogi.vn/thue-nha-hem-ngo",
        "Căn hộ chung cư": "https://mogi.vn/thue-can-ho-chung-cu",
        "Căn hộ tập thể, cư xá": "https://mogi.vn/thue-can-ho-tap-the-cu-xa",
        "Căn hộ Penthouse": "https://mogi.vn/thue-can-ho-penthouse",
        "Căn hộ dịch vụ": "https://mogi.vn/thue-can-ho-dich-vu",
        "Căn hộ Officetel": "https://mogi.vn/thue-can-ho-officetel",
        "Phòng trọ, nhà trọ": "https://mogi.vn/thue-phong-tro-nha-tro",
        "Văn phòng": "https://mogi.vn/thue-van-phong",
        "Nhà xưởng, kho bãi": "https://mogi.vn/thue-nha-xuong-kho-bai-dat",
    }
}

def get_online_count(url):
    try:
        resp = requests.get(url, proxies=PROXIES, timeout=10, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        if resp.status_code != 200:
            return -1
        
        # Parse HTML to find total
        # Mogi usually has: <b>X</b> tin đặc biệt ... or "Hiện có 1.234 bất động sản..."
        # Or look for ".main-content .summary .count" ? 
        # Easier: Search regex for "Hiện có ([\d\.]+) bất động sản" or similar
        
        text = resp.text
        # Pattern: <b>1 - 15</b> trong <b>86.395</b>
        m = re.search(r'trong\s*<b>([\d\.]+)</b>', text)
        if m:
            return int(m.group(1).replace('.', ''))
            
        # Helper pattern for fallback
        m = re.search(r'<b>([\d\.]+)</b>\s*tin', text)
        if m:
             return int(m.group(1).replace('.', ''))
             
        # Fallback 3: look for "trong 86.395" without bold if text parsed differently
        m = re.search(r'trong\s*([\d\.]+)\s*(\n|<)', text)
        if m:
             val = m.group(1).replace('.', '')
             if val.isdigit():
                 return int(val)

        return 0
    except Exception as e:
        print(f"Error checking {url}: {e}")
        return -1

def check_completeness():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print(f"{'CATEGORY':<30} | {'ONLINE TOTAL':<12} | {'DB COUNT':<10} | {'PROGRESS':<8}")
    print("-" * 70)
    
    total_online = 0
    total_db = 0
    
    cats = MOGI_CATEGORIES["Thuê"]
    for name, url in cats.items():
        # Get DB Count
        # We assume trade_type='thuê'
        cursor.execute("SELECT COUNT(*) FROM collected_links WHERE loaihinh = %s AND trade_type = 'thuê'", (name,))
        db_count = cursor.fetchone()[0]
        
        # Get Online Count
        online = get_online_count(url)
        
        online_str = f"{online:,}" if online >= 0 else "Error"
        progress = f"{(db_count / online * 100):.1f}%" if online > 0 else "N/A"
        
        if online > 0:
            total_online += online
        total_db += db_count
        
        print(f"{name[:30]:<30} | {online_str:<12} | {db_count:<10} | {progress:<8}")
        
    print("-" * 70)
    print(f"{'TOTAL':<30} | {total_online:<12} | {total_db:<10} | {(total_db/total_online*100):.1f}%")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_completeness()
