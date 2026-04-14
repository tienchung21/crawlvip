
from curl_cffi import requests
import json
import time
from pathlib import Path

CITY_CODES = {
    "Bình Dương": "BD"
}

class FullDataCrawler:
    """Crawler đầy đủ tất cả dữ liệu từ batdongsan.com.vn"""
    
    SELLER_API = "https://sellernetapi.batdongsan.com.vn/api"
    
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Referer': 'https://batdongsan.com.vn/',
            'apiversion': '2020-02-28 18:30',
            'sellernet-origin': 'tablet',
            'uniqueid': 'deviceidfromweb',
        }
    
    def _request(self, url, description=""):
        try:
            response = self.session.get(url, headers=self.headers, impersonate="chrome124", timeout=30)
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            print(f"Error: {e}")
            return []

    def _extract_list(self, payload, keys=None):
        if isinstance(payload, list): return payload
        if isinstance(payload, dict):
            for key in ("items", "list", "results", "districts", "wards", "streets", "districtList", "wardList"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
            if keys:
                for k in keys:
                    if k in payload and isinstance(payload[k], list): return payload[k]
        return []
    
    def get_districts(self, city_code):
        url = f"{self.SELLER_API}/common/fetchDistrictList?cityCode={city_code}"
        return self._extract_list(self._request(url), keys=("districtList",))
    
    def get_wards(self, district_id):
        url = f"{self.SELLER_API}/common/fetchWardList?districtId={district_id}"
        return self._extract_list(self._request(url), keys=("wardList",))
    
    def crawl_full_data(self, output_file="binhduong_full.json"):
        print("Crawling Binh Duong...")
        full_data = []
        
        for name, code in CITY_CODES.items():
            print(f"City: {name} ({code})")
            districts = self.get_districts(code)
            print(f"  Districts: {len(districts)}")
            
            city_data = {'city_name': name, 'city_code': code, 'districts': []}
            
            for d in districts:
                d_id = d.get('id') or d.get('districtId')
                d_name = d.get('name') or d.get('districtName')
                print(f"    District: {d_name} ({d_id})")
                
                wards = self.get_wards(d_id)
                print(f"      Wards: {len(wards)}")
                
                # We only need Wards for Merge Test (Streets optional)
                
                d_data = {
                    'district_info': d,
                    'wards': wards,
                    'streets': [],
                    'projects': []
                }
                city_data['districts'].append(d_data)
                time.sleep(0.1)
                
            full_data.append(city_data)
            
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(full_data, f, ensure_ascii=False, indent=2)
        print("Done. Saved to", output_file)

if __name__ == "__main__":
    crawler = FullDataCrawler()
    crawler.crawl_full_data()
