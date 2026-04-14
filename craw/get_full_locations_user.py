
from curl_cffi import requests
import json
import time
from pathlib import Path
# from city_codes import CITY_CODES, POPULAR_CITIES 

class FullDataCrawler:
    """Crawler đầy đủ tất cả dữ liệu từ batdongsan.com.vn"""
    
    SELLER_API = "https://sellernetapi.batdongsan.com.vn/api"
    
    def __init__(self):
        self.session = requests.Session()
        
        # Headers chuẩn cho SellerAPI
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://batdongsan.com.vn',
            'Referer': 'https://batdongsan.com.vn/',
            'Sec-Ch-Ua': '"Chromium";v="144", "Not_A Brand";v="8"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            # 3 headers quan trọng
            'apiversion': '2020-02-28 18:30',
            'sellernet-origin': 'tablet',
            'uniqueid': 'deviceidfromweb',
        }
    
    def _request(self, url, description=""):
        """Helper method để request với error handling"""
        try:
            response = self.session.get(
                url,
                headers=self.headers,
                impersonate="chrome124",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"    ❌  Error {response.status_code}: {description}")
                return []
        except Exception as e:
            print(f"    ⚠️ Exception: {description} - {e}")
            return []

    def _extract_list(self, payload, keys=None):
        """Normalize API payloads that may wrap lists inside dicts."""
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if keys:
                for key in keys:
                    value = payload.get(key)
                    if isinstance(value, list):
                        return value
            data = payload.get("data")
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                if keys:
                    for key in keys:
                        value = data.get(key)
                        if isinstance(value, list):
                            return value
                for key in ("items", "list", "results"):
                    value = data.get(key)
                    if isinstance(value, list):
                        return value
            for value in payload.values():
                if isinstance(value, list):
                    return value
        return []
    
    def get_districts(self, city_code):
        """Lấy danh sách quận/huyện"""
        url = f"{self.SELLER_API}/common/fetchDistrictList?cityCode={city_code}"
        payload = self._request(url, f"districts for {city_code}")
        return self._extract_list(payload, keys=("districts", "districtList", "items", "results"))
    
    def get_wards(self, district_id):
        """Lấy danh sách phường/xã"""
        url = f"{self.SELLER_API}/common/fetchWardList?districtId={district_id}"
        payload = self._request(url, f"wards for district {district_id}")
        return self._extract_list(payload, keys=("wards", "wardList", "items", "results"))
    
    def get_streets(self, city_code, district_id):
        """Lấy danh sách đường phố"""
        url = f"{self.SELLER_API}/common/fetchStreetList?cityCode={city_code}&districtId={district_id}"
        payload = self._request(url, f"streets for district {district_id}")
        return self._extract_list(payload, keys=("streets", "streetList", "items", "results"))
    
    def get_projects(self, city_code, district_id):
        """Lấy danh sách dự án"""
        url = f"{self.SELLER_API}/project/fetchProjectList?cityCode={city_code}&districtId={district_id}"
        payload = self._request(url, f"projects for district {district_id}")
        return self._extract_list(payload, keys=("projects", "projectList", "items", "results"))
    
    def crawl_full_data(self, 
                       output_file="batdongsan_full_data.json",
                       cities_list=None,
                       popular_only=True,
                       limit_cities=None):
        """
        Crawl toàn bộ dữ liệu: cities → districts → wards → streets → projects
        """
        
        print("="*80)
        print("🚀 CRAWL TOÀN BỘ DỮ LIỆU BATDONGSAN.COM.VN")
        print("="*80)
        
        # Xác định danh sách cities cần crawl
        # Placeholder for CITY_CODES
        CITY_CODES = {} # NEED TO FILL
        
        if cities_list:
            cities_to_crawl = cities_list
        # elif popular_only:
        #     cities_to_crawl = POPULAR_CITIES
        else:
            cities_to_crawl = list(CITY_CODES.keys())
        
        if limit_cities:
            cities_to_crawl = cities_to_crawl[:limit_cities]
        
        print(f"\n📋 Sẽ crawl {len(cities_to_crawl)} tỉnh/thành:")
        for city in cities_to_crawl:
            print(f"   {city} ({CITY_CODES[city]})")
        
        print("\n" + "="*80)
        
        # Data structure
        full_data = []
        
        # Crawl từng city
        for idx, city_name in enumerate(cities_to_crawl, 1):
            city_code = CITY_CODES[city_name]
            
            print(f"\n{'='*80}")
            print(f"🌆 [{idx}/{len(cities_to_crawl)}] {city_name} ({city_code})")
            print(f"{'='*80}")
            
            city_data = {
                'city_name': city_name,
                'city_code': city_code,
                'districts': []
            }
            
            # Lấy districts
            print(f"  🏢 Đang lấy districts...")
            districts = self.get_districts(city_code)
            print(f"  ✓ Có {len(districts)} quận/huyện")
            
            # Crawl từng district
            for d_idx, district in enumerate(districts, 1):
                if not isinstance(district, dict):
                    print(f"    ⚠️  Unexpected district type ({type(district).__name__}); skipping")
                    continue
                dist_name = district.get('name') or district.get('districtName') or 'N/A'
                dist_id = district.get('id') or district.get('districtId')
                if dist_id is None:
                    print(f"    ⚠️  Missing district id for {dist_name}; skipping")
                    continue
                
                print(f"\n    [{d_idx}/{len(districts)}] {dist_name} (ID: {dist_id})")
                
                district_data = {
                    'district_info': district,
                    'wards': [],
                    'streets': [],
                    'projects': []
                }
                
                # Lấy wards
                print(f"       Wards...", end=" ")
                wards = self.get_wards(dist_id)
                district_data['wards'] = wards
                print(f"→ {len(wards)}")
                
                time.sleep(0.1)
                
                # Lấy streets
                print(f"       Streets...", end=" ")
                streets = self.get_streets(city_code, dist_id)
                district_data['streets'] = streets
                print(f"→ {len(streets)}")
                
                time.sleep(0.1)
                
                # Lấy projects
                print(f"       Projects...", end=" ")
                projects = self.get_projects(city_code, dist_id)
                district_data['projects'] = projects
                print(f"→ {len(projects)}")
                
                city_data['districts'].append(district_data)
                
                # Rate limiting giữa các districts
                time.sleep(0.2)
            
            full_data.append(city_data)
            
            # Save progress sau mỗi city
            print(f"\n  💾 Saving progress...")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(full_data, f, ensure_ascii=False, indent=2)
            print(f"  ✓ Saved to {output_file}")
            
            # Rate limiting giữa các cities
            time.sleep(1)
        
        # Thống kê cuối cùng
        print("\n" + "="*80)
        print("✅ HOÀN THÀNH!")
        print("="*80)
        
        return full_data

def main():
    crawler = FullDataCrawler()
    # Need to setup CITY_CODES first.
