
"""
Crawl Ð?Y Ð?: Cities ? Districts ? Wards ? Streets ? Projects
Luu toàn b? vào JSON
"""

from curl_cffi import requests
import json
import time
from pathlib import Path
try:
    from city_codes import CITY_CODES, POPULAR_CITIES
except ImportError:
    # Placeholder if missing
    CITY_CODES = {
        "Hồ Chí Minh": "SG",
        "Hà Nội": "HN",
        "Đà Nẵng": "DN",
        "Bình Dương": "BD",
        "Đồng Nai": "DNA"
    }
    POPULAR_CITIES = ["Hồ Chí Minh", "Hà Nội"]

class FullDataCrawler:
    """Crawler d?y d? t?t c? d? li?u t? batdongsan.com.vn"""
    
    SELLER_API = "https://sellernetapi.batdongsan.com.vn/api"
    
    def __init__(self):
        self.session = requests.Session()
        
        # Headers chu?n cho SellerAPI
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
            # 3 headers quan tr?ng
            'apiversion': '2020-02-28 18:30',
            'sellernet-origin': 'tablet',
            'uniqueid': 'deviceidfromweb',
        }
    
    def _request(self, url, description=""):
        """Helper method d? request v?i error handling"""
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
                print(f"    ??  Error {response.status_code}: {description}")
                return []
        except Exception as e:
            print(f"    ? Exception: {description} - {e}")
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
        """L?y danh sách qu?n/huy?n"""
        url = f"{self.SELLER_API}/common/fetchDistrictList?cityCode={city_code}"
        payload = self._request(url, f"districts for {city_code}")
        return self._extract_list(payload, keys=("districts", "districtList", "items", "results"))
    
    def get_wards(self, district_id):
        """L?y danh sách phu?ng/xã"""
        url = f"{self.SELLER_API}/common/fetchWardList?districtId={district_id}"
        payload = self._request(url, f"wards for district {district_id}")
        return self._extract_list(payload, keys=("wards", "wardList", "items", "results"))
    
    def get_streets(self, city_code, district_id):
        """L?y danh sách du?ng ph?"""
        url = f"{self.SELLER_API}/common/fetchStreetList?cityCode={city_code}&districtId={district_id}"
        payload = self._request(url, f"streets for district {district_id}")
        return self._extract_list(payload, keys=("streets", "streetList", "items", "results"))
    
    def get_projects(self, city_code, district_id):
        """L?y danh sách d? án"""
        url = f"{self.SELLER_API}/project/fetchProjectList?cityCode={city_code}&districtId={district_id}"
        payload = self._request(url, f"projects for district {district_id}")
        return self._extract_list(payload, keys=("projects", "projectList", "items", "results"))
    
    def crawl_full_data(self, 
                       output_file="batdongsan_full_data.json",
                       cities_list=None,
                       popular_only=True,
                       limit_cities=None):
        """
        Crawl toàn b? d? li?u: cities ? districts ? wards ? streets ? projects
        
        Args:
            output_file: Tên file JSON output
            cities_list: List tên các t?nh/thành c?n crawl (None = t?t c?)
            popular_only: Ch? crawl t?nh/thành ph? bi?n
            limit_cities: Gi?i h?n s? lu?ng t?nh/thành
        """
        
        print("="*80)
        print("?? CRAWL TOÀN B? D? LI?U BATDONGSAN.COM.VN")
        print("="*80)
        
        # Xác d?nh danh sách cities c?n crawl
        if cities_list:
            cities_to_crawl = cities_list
        elif popular_only:
            cities_to_crawl = POPULAR_CITIES
        else:
            cities_to_crawl = list(CITY_CODES.keys())
        
        if limit_cities:
            cities_to_crawl = cities_to_crawl[:limit_cities]
        
        print(f"\n?? S? crawl {len(cities_to_crawl)} t?nh/thành:")
        for city in cities_to_crawl:
            print(f"   {city} ({CITY_CODES[city]})")
        
        print("\n" + "="*80)
        
        # Data structure
        full_data = []
        
        # Crawl t?ng city
        for idx, city_name in enumerate(cities_to_crawl, 1):
            city_code = CITY_CODES[city_name]
            
            print(f"\n{'='*80}")
            print(f"?? [{idx}/{len(cities_to_crawl)}] {city_name} ({city_code})")
            print(f"{'='*80}")
            
            city_data = {
                'city_name': city_name,
                'city_code': city_code,
                'districts': []
            }
            
            # L?y districts
            print(f"  ?? Ðang l?y districts...")
            districts = self.get_districts(city_code)
            print(f"  ? Có {len(districts)} qu?n/huy?n")
            
            # Crawl t?ng district
            for d_idx, district in enumerate(districts, 1):
                if not isinstance(district, dict):
                    print(f"    ??  Unexpected district type ({type(district).__name__}); skipping")
                    continue
                dist_name = district.get('name') or district.get('districtName') or 'N/A'
                dist_id = district.get('id') or district.get('districtId')
                if dist_id is None:
                    print(f"    ??  Missing district id for {dist_name}; skipping")
                    continue
                
                print(f"\n    [{d_idx}/{len(districts)}] {dist_name} (ID: {dist_id})")
                
                district_data = {
                    'district_info': district,
                    'wards': [],
                    'streets': [],
                    'projects': []
                }
                
                # L?y wards
                print(f"       Wards...", end=" ")
                wards = self.get_wards(dist_id)
                district_data['wards'] = wards
                print(f"? {len(wards)}")
                
                time.sleep(0.1)
                
                # L?y streets
                print(f"       Streets...", end=" ")
                streets = self.get_streets(city_code, dist_id)
                district_data['streets'] = streets
                print(f"? {len(streets)}")
                
                time.sleep(0.1)
                
                # L?y projects
                print(f"       Projects...", end=" ")
                projects = self.get_projects(city_code, dist_id)
                district_data['projects'] = projects
                print(f"? {len(projects)}")
                
                city_data['districts'].append(district_data)
                
                # Rate limiting gi?a các districts
                time.sleep(0.2)
            
            full_data.append(city_data)
            
            # Save progress sau m?i city
            print(f"\n  ?? Saving progress...")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(full_data, f, ensure_ascii=False, indent=2)
            print(f"  ? Saved to {output_file}")
            
            # Rate limiting gi?a các cities
            time.sleep(1)
        
        # Th?ng kê cu?i cùng
        print("\n" + "="*80)
        print("? HOÀN THÀNH!")
        print("="*80)
        
        total_districts = sum(len(c['districts']) for c in full_data)
        total_wards = sum(
            len(d['wards']) 
            for c in full_data 
            for d in c['districts']
        )
        total_streets = sum(
            len(d['streets']) 
            for c in full_data 
            for d in c['districts']
        )
        total_projects = sum(
            len(d['projects']) 
            for c in full_data 
            for d in c['districts']
        )
        
        print(f"\n?? TH?NG KÊ:")
        print(f"    T?nh/thành: {len(full_data)}")
        print(f"    Qu?n/huy?n: {total_districts}")
        print(f"    Phu?ng/xã: {total_wards}")
        print(f"    Ðu?ng ph?: {total_streets}")
        print(f"    D? án: {total_projects}")
        
        print(f"\n?? D? LI?U:")
        print(f"    File: {output_file}")
        print(f"    Size: {Path(output_file).stat().st_size / 1024 / 1024:.2f} MB")
        
        return full_data


def main():
    crawler = FullDataCrawler()
    
    print("="*80)
    print("?? CH?N CH? Ð? CRAWL")
    print("="*80)
    print("""
    Option 1: Crawl 10 t?nh/thành PH? BI?N (recommended)
              ? Nhanh, d? dùng cho h?u h?t m?c dích
    
    Option 2: Crawl 3 t?nh/thành d?u tiên (test)
              ? Ð? test xem có ho?t d?ng không
    
    Option 3: Crawl T?T C? 63 t?nh/thành
              ? Lâu (~30-60 phút), d? li?u d?y d? nh?t
    
    Option 4: Crawl m?t s? t?nh/thành c? th?
              ? T? ch?n danh sách
    """)
    print("="*80)
    
    # ============================================================
    # CH?N M?T TRONG CÁC OPTION SAU (uncomment d? dùng)
    # ============================================================
    
    # OPTION 1: Crawl t?nh/thành ph? bi?n (RECOMMENDED)
    print("\n?? Ðang ch?y: Option 1 - T?nh/thành ph? bi?n\n")
    data = crawler.crawl_full_data(
        output_file="batdongsan_popular.json",
        popular_only=True
    )
    
    # OPTION 2: Test v?i 3 t?nh/thành d?u
    # data = crawler.crawl_full_data(
    #     output_file="batdongsan_test.json",
    #     limit_cities=3
    # )
    
    # OPTION 3: Crawl T?T C? 63 t?nh/thành
    # print("\n?? Ðang ch?y: Option 3 - T?T C? 63 t?nh/thành\n")
    # data = crawler.crawl_full_data(
    #     output_file="batdongsan_full.json",
    #     popular_only=False,
    #     limit_cities=None
    # )
    
    # OPTION 4: Crawl m?t s? t?nh/thành c? th?
    # my_cities = ["H? Chí Minh", "Hà N?i", "Ðà N?ng"]
    # data = crawler.crawl_full_data(
    #     output_file="batdongsan_custom.json",
    #     cities_list=my_cities
    # )
    
    print("\n? XONG! Ki?m tra file JSON d? xem d? li?u")


if __name__ == "__main__":
    main()
