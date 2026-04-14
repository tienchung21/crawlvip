
import sys
import os
import json
import time
import random
import requests

# Add parent directory to path to import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from craw.database import Database
except ImportError:
    # Handle running from different directories
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from database import Database

JSON_FILE = "/home/chungnt/crawlvip/mogi_cities.json"

# Headers provided by user
HEADERS = {
    'authority': 'pro.mogi.vn',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
    'cache-control': 'no-cache',
    'cookie': '_gcl_au=1.1.1138560887.1767695053; _fbp=fb.1.1767695053543.710685843711188987; _hjSessionUser_3876449=eyJpZCI6IjI1Nzk5MjAyLTY3ZTQtNWQ3NS1iZGMzLTYxYzA3NmM2NjljNiIsImNyZWF0ZWQiOjE3Njc2OTUwNTM0OTksImV4aXN0aW5nIjp0cnVlfQ==; _gid=GA1.2.594820299.1768968225; .ASKAUTH=eyJhbGciOiJkaXIiLCJlbmMiOiJBMTI4Q0JDLUhTMjU2IiwidHlwIjoiSldUIn0..G2vEd-Ei9O32VH7dP6zqlQ.bWCk7TjyyopD2X1BGVvGZAXyDQ0Q3Qu_ZYW-JocR47v__sZqf9ffjyM891kwexcz43w9xkATgf2xFEfDBtGtF0eMxSn0sEmwBjJyCkk1KK2OOd0OecyUPagDPmPPPkmizwWrJOnah2vivFW6vp0Wl2BPi51f1sczKHwWF1gdOGDAmtklLPvoUgf5b9TurNQzdlfTLOL7DPfqsx_2ACE3C5vJtHX9kod1U6XQ1MvMnxXNDoF_lKQFpYzshn-TTeNWX6d3hfiN9IHzyRKa9CzecVQWeeFIvcNp7mT5jwIZO-g_0AVtqsnluWZJzcg35fTVeRsR6ARLrNKls1TviiozWHJmjCm016nmXwf-o_BS2CQiuTuxSjAcjwTFQ7LOeup64T8fuKJ9WQO1-rMyOjnZubWX3WUqTAHbu0-bg3ZFnDpDh7eLC-miPgfnb-ASL6Kh3Vdc8qW7qzIcXQGcxgWxfmoizYAjlFuV7CeYXpPTKvGn4M2jwKMyUy55EQPW0l56Evl2LdSd5yp9nOcXSbjk_Mqfe6RQscjR_wjnkIJ5cIDY4PmmOeVGoMuyoyp--WiYtxlUqkLdORI8cDEiWNaakIsgT2OB7WK9uIaGFy_c8wpGJYhBThX6Lk0OUXQwuOslsYOI7dlSlbTcEj6WYhHh4siPJ2ixDhR_wPTT-QQ08ROLbWMouU05b4Wc02adkAEB6iUntySVJVx-0-by85O4TqRjROMPyl8qgcxiTDkflbL8MrtCej3ipp6RO82Sg5HLdG9veo7XltgzlMW69EyWeU0AcZgZzj7EYgzmWjLHjRo.tebp5fgk-HmE1_ReUwvmHA; _ga=GA1.3.1658612326.1767695053; _gid=GA1.3.594820299.1768968225; __gads=ID=7cdaad209e3e6e08:T=1767695215:RT=1769072789:S=ALNI_MZnF-uX-2F-zD7_tomkQogzW5agpg; __gpi=UID=000011ddbc0debc3:T=1767695215:RT=1769072789:S=ALNI_MYCfEgJeru2Pw_I2E9bdNiMkQ8OcA; __eoi=ID=c190deb79681c1e4:T=1767695215:RT=1769072789:S=AA-AfjbyBBHXW1Z5nf2_zkzdBQnB; FCCDCF=%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%22dfa27511-8783-4f4e-ac63-8b316e17ff41%5C%22%2C%5B1767695054%2C39000000%5D%5D%22%5D%5D%5D; FCNEC=%5B%5B%22AKsRol8-fwXDKrvuUpu23x-3pVhuwp_mfmAsjzbqHA3xRB5QpzllVV6tdf8otCcp9njk2aXDkMSQjuGJmCqU2tSKjUjLpwBLTd_tlUUEqkcK9nyndljdIxf6rdKIm3-GykGEGT86oxIKUaiaX8Q7o73Is15Z9qDenw%3D%3D%22%5D%5D; ASP.NET_SessionId=ez2z2swmru2kobz1t2djwfwi; _hjSession_3876449=eyJpZCI6ImQ1OGFlZTUzLWM0NDAtNGFmMC05NDJlLWY3ZDAzNTBlZmEwYSIsImMiOjE3NjkwODAyMTUwNTEsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; _ga=GA1.2.1658612326.1767695053; .MOGIAUTH=yEE_JNk3SfANPfkKBnDCW7rBT0o-6yMC6_ItMz8bd4vj2cFKl1o_sDV6VIm9jFp4UVqWEsRGJS02ge3eX01QuH2_OguPOD0rADmNq7m9J4bKgea10TWDPP4YDkZFvOyF0eauFDh4DGJKJcy57xhuQhfRJFxlw82lhK50veQD6fkSfljFZlfJq38lutyal-fiUGpXAZPGSizhexhnuDo9umc6m12PLDywO9ZSoINBB0bgoQxOfTo9S-Cbl-lc-LJ65p083th1Cm-PqsPcFgGWkEHwmqhSwyZO7V_qrZZgUgFCbIulCCMybY8Iw9aQPUDq4QHpOi0X5Qf_aavF7e1LqWPSoFydu-UITuoHoX_vgvgPnJmMIj7ATviZw77adRuGwOkXhTU47bz8JW1sxmNF_CvCwGCEWiEN9t8B34BzEjJphNZL39mbW8v3KeRNyLxWRP4EP4OriTxgiuKa-_YIHNv39zfSzMgPUQ_Mfxzr-vXGumBGFE_m5gRfnohcZrYyPaiaIWLeREUxlnIJTYZXL37nT4TYlNSerqHUAFDceQy16h0d6MQLXv9oshGQElQ5ivAjBmxPPtgeBc1T1_v1MAKF_XxBFgFSp7nX-WjjEoIRy4LxuGWrNQ-C-VNwf_BsBCCoZV49WS0nW-TBLf_FQcmZeismCdHrqgkYZVBcmkO3KbkYAlfIQ2JjKhXPHwpsnmkh1qtq68ITJ8EBpD16YsguCp4gHPGif3QCeiy6zwWZ4ZP5BVyMLyfeJC9Fy-IJ7QqaG2RPVfrsY2aIOw8xdQ; _ga_EPTMT9HK3X=GS2.1.s1769080214$o20$g1$t1769080270$j4$l0$h0; _dc_gtm_UA-52097568-1=1',
    'pragma': 'no-cache',
    'referer': 'https://pro.mogi.vn/Property/Posting/0',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'x-aspnetmvc-version': '5.2',
    'x-serv': '126'
}

def sync_data():
    print("=== STARTING MOGI LOCATION SYNC (ALL PROVINCES) ===\n")
    
    if not os.path.exists(JSON_FILE):
        print(f"Error: JSON file not found at {JSON_FILE}")
        return

    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        cities = data.get('data', [])
        
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        print(f"Found {len(cities)} provinces to process.")
        
        for city_idx, city in enumerate(cities):
            c_name = city.get('n')
            c_id = city.get('i')
            c_slug = city.get('u')
            c_code = city.get('co')
            
            print(f"\n[{city_idx+1}/{len(cities)}] Processing: {c_name} (ID: {c_id})")
            
            # Insert City
            cursor.execute("""
                INSERT INTO location_mogi (mogi_id, parent_id, name, slug, type, code)
                VALUES (%s, 0, %s, %s, 'CITY', %s)
                ON DUPLICATE KEY UPDATE name=VALUES(name), slug=VALUES(slug), code=VALUES(code)
            """, (c_id, c_name, c_slug, c_code))
            
            districts = city.get('c', [])
            print(f"   Found {len(districts)} districts.")
            
            for dist in districts:
                d_name = dist.get('n')
                d_id = dist.get('i')
                d_slug = dist.get('u')
                d_code = dist.get('co')
                
                cursor.execute("""
                    INSERT INTO location_mogi (mogi_id, parent_id, name, slug, type, code)
                    VALUES (%s, %s, %s, %s, 'DISTRICT', %s)
                    ON DUPLICATE KEY UPDATE name=VALUES(name), slug=VALUES(slug), code=VALUES(code), parent_id=VALUES(parent_id)
                """, (d_id, c_id, d_name, d_slug, d_code))
            
            conn.commit()
            
            # Sync Wards and Streets for this city's districts
            for idx, dist in enumerate(districts):
                d_id = dist.get('i')
                d_name = dist.get('n')
                print(f"   [{idx+1}/{len(districts)}] {d_name} ({d_id})...", end=" ")
                
                # Delay to avoid rate limiting (speedup x3)
                time.sleep(random.uniform(0.5, 1.0))
                
                ward_count = 0
                street_count = 0
                
                # Fetch Wards
                try:
                    url_ward = f"https://pro.mogi.vn/City/GetWardByDistrict?districtId={d_id}"
                    resp_ward = requests.get(url_ward, headers=HEADERS, timeout=15)
                    
                    if resp_ward.status_code == 200:
                        ward_data = resp_ward.json()
                        WARDS = ward_data.get('Data', [])
                        ward_count = len(WARDS)
                        for w in WARDS:
                            w_id = w.get('WardId')
                            w_name = w.get('Name')
                            w_slug = w.get('CodeUrl')
                            w_code = w.get('Code')
                            
                            cursor.execute("""
                                INSERT INTO location_mogi (mogi_id, parent_id, name, slug, type, code)
                                VALUES (%s, %s, %s, %s, 'WARD', %s)
                                ON DUPLICATE KEY UPDATE name=VALUES(name), slug=VALUES(slug), code=VALUES(code), parent_id=VALUES(parent_id)
                            """, (w_id, d_id, w_name, w_slug, w_code))
                except Exception as e:
                    print(f"[Ward ERR: {e}]", end=" ")

                # Fetch Streets
                try:
                    url_street = f"https://pro.mogi.vn/City/GetStreetByDistrict?districtId={d_id}"
                    resp_street = requests.get(url_street, headers=HEADERS, timeout=15)
                    
                    if resp_street.status_code == 200:
                        street_data = resp_street.json()
                        STREETS = street_data.get('Data', [])
                        street_count = len(STREETS)
                        for s in STREETS:
                            s_id = s.get('StreetId')
                            s_name = s.get('Name')
                            
                            cursor.execute("""
                                INSERT INTO location_mogi (mogi_id, parent_id, name, slug, type, code)
                                VALUES (%s, %s, %s, NULL, 'STREET', NULL)
                                ON DUPLICATE KEY UPDATE name=VALUES(name), parent_id=VALUES(parent_id)
                            """, (s_id, d_id, s_name))
                except Exception as e:
                    print(f"[Street ERR: {e}]", end=" ")
                    
                print(f"W:{ward_count} S:{street_count}")
                conn.commit()

        print("\n=== SYNC COMPLETED ===")

    except Exception as e:
        print(f"Sync failed: {e}")
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    sync_data()
