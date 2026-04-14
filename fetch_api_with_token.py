
from curl_cffi import requests
import json

def main():
    endpoints = [
        "https://sellernetapi.batdongsan.com.vn/api/common/fetchCityList",
        "https://sellernetapi.batdongsan.com.vn/api/common/fetchProvinceList",
        "https://sellernetapi.batdongsan.com.vn/api/common/GetAllCities",
        "https://sellernetapi.batdongsan.com.vn/api/master/fetchCityList",
        # Test the one known working for user
        "https://sellernetapi.batdongsan.com.vn/api/common/fetchDistrictList?cityCode=SG"
    ]

    # Cleaned cookie string (removed newlines manually if any)
    cookie_str = """_ga=GA1.1.205476812.1758853035; _tt_enable_cookie=1; _ttp=01K61WYYP51ZQH10FC483E0916_.tt.2; __uidac=0168d5f7ace0f36ae9ac7c6a9a9d3cc5; __iid=6461; _hjSessionUser_1708983=eyJpZCI6IjYyMjJhNjViLWVmNzYtNWVjNS05NzhkLWY2OTE1ZDQ5NWRjNyIsImNyZWF0ZWQiOjE3NTg4NTMwMzY1OTIsImV4aXN0aW5nIjp0cnVlfQ==; _fbp=fb.2.1758853046694.975898389715210531; _gcl_gs=2.1.k1$i1764902240$u215423364; _gcl_aw=GCL.1764903516.Cj0KCQiA_8TJBhDNARIsAPX5qxQqNrtoW2peosBJYMqYfhbEvX6ZwDK_xvr9cOk3m2RcVpUM7PS9hrMaAryWEALw_wcB; _gcl_au=1.1.1221022750.1766643360; __admUTMtime=1767579238; c_u_id=4925828; ajs_user_id=4925828; ajs_anonymous_id=65d670e2-ffec-4f26-9b21-5d4dcc6c7230; __su=0; __utm=source%3D1125_1225_BRANDC_B_PG_WEBSITEBDS%7Cmedium%3DBANNER; _clck=c0ycbh%5E2%5Eg33%5E0%5E2095; _hjSession_1708983=eyJpZCI6ImM1NTdjMzYxLWJiOWQtNGM4Ni04N2FjLTkwNWU1YTliMTBjNiIsImMiOjE3Njk1ODcyMzQwNTAsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=; refreshToken=fvJQCeuJygBiq0q5aqphsxRkM85uNZh8Zj8Yec_-UZw; accessToken=eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg5Mzg0OTU1MkNDRTExMUFDMjc5RjUyNDI3RUEwMUY5QzdDMzAxNTQiLCJ0eXAiOiJhdCtqd3QiLCJ4NXQiOiJpVGhKVlN6T0VSckNlZlVrSi1vQi1jZkRBVlEifQ.eyJuYmYiOjE3Njk1OTEwNzgsImV4cCI6MTc2OTU5NDY3OCwiaXNzIjoiaHR0cDovL2F1dGhlbnRpY2F0aW9uLmJkcy5sYyIsImF1ZCI6IkFwaUdhdGV3YXkiLCJjbGllbnRfaWQiOiIwM2QwZjkwNS0xMGM5LTRkMjEtOTBmNS0xMGI3OGUwYTk4OWMiLCJzdWIiOiI0OTI1ODI4IiwiYXV0aF90aW1lIjoxNzY3NzExODU0LCJpZHAiOiJsb2NhbCIsInByZWZlcnJlZF91c2VybmFtZSI6IjAzODQ3Mjc2OTYiLCJlbWFpbCI6IjEwMWJlZjIxZGU0NDQ2YzE4ZWIxNGZmZjRhMTNiN2FhQGJkcy5sYyIsInNjb3BlIjpbIm9wZW5pZCIsInByb2ZpbGUiLCJBcGlHYXRld2F5Iiwib2ZmbGluZV9hY2Nlc3MiXSwiYW1yIjpbInB3ZCJdfQ.WFo-4zl2OqmMV0U0309KqPD03ajIp9rXjgK5dLoafyvveTTYXxvK08IljV0cQ-f7oAyBEwh7mg6e_H2TeCvM9EJXk6RhylAopOu7-jFHIRCNrl7QPPXO2VTpifBNlaP-1NvE5EAo8IqrjBnAVO0UOK7e63DmYyv5Svp2bDRVnD7Y7WUXv7MWqpzCl21-Os9EqdWw3Xt5UJPaY4Uy1HJSFFsXeIsORfKRmJne3WWri2LJx-o04LZb5r3sn_Tor63QcsQsY5uhhxMUyHD230MNt2IJkYfV9oULoTyOY2c59wx8ihIxPjdESFZ2Zr_jND3g4y1uwTdIbgD61aGKqOnI8A; __gads=ID=e5db77d8302d6cfc:T=1758853073:RT=1769593637:S=ALNI_Ma1RIbBC_K86l56ZJ2E3hN90aQjWg; __gpi=UID=0000119b0079cb37:T=1758853073:RT=1769593637:S=ALNI_MZ47GMgFvIQJvk2ghfqX3QqqvrOFg; __eoi=ID=b182f19e242cba51:T=1758853073:RT=1769593637:S=AA-AfjZUqUUx1CRnigOwT7buPXl8; cf_clearance=ipsSMKz77L4IRUa_.q8RBciet.2s4i2kxLqzzeO3eFc-1769593637-1.2.1.1-LhitAVDenVP7EIkqIYLlgJqiOn_d6LuyOIRBIp2VeNwbxTdA3VEzUO4A4.jXQXmWqqxLoYKLYB4ctrVvvzLCoidqdQJHuNsmBaB4g6dD_HpOSSkpgSVLd7TpcWv8.1qjd3NSrMGNPJtIg_GojT2U177ewYBXf0By0HKPmdTX1sjx82TV81Q1ZQRoL79C7mgomNPJrjsrnd4v9vYLB0OrBoSULVFmxMRxZTs7V14Yowo; ab.storage.deviceId.892f88ed-1831-42b9-becb-90a189ce90ad=%7B%22g%22%3A%2286d5ab7f-e87f-ff3e-233e-749bf2b70473%22%2C%22c%22%3A1758853035723%2C%22l%22%3A1769593640447%7D; ab.storage.userId.892f88ed-1831-42b9-becb-90a189ce90ad=%7B%22g%22%3A%224925828%22%2C%22c%22%3A1767594891138%2C%22l%22%3A1769593640447%7D; ab.storage.sessionId.892f88ed-1831-42b9-becb-90a189ce90ad=%7B%22g%22%3A%22ebb5d692-b169-f792-9951-ff9e47529f27%22%2C%22e%22%3A1769595442793%2C%22c%22%3A1769593640444%2C%22l%22%3A1769593642793%7D; _ga_HTS298453C=GS2.1.s1769593639$o155$g1$t1769593650$j49$l0$h0$dK68UVaFWz8ven5sxkGunFnZzGYq8y8Fp7g; ttcsid=1769593640088::nsVUA_5CZFh-3AG1QAUm.129.1769593672143.0; ttcsid_CHHL1E3C77U1H95PSJM0=1769593640087::mA4i9zCZTf6rsUow59Qc.76.1769593672143.1; _cfuvid=WEIWSmPP3AfU43fGwVhZX4gK0cgvXEdbtYH.wjwMH7w-1769593926315-0.0.1.1-604800000; AWSALB=+FCj8s8yWCJHXBLMz1PQ0c0x1h2nfm5VQ1EVaTb8SOH2IHHOpxiVFjAP2IwNLgc5TpDUxsO6ie8w/uDz8r+h1GhAAUHGNW3xI3219hCCi9tqCPMKYK8cqlrJeXr8; AWSALBCORS=+FCj8s8yWCJHXBLMz1PQ0c0x1h2nfm5VQ1EVaTb8SOH2IHHOpxiVFjAP2IwNLgc5TpDUxsO6ie8w/uDz8r+h1GhAAUHGNW3xI3219hCCi9tqCPMKYK8cqlrJeXr8; _clsk=1vhcojj%5E1769594218054%5E17%5E0%5Ea.clarity.ms%2Fcollect"""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://batdongsan.com.vn/",
        "Origin": "https://batdongsan.com.vn",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Cookie": cookie_str
    }

    print("Testing APIs with Token...")
    
    for url in endpoints:
        print(f"\nGET {url}")
        try:
            r = requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
            print(f"Status: {r.status_code}")
            
            if r.status_code == 200:
                try:
                    data = r.json()
                    # print(f"Type: {type(data)}")
                    if isinstance(data, list):
                        print(f"Count: {len(data)}")
                        if len(data) > 0: print(json.dumps(data[0], ensure_ascii=False))
                    elif isinstance(data, dict):
                        print(json.dumps(data, indent=2, ensure_ascii=False)[:300]) 
                        
                except Exception as e:
                    print(f"Content: {r.text[:200]}")
            else:
                 print(f"Failed: {r.status_code}")
                 print(r.text[:200])
                 
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
