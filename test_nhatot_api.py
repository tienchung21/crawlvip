import requests
import json

url = "https://gateway.chotot.com/v1/public/ad-listing?cg=1000&region_v2=13000&limit=1&o=0&st=s,k&key_param_included=true&include_expired_ads=true"
headers = {
    "accept": "application/json;version=1",
    "origin": "https://www.nhatot.com",
    "referer": "https://www.nhatot.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

resp = requests.get(url, headers=headers, timeout=20)
print("status:", resp.status_code)

try:
    data = resp.json()
except Exception as exc:
    print("json error:", exc)
    print(resp.text[:500])
    raise

items = data.get("ads", [])
print("items:", len(items))

if items:
    item = items[0]
    fields = [
        "account_id","account_name","ad_id","area","area_name","area_v2","body",
        "category","category_name","company_ad","floors","full_name","house_type",
        "images","latitude","longitude","price","price_string","region","region_name",
        "region_v2","rooms","ward","ward_name","subject","size","size_unit_string"
    ]
    out = {k: item.get(k) for k in fields}
    print(json.dumps(out, ensure_ascii=False, indent=2))
