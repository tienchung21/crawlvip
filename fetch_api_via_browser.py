
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import json

def main():
    target_url = "https://batdongsan.com.vn/"
    api_base = "https://sellernetapi.batdongsan.com.vn/api/common"
    
    # Guessing endpoints based on user input (fetchDistrictList)
    candidates = [
        f"{api_base}/fetchCityList",
        f"{api_base}/fetchProvinceList",
        f"{api_base}/GetAllCities" 
    ]
    
    try:
        print("Starting Browser API Fetch...")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            sb.open(target_url)
            print("Page Loaded. Waiting 5s...")
            sb.sleep(5)
            
            for api_url in candidates:
                print(f"\nTrying Fetch: {api_url}")
                # JS Fetch
                js_code = f"return fetch('{api_url}', {{ headers: {{ 'accept': 'application/json' }} }}).then(r => r.json()).catch(e => {{ return {{error: e.toString()}} }});"
                
                result = sb.execute_script(js_code)
                print(f"Result Type: {type(result)}")
                
                if isinstance(result, list) and len(result) > 0:
                     print("SUCCESS! Got List.")
                     print(json.dumps(result[:3], indent=2, ensure_ascii=False))
                     # If success, maybe save it?
                     with open("bds_locations.json", "w") as f:
                         json.dump(result, f, ensure_ascii=False, indent=2)
                     break
                elif isinstance(result, dict) and 'error' not in result:
                     print("SUCCESS! Got Dict.")
                     print(json.dumps(result, indent=2, ensure_ascii=False)[:300])
                else:
                     print(f"Failed: {result}")
                     
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
