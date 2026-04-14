
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import json
import time

def main():
    profile_dir = os.path.abspath("./crawlvip_profile")
    target_url = "https://batdongsan.com.vn/nguoi-ban/dang-tin" # Go to authenticated page
    
    api_base = "https://sellernetapi.batdongsan.com.vn/api/common"
    endpoints = [
        f"{api_base}/fetchCityList",
        f"{api_base}/fetchDistrictList?cityCode=SG",
        f"{api_base}/fetchProvinceList",
        f"{api_base}/GetAllCities"
    ]
    
    try:
        print(f"Using Persistent Profile: {profile_dir}")
        with SB(uc=True, headless=False, page_load_strategy="eager", user_data_dir=profile_dir) as sb:
            print("Opening Dashboard...")
            sb.open(target_url)
            print("Waiting 10s for Auth/Cloudflare...")
            sb.sleep(10)
            
            # Check if logged in (url redirect?)
            curr_url = sb.get_current_url()
            print(f"Current URL: {curr_url}")
            
            for api_url in endpoints:
                print(f"\nFetching: {api_url}")
                # Single line fetch
                js_code = f"return fetch('{api_url}', {{ headers: {{ 'accept': 'application/json' }} }}).then(r => r.json()).catch(e => {{ return {{error: e.toString()}} }});"
                
                result = sb.execute_script(js_code)
                
                if isinstance(result, list) and len(result) > 0:
                     print("SUCCESS! Got List.")
                     print(json.dumps(result[:3], indent=2, ensure_ascii=False))
                     
                     # Save to file if it is City List
                     if "fetchCityList" in api_url or "Province" in api_url:
                         fname = "bds_cities.json"
                         with open(fname, "w") as f:
                             json.dump(result, f, ensure_ascii=False, indent=2)
                         print(f"Saved to {fname}")
                         
                elif isinstance(result, dict) and 'error' not in result:
                     print("SUCCESS! Got Dict.")
                     # Check if dict has data
                     print(json.dumps(result, indent=2, ensure_ascii=False)[:300])
                else:
                     print(f"Result: {result}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
