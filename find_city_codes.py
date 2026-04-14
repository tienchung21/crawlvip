
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import sys

def main():
    target_url = "https://batdongsan.com.vn/"
    try:
        print("Starting Code Scan...")
        with SB(uc=True, headless=False, page_load_strategy="eager") as sb:
            sb.open(target_url)
            print("Page Loaded. Scanning JS...")
            sb.sleep(10)
            
            # 1. Get List of Scripts
            js_files = sb.execute_script("return window.performance.getEntriesByType('resource').filter(r => r.initiatorType === 'script').map(r => r.name);")
            print(f"Found {len(js_files)} Scripts.")
            
            found_data = False
            
            # 2. Fetch and Search
            for js_url in js_files:
                if ".js" not in js_url: continue
                # Skip external analytics (google, facebook, etc)
                if "batdongsan.com.vn" not in js_url and "staticfile" not in js_url: 
                    continue

                # Fetch content via JS fetch
                try:
                    # Simple heuristic check
                    # We look for "Đồng Nai" and "dna" nearby? Or just strictly containing both.
                    check_code = f"""
                        return fetch('{js_url}').then(r => r.text()).then(t => {{
                            if (t.includes('Đồng Nai') && t.includes('dna')) {{
                                return t;
                            }}
                            return null;
                        }}).catch(e => null);
                    """
                    content = sb.execute_script(check_code)
                    
                    if content:
                        print(f"\n[MATCH FOUND] in {js_url}")
                        # Extract the mapping part
                        # It's likely a JSON object or array.
                        # Dump a snippet around 'Đồng Nai'
                        idx = content.find("Đồng Nai")
                        start = max(0, idx - 100)
                        end = min(len(content), idx + 200)
                        print("Snippet:")
                        print(content[start:end])
                        found_data = True
                        break # Stop after finding likely map
                        
                except Exception as e:
                    pass

            if not found_data:
                print("No obvious city mapping found in loaded scripts.")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
