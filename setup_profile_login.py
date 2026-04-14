
import os
os.environ['no_proxy'] = '*'
if 'http_proxy' in os.environ: del os.environ['http_proxy']
if 'https_proxy' in os.environ: del os.environ['https_proxy']

from seleniumbase import SB
import time

def main():
    profile_dir = os.path.abspath("./crawlvip_profile")
    login_url = "https://batdongsan.com.vn/nguoi-ban/dang-nhap"
    
    print(f"Setting up Persistent Profile at: {profile_dir}")
    
    try:
        # Use user_data_dir to create persistent profile
        with SB(uc=True, headless=False, page_load_strategy="eager", user_data_dir=profile_dir) as sb:
            print("Opening Login Page...")
            sb.open(login_url)
            
            print("\n*** ACTION REQUIRED ***")
            print("Please LOGIN to your account manually in the browser window.")
            print("I will wait 3 minutes (180s)...")
            
            sb.sleep(180)
            
            print("Time's up! Closing Browser.")
            print("Profile saved. Use this profile for future API calls.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
