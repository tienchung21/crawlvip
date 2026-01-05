"""
Script decrypt phone - User paste cookies từ browser vào đây

HƯỚNG DẪN:
1. Mở Chrome, đăng nhập batdongsan.com.vn
2. F12 → Network tab
3. Refresh trang
4. Click vào request đầu tiên
5. Tab Headers → Request Headers
6. Copy TOÀN BỘ dòng "cookie:" (rất dài)
7. Paste vào biến COOKIES_FROM_BROWSER bên dưới
8. Copy dòng "user-agent:"
9. Paste vào biến USER_AGENT_FROM_BROWSER
10. Chạy script: python decrypt_phone_manual.py
"""

from curl_cffi import requests
import sys

# ============================================================
# USER PASTE COOKIES VÀO ĐÂY (giữa dấu """ """)
# ============================================================
COOKIES_FROM_BROWSER = """
_ga=GA1.1.205476812.1758853035; _gcl_au=1.1.1327549992.1758853035; _tt_enable_cookie=1; _ttp=01K61WYYP51ZQH10FC483E0916_.tt.2; __uidac=0168d5f7ace0f36ae9ac7c6a9a9d3cc5; __iid=6461; __iid=6461; __RC=5; __R=3; __tb=0; _hjSessionUser_1708983=eyJpZCI6IjYyMjJhNjViLWVmNzYtNWVjNS05NzhkLWY2OTE1ZDQ5NWRjNyIsImNyZWF0ZWQiOjE3NTg4NTMwMzY1OTIsImV4aXN0aW5nIjp0cnVlfQ==; _fbp=fb.2.1758853046694.975898389715210531; __admUTMtime=1764582199; __utm=source%3D1125_1225_BRANDC_B_PG_WEBSITEBDS%7Cmedium%3DBANNER; __utm=source%3D1125_1225_BRANDC_B_PG_WEBSITEBDS%7Cmedium%3DBANNER; _gcl_gs=2.1.k1$i1764902240$u215423364; _gcl_aw=GCL.1764903516.Cj0KCQiA_8TJBhDNARIsAPX5qxQqNrtoW2peosBJYMqYfhbEvX6ZwDK_xvr9cOk3m2RcVpUM7PS9hrMaAryWEALw_wcB; __su=0; __su=0; c_u_id=4883613; ajs_user_id=4883613; ajs_anonymous_id=65d670e2-ffec-4f26-9b21-5d4dcc6c7230; __UF=-1; banner-below-cookie=2; desapp=sellernet01; NPS_b514e4e7_last_seen=1765253333283; _ga_HTS298453C=deleted; con.unl.lat=1765299600; con.unl.sc=23; _hjSession_1708983=eyJpZCI6ImM2ZjJmZTIzLTM0YzAtNDYxNy04MDI3LWM4MDg2NTQ4MmQ1ZiIsImMiOjE3NjUzNDk3MjgzNzUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; _clck=c0ycbh%5E2%5Eg1q%5E0%5E2095; con.ses.id=a5c04e4f-895b-4512-be15-cd47733e63f4; c_u_id=4883613; __IP=1906387364; __gads=ID=e5db77d8302d6cfc:T=1758853073:RT=1765357850:S=ALNI_Ma1RIbBC_K86l56ZJ2E3hN90aQjWg; __gpi=UID=0000119b0079cb37:T=1758853073:RT=1765357850:S=ALNI_MZ47GMgFvIQJvk2ghfqX3QqqvrOFg; __eoi=ID=b182f19e242cba51:T=1758853073:RT=1765357850:S=AA-AfjZUqUUx1CRnigOwT7buPXl8; _hjHasCachedUserAttributes=true; userinfo=chung9atm@gmail.com; BDS.UMS.Cookie=CfDJ8B4NEwUkr35PrdLijyPjhIk2mMGUsoEqaJxi-PDXekdTqkawu5mm3DZYYLPCeKM4tBwPhGoGo_0yF2n1PxsSiljUVTbYWSOziKP6SxfZebc-v65D46_b6MEZP0oWWjgd0Ymt89bM3SFrVRSxdqeJzRh6-kox-4rXLt6BIHGzoLruuR01yhQWdfocl6PvBiU0iyAq9kWP5hyak00maMkeId1afuTzIVDu2ZFPDP_DrDvJhNNrV23a5JK_8oNWKSuYvPaifCj_7Ds-A4X8xtdnJxCBUmL5Xxc05UiRpuw7F9vrx06i9jt4VibgYsDCoo0Ty9Xhy5RlKWfkXzKeK3Nnw43KEMWcvUxYzs42ha_Nx8t-mIgfgku2Jy7Q3z_AzgaObaeurLLAj9dwcnY531bmWtar0O4TPIAU3iq-v1RsSYiR0S5SAqyu_gmEA0zEuBfTH05uH1RirBX1Weze63HzEht81WyqzHffcVk-vcMeUHpz20m-VDfTcwbvNpcjzdVZYuG5QAHkJDbSsb9YuMaRT9O2ppdw45f4Zvdg4UOiNc2dYHEHLm0M_aoZ2BE-hjSh6cEmzX1eEmgVHV55zJ-tPkj0gtFxP9Un3imrXG1lcQCAFflLAvvIL5bmSHVhUpXGbpocH9qFlG2BWM1XFmpxO77zlh9Xs340gEb3Pg9qL7NX; refreshToken=JDA9Ekal8Upk7y57PbOmOXwmObtR_96_9NjknV599p0; accessToken=eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg5Mzg0OTU1MkNDRTExMUFDMjc5RjUyNDI3RUEwMUY5QzdDMzAxNTQiLCJ0eXAiOiJhdCtqd3QiLCJ4NXQiOiJpVGhKVlN6T0VSckNlZlVrSi1vQi1jZkRBVlEifQ.eyJuYmYiOjE3NjUzNTkwOTUsImV4cCI6MTc2NTM2MjY5NSwiaXNzIjoiaHR0cDovL2F1dGhlbnRpY2F0aW9uLmJkcy5sYyIsImF1ZCI6IkFwaUdhdGV3YXkiLCJjbGllbnRfaWQiOiIwM2QwZjkwNS0xMGM5LTRkMjEtOTBmNS0xMGI3OGUwYTk4OWMiLCJzdWIiOiI0ODgzNjEzIiwiYXV0aF90aW1lIjoxNzY1Mzg0Mjk1LCJpZHAiOiJsb2NhbCIsInByZWZlcnJlZF91c2VybmFtZSI6ImNodW5nOWF0bUBnbWFpbC5jb20iLCJlbWFpbCI6ImNodW5nOWF0bUBnbWFpbC5jb20iLCJzY29wZSI6WyJvcGVuaWQiLCJwcm9maWxlIiwiQXBpR2F0ZXdheSIsIm9mZmxpbmVfYWNjZXNzIl0sImFtciI6WyJwd2QiXX0.imP_c1loflLIs3eTQBZGzT8XoMT-tNS7o3TePgNt2pl7MOtvUYAzGgZHUo913eAJUKBY5RVoWMcxlDfw1gQPxCocEuRy7y_zxsm6K37_tlcn5FFIGBMwAyGzwgI0s6A00OWrMavP_456XCorIaJFfM3bgvjVlAHW0XZiwpoXP2GRs2oQ1HLfWbGZZO6lD6AqGK-M9UZ8MV9lQV0spM_7IiBnOPVrXxuKkkmexI0CEPr_j02dNMTKwWNSx_RsYSI85g-xCe28hbmvwb7NizlIJJRw6LJddvId3pgPCHWbA-KA7ywxRWfk9is4rwnFnw2qhRhNjN351Yk5gauwo0e0kA; _clsk=1jlkwno%5E1765359078803%5E10%5E1%5Eh.clarity.ms%2Fcollect; USER_PRODUCT_SEARCH=49%7C52%7CHN%7C5%7C9332%7C65%7C0%2C44764107; ttcsid=1765357245847::RKNCSkqs71akRZtSB5Bu.41.1765359079039.0; ttcsid_CHHL1E3C77U1H95PSJM0=1765357245848::iWW-SEIvY7tY5B9Ja6g_.41.1765359079040.1; __uif=__uid%3A5077213022505521518%7C__ui%3A-1%7C__create%3A1763341794; _ga_HTS298453C=GS2.1.s1765357197$o55$g1$t1765359079$j48$l0$h0$d1D-RmiFyNoy5NxG5ECUdbg3uI4ZKRk_23w; AWSALB=eomwKz3+bHtXyN/y2nJkek+qDEwlpq4iaejq6Ru5CmsCYfKRLx14k+zPkAbp96T2RIUwat4FRTfQq4QDKeiVirscngqPptyC+jkeFgwawpqQTdnFDJi4QIeDYl6yRr4IfhTDS1ozFcKx6YWifBY4JuCSWTw5f8RNR906MvFV5W92/0cb+UG2kM+dKEq8lA==; AWSALBCORS=eomwKz3+bHtXyN/y2nJkek+qDEwlpq4iaejq6Ru5CmsCYfKRLx14k+zPkAbp96T2RIUwat4FRTfQq4QDKeiVirscngqPptyC+jkeFgwawpqQTdnFDJi4QIeDYl6yRr4IfhTDS1ozFcKx6YWifBY4JuCSWTw5f8RNR906MvFV5W92/0cb+UG2kM+dKEq8lA==; con.unl.usr.id=%7B%22key%22%3A%22userId%22%2C%22value%22%3A%2265d670e2-ffec-4f26-9b21-5d4dcc6c7230%22%2C%22expireDate%22%3A%222026-12-10T16%3A31%3A38.364022Z%22%7D; con.unl.cli.id=%7B%22key%22%3A%22clientId%22%2C%22value%22%3A%22d6f8054f-8679-4698-b4b8-d1816572c62d%22%2C%22expireDate%22%3A%222026-12-10T16%3A31%3A38.3640484Z%22%7D; cf_clearance=y_wFXQgdCaZhX5G5PPqGminlC2LAp.e24UxWEe5I1no-1765359098-1.2.1.1-PDrSRsRM34GgVbR_3fI45VSrVCVagKWzglK8g12xKWC5Esd1ESv3LakE97SvFfBtzyj.1w4pnCdbANYT_WSYNUrsCagBhnPjZsREdWxEZ1uaraLL86d8bAT2b8w4u0gyLGwRJgfQ4.GqyIQcku_5I2bp4t_n2md.RLhwd6ZCPew8.wtWWVnLhnuHl4So36ZFOaxpKFPZQeTdEJnv1n_qRj6MaGZON_b42vKhAitoVdQ; ab.storage.sessionId.892f88ed-1831-42b9-becb-90a189ce90ad=%7B%22g%22%3A%2285d6dd3e-3eee-4b4f-de5e-2aca6f9afb1e%22%2C%22e%22%3A1765360879763%2C%22c%22%3A1765357832526%2C%22l%22%3A1765359079763%7D; _cfuvid=zU8uA_.HrqTO9oy63fMhTURIv4ZY3bshe40AZhB12FY-1765359102783-0.0.1.1-604800000
"""

USER_AGENT_FROM_BROWSER = """
Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36
"""

# ============================================================


def decrypt_phone(encrypted_phone):
    """Decrypt phone number"""
    
    url = "https://batdongsan.com.vn/Product/ProductDetail/DecryptPhone"
    
    # Clean cookies
    cookies_string = COOKIES_FROM_BROWSER.strip().replace('\n', '').replace('\r', '')
    user_agent = USER_AGENT_FROM_BROWSER.strip().replace('\n', ' ').replace('\r', '')
    
    # Parse cookies
    cookies = {}
    for cookie in cookies_string.split('; '):
        if '=' in cookie:
            key, value = cookie.split('=', 1)
            cookies[key.strip()] = value.strip()
    
    # Headers giống y hệt Postman
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://batdongsan.com.vn',
        'Referer': 'https://batdongsan.com.vn/',
        'User-Agent': user_agent,
        'X-Requested-With': 'XMLHttpRequest',
        'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    
    # Data giống Postman
    data = {
        'PhoneNumber': encrypted_phone,
        'createLead[mobile]': '4883611',
        'createLead[sellerId]': '4491058',
        'createLead[productId]': '44503982',
        # 'createLead[productType]': '0',
        'createLead[leadSourcePage]': 'BDS_LISTING_DETAILS_PAGE',
        'createLead[leadSourceAction]': 'PHONE_REVEAL',
        'createLead[fromLeadType]': 'AGENT_LISTING'
    }
    
    print(f"\n📝 Cookies count: {len(cookies)}")
    print(f"📝 User-Agent: {user_agent[:80]}...")
    print(f"\n🔄 Calling API with curl_cffi (impersonate Chrome)...")
    
    try:
        response = requests.post(
            url, 
            headers=headers, 
            cookies=cookies, 
            data=data, 
            timeout=15,
            impersonate="chrome120"  # Giả lập TLS fingerprint của Chrome
        )
        
        print(f"📊 Status: {response.status_code}")
        
        if response.status_code == 200:
            response_text = response.text.strip()
            
            # API trả về plain text (số phone trực tiếp) hoặc JSON
            try:
                result = response.json()
                print(f"📊 JSON Response: {result}")
                
                if result.get('success'):
                    phone = result.get('phone')
                    print(f"\n✅ SUCCESS!")
                    print(f"📞 Decrypted phone: {phone}")
                    return phone
                else:
                    print(f"\n❌ API returned success=false")
                    print(f"Message: {result.get('message')}")
                    
            except:
                # Response là plain text (số phone trực tiếp)
                if response_text and len(response_text) >= 9:  # Phone ít nhất 9-10 ký tự
                    print(f"\n✅ SUCCESS!")
                    print(f"📞 Decrypted phone: {response_text}")
                    return response_text
                else:
                    print(f"\n❌ Unexpected response")
                    print(f"Response text: {response_text[:200]}")
                
        elif response.status_code == 403:
            if 'cloudflare' in response.text.lower() or 'just a moment' in response.text.lower():
                print(f"\n❌ CLOUDFLARE BLOCK!")
                print(f"\n💡 Giải pháp:")
                print(f"   1. Cookies đã hết hạn - cần copy cookies MỚI từ browser")
                print(f"   2. Mở browser ở chế độ thường (không Incognito)")
                print(f"   3. Đảm bảo đã đăng nhập batdongsan.com.vn")
                print(f"   4. Copy cookies NGAY sau khi đăng nhập")
            else:
                print(f"\n❌ 403 Forbidden")
                print(f"Response: {response.text[:300]}")
        else:
            print(f"\n❌ Unexpected status code")
            print(f"Response: {response.text[:300]}")
            
        return None
        
    except Exception as e:
        print(f"\n❌ Exception: {e}")
        return None


if __name__ == "__main__":
    print("=" * 70)
    print("DECRYPT PHONE NUMBER")
    print("=" * 70)
    
    # Kiểm tra user đã paste cookies chưa
    if "_ga=GA1.1.205476812" in COOKIES_FROM_BROWSER:
        print("\n⚠️  WARNING: Bạn chưa thay cookies!")
        print("   → Cookies hiện tại là cookies mẫu (sẽ không work)")
        print("   → Mở file decrypt_phone_manual.py và paste cookies của BẠN vào")
        print()
    
    # Token từ file extract
    encrypted = "NYh5jSTvzwRrevp0KPFNO0nuXrIdcOQdILy0bZ3R41FoRHkXUpwRXQ"
    
    print(f"🔐 Encrypted: {encrypted}")
    
    result = decrypt_phone(encrypted)
    
    if not result:
        print("\n" + "=" * 70)
        print("HƯỚNG DẪN CHI TIẾT:")
        print("=" * 70)
        print("1. Mở Chrome, truy cập https://batdongsan.com.vn")
        print("2. Đăng nhập (nếu chưa)")
        print("3. Bấm F12 → tab Network")
        print("4. Refresh trang (Ctrl+R)")
        print("5. Click vào request đầu tiên trong list")
        print("6. Scroll xuống 'Request Headers'")
        print("7. Tìm dòng 'cookie:' (rất dài)")
        print("8. Click chuột phải → Copy value")
        print("9. Mở file decrypt_phone_manual.py")
        print("10. Paste vào COOKIES_FROM_BROWSER")
        print("11. Làm tương tự với 'user-agent:'")
        print("12. Save file và chạy lại")
