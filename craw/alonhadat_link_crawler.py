import os
import sys
import time
import random
import argparse
import pymysql
from bs4 import BeautifulSoup
from curl_cffi import requests
from pathlib import Path

# Add current dir to path to import Database and CaptchaSolver
sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    # Fallback if run from a different directory
    sys.path.append(os.path.dirname(os.getcwd()))
    from craw.database import Database

# Import Alonhadat Hash Captcha Solver
sys.path.append(os.path.join(os.getcwd(), 'alonhadat-captcha'))
try:
    from captcha_solver import solve as solve_captcha
except ImportError:
    print("WARNING: Cannot import 'captcha_solver.py' from 'alonhadat-captcha'.")
    solve_captcha = None

try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None


# --- CONFIGURATION ---
BASE_URL = "https://alonhadat.com.vn"
DOMAIN = "alonhadat.com.vn"
DEFAULT_ALONHADAT_COOKIE = "ignoredmember=; _gcl_au=1.1.629991630.1765351837; _ga=GA1.1.529713158.1765351837; _gcl_gs=2.1.k1$i1765590593$u179831156; _gcl_aw=GCL.1765590599.CjwKCAiAl-_JBhBjEiwAn3rN7Tf-sCsCP4Xjkrf3aOrC1Z1jgUGmDs1tGdSjF-KrDzxE89jTY3M9eRoCVNIQAvD_BwE; ASP.NET_SessionId=lvmjczyyaju32xe33nx2n3ya; remember=; page_idcc=4b04a686b0ad13dce35fa99fa4161c65; _ga_ERYH5XEJQM=GS2.1.s1772690813$o61$g1$t1772692283$j58$l0$h0"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    "Referer": "https://alonhadat.com.vn/"
}

MAX_CONSECUTIVE_DUPLICATES = int(os.getenv("ALONHADAT_MAX_CONSECUTIVE_DUPLICATES", "50"))
PAGE_DELAY_MIN_SECONDS = float(os.getenv("ALONHADAT_PAGE_DELAY_MIN", "9"))
PAGE_DELAY_MAX_SECONDS = float(os.getenv("ALONHADAT_PAGE_DELAY_MAX", "12"))
ROBOT_RETRY_TIMES = int(os.getenv("ALONHADAT_ROBOT_RETRY_TIMES", "4"))
ROBOT_RETRY_DELAY_MIN_SECONDS = float(os.getenv("ALONHADAT_ROBOT_RETRY_DELAY_MIN", "7"))
ROBOT_RETRY_DELAY_MAX_SECONDS = float(os.getenv("ALONHADAT_ROBOT_RETRY_DELAY_MAX", "11"))
STOP_IF_LINKS_LT = int(os.getenv("ALONHADAT_STOP_IF_LINKS_LT", "20"))
DEFAULT_COOKIE_PROFILE_DIR = str(Path(__file__).resolve().parent / "playwright_profile_alonhadat")


def refresh_cookie_from_playwright(
    profile_dir: str,
    headless: bool = True,
    force_login: bool = False,
    login_user: str = "",
    login_pass: str = "",
):
    if sync_playwright is None:
        print("[COOKIE] playwright not available; skip auto refresh.")
        return None
    profile_dir = (profile_dir or "").strip()
    if not profile_dir:
        print("[COOKIE] missing profile_dir; skip auto refresh.")
        return None
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=headless,
            )
            page = context.new_page()
            target = "https://alonhadat.com.vn/forbidden.aspx" if force_login else "https://alonhadat.com.vn/quan-ly-ca-nhan.html"
            page.goto(target, wait_until="domcontentloaded", timeout=60000)

            if login_user and login_pass:
                try:
                    page.locator("#account").first.fill(login_user)
                    page.locator("#password").first.fill(login_pass)
                    page.locator("#remember").first.check()
                    try:
                        page.evaluate("() => Login()")
                    except Exception:
                        page.locator("span.login").first.click()
                    page.wait_for_timeout(2000)
                except Exception:
                    pass

            page.wait_for_timeout(1500)
            cookies = context.cookies("https://alonhadat.com.vn")
            context.close()

        if not cookies:
            print("[COOKIE] no cookies from browser context.")
            return None
        kv = [f"{c.get('name')}={c.get('value')}" for c in cookies if c.get("name") and c.get("value") is not None]
        cookie_str = "; ".join(kv).strip()
        names = sorted({c.get("name") for c in cookies if c.get("name")})
        has_session = any(n.lower() == "asp.net_sessionid" for n in names)
        print(
            f"[COOKIE] refreshed from profile={profile_dir} total={len(cookies)} "
            f"has_session={has_session} names={names}"
        )
        return cookie_str or None
    except Exception as e:
        print(f"[COOKIE] refresh failed: {e}")
        return None

def get_trade_type(url: str) -> str:
    if "can-ban" in url:
        return "s"
    elif "cho-thue" in url:
        return "u"
    return "s" # Default

def normalize_slug(slug: str) -> str:
    return (slug or "").strip().strip("/").lower()

def parse_province_slugs(raw: str):
    if not raw:
        return []
    parts = [normalize_slug(x) for x in str(raw).split(",")]
    out = []
    seen = set()
    for p in parts:
        if not p:
            continue
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out

def build_start_url(category: str, province_slug: str) -> str:
    return f"{BASE_URL}/{category}/{province_slug}"

def is_robot_block(resp_url: str, html_content: str) -> bool:
    u = (resp_url or "").lower()
    s = (html_content or "").lower()
    if "xac-thuc-nguoi-dung" in u:
        return True
    if "vui-long-thu-lai.html" in u:
        return True
    if "vui lòng xác minh không phải robot" in s:
        return True
    if "vui long xac minh khong phai robot" in s:
        return True
    if "mã xác thực" in s or "ma xac thuc" in s:
        return True
    if "vui-long-thu-lai.html" in s and "robot" in s:
        return True
    return False

def solve_captcha_if_needed(session, url, html_content):
    max_retries = 3
    for attempt in range(max_retries):
        if "ImageCaptcha.ashx" not in html_content:
            return html_content, session # No captcha
        
        if solve_captcha is None:
            raise Exception("Captcha DETECTED but captcha_solver is not loaded!")
            
        print(f"  [!] Captcha DETECTED. Attempting to solve (Attempt {attempt+1}/{max_retries})...")
        try:
            # Tải ảnh Captcha bằng session hiện tại để giữ cookie
            captcha_url = f"{BASE_URL}/ImageCaptcha.ashx?v=3"
            img_resp = session.get(captcha_url, headers=HEADERS, impersonate="chrome120")
            if img_resp.status_code != 200:
                print(f"  [x] Failed to download Captcha image: {img_resp.status_code}")
                time.sleep(1)
                continue
                
            answer = solve_captcha(img_resp.content)
            if not answer:
                print("  [x] Cannot recognize Captcha cells. Retrying...")
                # Download a new captcha page to get a fresh captcha image?
                # Actually, requesting ImageCaptcha.ashx again regenerates it for the session
                time.sleep(1)
                continue
                
            print(f"  [+] Solved Captcha Answer: {answer}")
            
            # Submit the captcha answer
            soup = BeautifulSoup(html_content, 'html.parser')
            form = soup.find('form')
            if form:
                data = {}
                for input_tag in form.find_all('input'):
                    name = input_tag.get('name')
                    value = input_tag.get('value', '')
                    if name:
                        if 'txtCaptcha' in name or 'captcha' in name.lower() or 'code' in name.lower():
                            data[name] = answer
                        else:
                            data[name] = value
                
                # Fix: Alonhadat form submissions require __EVENTTARGET to simulate button click
                data['__EVENTTARGET'] = 'verify'
                
                # submit form (POST) form action usually resets the current URL
                post_url = url
                action = form.get('action')
                if action:
                    if action.startswith('/'):
                        post_url = f"{BASE_URL}{action}"
                    elif not action.startswith('http'):
                         post_url = f"{BASE_URL}/{action}"
                         
                print(f"  [+] Submitting Captcha form to {post_url}...")
                resp = session.post(post_url, data=data, headers=HEADERS, impersonate="chrome120")
                if "ImageCaptcha.ashx" not in resp.text:
                    print("  [v] Captcha passed successfully!")
                    return resp.text, session
                else:
                     print("  [x] Captcha failed (wrong answer or expired). Retrying...")
                     html_content = resp.text # Update content with new captcha form
                     time.sleep(2)
            else:
                print("  [x] Could not find form to submit captcha.")
                return None, session
                
        except Exception as e:
            print(f"  [x] Error solving captcha: {e}")
            time.sleep(2)
            
    print("  [x] All captcha retry attempts failed.")
    return None, session

def _extract_form_data_for_verify(html_content: str, captcha_answer: str):
    soup = BeautifulSoup(html_content, "html.parser")
    form = soup.find("form")
    if not form:
        return None
    data = {}
    for input_tag in form.find_all("input"):
        name = input_tag.get("name")
        if not name:
            continue
        value = input_tag.get("value", "")
        lname = name.lower()
        if "captcha" in lname or "txtcaptcha" in lname or "code" in lname:
            data[name] = captcha_answer
        else:
            data[name] = value
    data["__EVENTTARGET"] = "verify"
    return data

def solve_verify_page_if_needed(session, target_url: str, resp_url: str, html_content: str, max_retry: int = 3):
    # Same style as detail crawler: solve verification page by fetching ImageCaptcha.ashx,
    # submit verify form, then re-open target URL.
    if not is_robot_block(resp_url, html_content):
        return True, html_content, resp_url
    if solve_captcha is None:
        return False, html_content, resp_url

    for i in range(max_retry):
        try:
            img_url = f"{BASE_URL}/ImageCaptcha.ashx?v=3"
            img_resp = session.get(img_url, headers=HEADERS, timeout=20, impersonate="chrome120")
            if img_resp.status_code != 200:
                continue
            answer = solve_captcha(img_resp.content)
            if not answer:
                continue

            form_data = _extract_form_data_for_verify(html_content, answer)
            if not form_data:
                continue

            post_url = resp_url or target_url
            try:
                s = BeautifulSoup(html_content, "html.parser")
                form = s.find("form")
                if form and form.get("action"):
                    action = form.get("action")
                    if action.startswith("/"):
                        post_url = f"{BASE_URL}{action}"
                    elif not action.startswith("http"):
                        post_url = f"{BASE_URL}/{action}"
            except Exception:
                pass

            print(f"  [!] Verification page detected. Solving captcha ({i+1}/{max_retry})...")
            session.post(
                post_url,
                data=form_data,
                headers=HEADERS,
                timeout=30,
                impersonate="chrome120",
                allow_redirects=True,
            )
            resp2 = session.get(target_url, headers=HEADERS, timeout=30, impersonate="chrome120", allow_redirects=True)
            html2 = resp2.text
            final2 = getattr(resp2, "url", target_url)
            if not is_robot_block(final2, html2):
                print("  [v] Verification passed.")
                return True, html2, final2
            html_content = html2
            resp_url = final2
        except Exception as e:
            print(f"  [x] Verify solve error: {e}")
            continue

    return False, html_content, resp_url

def crawl_one_category(
    cur,
    conn,
    session,
    start_url: str,
    max_dupes: int,
    delay_min: float,
    delay_max: float,
    start_page: int = 1,
    stop_if_links_lt: int = 20,
    auto_refresh_cookie_on_block: bool = False,
    cookie_profile_dir: str = "",
    cookie_refresh_headless: bool = True,
    login_user: str = "",
    login_pass: str = "",
):
    trade_type = get_trade_type(start_url)
    city_name = start_url.split('/')[-1].replace('.htm', '')
    print(f"Category URL: {start_url}")
    print(f"Trade Type: {trade_type} | City Name: {city_name}")

    page = max(1, int(start_page))
    consecutive_dupes = 0
    total_added = 0

    # Never use ".htm" suffix here; Alonhadat pagination path is:
    # /can-ban-nha-dat/{tinh}/trang-{n}
    # Adding ".htm" can trigger redirect to wrong nationwide pages.
    base_no_ext = start_url.replace(".htm", "").rstrip("/")

    while True:
        # Construct Page URL
        if page == 1:
            url = base_no_ext
        else:
            url = f"{base_no_ext}/trang-{page}"

        print(f"\n-> Crawling {url}")

        try:
            resp = session.get(url, headers=HEADERS, impersonate="chrome120", timeout=15)
            html_content = resp.text
            did_cookie_refresh = False

            # Handle anti-bot verify page (same spirit as detail crawler)
            verify_retry = 1 if auto_refresh_cookie_on_block else 3
            solved, html_content, resolved_url = solve_verify_page_if_needed(
                session=session,
                target_url=url,
                resp_url=getattr(resp, "url", url),
                html_content=html_content,
                max_retry=verify_retry,
            )
            if not solved and is_robot_block(getattr(resp, "url", url), html_content):
                print("  [x] Verification captcha could not be solved.")
                # Refresh cookie early (before entering heavy retry loop) to avoid repeated captcha solve cycles.
                if auto_refresh_cookie_on_block:
                    new_cookie = refresh_cookie_from_playwright(
                        profile_dir=cookie_profile_dir,
                        headless=cookie_refresh_headless,
                        login_user=login_user,
                        login_pass=login_pass,
                        force_login=True,
                    )
                    if new_cookie:
                        HEADERS["Cookie"] = new_cookie
                        session = requests.Session()
                        did_cookie_refresh = True
                        print("  -> Early cookie refresh applied. Retrying page once...")
                        resp = session.get(url, headers=HEADERS, impersonate="chrome120", timeout=20)
                        html_content = resp.text
                        solved, html_content, resolved_url = solve_verify_page_if_needed(
                            session=session,
                            target_url=url,
                            resp_url=getattr(resp, "url", url),
                            html_content=html_content,
                            max_retry=1 if auto_refresh_cookie_on_block else 2,
                        )

            # Fallback retry if still blocked after solve attempts
            robot_retry = 0
            while is_robot_block(getattr(resp, "url", url), html_content) and robot_retry < ROBOT_RETRY_TIMES:
                robot_retry += 1
                wait_s = random.uniform(
                    max(0, ROBOT_RETRY_DELAY_MIN_SECONDS),
                    max(ROBOT_RETRY_DELAY_MIN_SECONDS, ROBOT_RETRY_DELAY_MAX_SECONDS),
                )
                print(
                    f"  [!] Anti-bot page detected (attempt {robot_retry}/{ROBOT_RETRY_TIMES}). "
                    f"Retry after {wait_s:.2f}s..."
                )
                time.sleep(wait_s)
                resp = session.get(url, headers=HEADERS, impersonate="chrome120", timeout=20)
                html_content = resp.text
                solved, html_content, resolved_url = solve_verify_page_if_needed(
                    session=session,
                    target_url=url,
                    resp_url=getattr(resp, "url", url),
                    html_content=html_content,
                    max_retry=1 if auto_refresh_cookie_on_block else 2,
                )
                if solved:
                    break

                # If still blocked after 2 retries, refresh cookie once (if not refreshed yet).
                if auto_refresh_cookie_on_block and (not did_cookie_refresh) and robot_retry >= 2:
                    new_cookie = refresh_cookie_from_playwright(
                        profile_dir=cookie_profile_dir,
                        headless=cookie_refresh_headless,
                        login_user=login_user,
                        login_pass=login_pass,
                        force_login=True,
                    )
                    if new_cookie:
                        HEADERS["Cookie"] = new_cookie
                        session = requests.Session()
                        did_cookie_refresh = True
                        print("  -> Cookie refresh in retry loop applied.")

            if is_robot_block(getattr(resp, "url", url), html_content):
                if auto_refresh_cookie_on_block and not did_cookie_refresh:
                    new_cookie = refresh_cookie_from_playwright(
                        profile_dir=cookie_profile_dir,
                        headless=cookie_refresh_headless,
                        login_user=login_user,
                        login_pass=login_pass,
                        force_login=True,
                    )
                    if new_cookie:
                        HEADERS["Cookie"] = new_cookie
                        session = requests.Session()
                        print("  -> Retrying once after cookie refresh...")
                        resp = session.get(url, headers=HEADERS, impersonate="chrome120", timeout=20)
                        html_content = resp.text
                        solved, html_content, resolved_url = solve_verify_page_if_needed(
                            session=session,
                            target_url=url,
                            resp_url=getattr(resp, "url", url),
                            html_content=html_content,
                            max_retry=1 if auto_refresh_cookie_on_block else 2,
                        )
                        if not is_robot_block(getattr(resp, "url", url), html_content):
                            print("  [v] Passed anti-bot after cookie refresh.")
                            # continue normal parsing below
                        else:
                            print("  [!] Still blocked after cookie refresh.")

                if is_robot_block(getattr(resp, "url", url), html_content):
                    print(
                        "  [x] Still blocked by anti-bot verify page after retries. "
                        "Stopping this category to avoid false 'no links'."
                    )
                    break

            # Handle CAPTCHA
            if "ImageCaptcha.ashx" in html_content:
                html_content, session = solve_captcha_if_needed(session, url, html_content)
                if not html_content:
                    print(f"Stopping crawl due to unresolved CAPTCHA at page {page}.")
                    break

            if resp.status_code != 200 and resp.status_code != 302:
                print(f"Error: Status code {resp.status_code}. Stopping.")
                break

            soup = BeautifulSoup(html_content, 'html.parser')

            # Check if page is empty (reached the end of actual listings)
            if "không tìm thấy tin nào theo yêu cầu của bạn" in html_content.lower():
                print(f"  [INFO] Đã lướt hết dữ liệu thực ở trang {page}: 'Không tìm thấy tin...' -> DỪNG.")
                break

            # Extract links
            elements = soup.select(".link")
            page_links = []

            for el in elements:
                href = None
                if el.name == 'a' and el.has_attr('href'):
                    href = el['href']
                else:
                    a_tag = el.find('a')
                    if a_tag and a_tag.has_attr('href'):
                        href = a_tag['href']

                if href:
                    if href.startswith('/'):
                        href = f"{BASE_URL}{href}"
                    elif not href.startswith('http'):
                        href = f"{BASE_URL}/{href}"

                    if href not in page_links:
                        page_links.append(href)

            # Sub-fallback if .link is empty
            if not page_links:
                fallback_elements = soup.select(".ct_title a")
                for el in fallback_elements:
                    if el.has_attr('href'):
                        href = el['href']
                        if href.startswith('/'):
                            href = f"{BASE_URL}{href}"
                        if href not in page_links:
                             page_links.append(href)

            print(f"  Found {len(page_links)} links on page {page}.")

            if not page_links:
                print("  No links found. End of pagination or structural change. Stopping.")
                break

            # Insert into DB
            added_in_page = 0
            for link in page_links:
                batch_date = time.strftime('%Y%m%d')
                check_sql = "SELECT id FROM collected_links WHERE url = %s"
                cur.execute(check_sql, (link,))
                if cur.fetchone():
                    consecutive_dupes += 1
                else:
                    consecutive_dupes = 0
                    insert_sql = """
                        INSERT INTO collected_links
                        (url, domain, batch_date, trade_type, city_name, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    """
                    try:
                        cur.execute(insert_sql, (link, DOMAIN, batch_date, trade_type, city_name))
                        added_in_page += 1
                        total_added += 1
                    except pymysql.Error as e:
                        print(f"  DB Insert Error for {link}: {e}")

            conn.commit()
            print(f"  Added {added_in_page} new links to DB. Consecutive Duplicates: {consecutive_dupes}/{max_dupes}")

            if len(page_links) < max(1, int(stop_if_links_lt)):
                print(
                    f"  [INFO] Page has only {len(page_links)} links (< {max(1, int(stop_if_links_lt))}). "
                    "Treat as end of listing and stop this category."
                )
                break

            if consecutive_dupes >= max_dupes:
                 print(f"Reached {consecutive_dupes} consecutive duplicates. Stopping crawl as we caught up with old data.")
                 break

            page += 1
            sleep_s = random.uniform(max(0, delay_min), max(delay_min, delay_max))
            print(f"  Sleeping {sleep_s:.2f}s...")
            time.sleep(sleep_s)

        except Exception as e:
            print(f"Error processing page {page}: {e}")
            break

    return total_added

def crawl_alonhadat_links(
    province_slug: str,
    max_dupes: int,
    delay_min: float,
    delay_max: float,
    cookie: str = "",
    start_page_sale: int = 1,
    start_page_rent: int = 1,
    only_category: str = "all",
    stop_if_links_lt: int = 20,
    auto_refresh_cookie_on_block: bool = False,
    cookie_profile_dir: str = "",
    cookie_refresh_headless: bool = True,
    login_user: str = "",
    login_pass: str = "",
):
    province_slug = normalize_slug(province_slug)
    if not province_slug:
        raise ValueError("province_slug is required, ví dụ: nghe-an")

    sale_url = build_start_url("can-ban-nha-dat", province_slug)
    rent_url = build_start_url("cho-thue-nha-dat", province_slug)

    print("=" * 50)
    print("Starting Alonhadat Link Crawler")
    print(f"Province: {province_slug}")
    print(f"Phase 1 (sale): {sale_url}")
    print(f"Phase 2 (rent): {rent_url}")
    print("=" * 50)
    
    # Initialize DB
    db = Database()
    conn = db.get_connection()
    if not conn:
        print("Database connection failed.")
        return
    cur = conn.cursor()
    
    # Same behavior as detail crawler: allow --cookie, fallback env ALONHADAT_COOKIE.
    alon_cookie = (
        (cookie or "").strip()
        or os.getenv("ALONHADAT_COOKIE", "").strip()
        or DEFAULT_ALONHADAT_COOKIE
    )
    if alon_cookie:
        HEADERS["Cookie"] = alon_cookie
        print("Cookie: loaded")
    else:
        print("Cookie: not set")

    # Initialize Session
    session = requests.Session()

    total_added = 0
    oc = (only_category or "all").strip().lower()
    if oc in ("all", "sale"):
        total_added += crawl_one_category(
            cur, conn, session, sale_url, max_dupes, delay_min, delay_max,
            start_page=start_page_sale, stop_if_links_lt=stop_if_links_lt,
            auto_refresh_cookie_on_block=auto_refresh_cookie_on_block,
            cookie_profile_dir=cookie_profile_dir,
            cookie_refresh_headless=cookie_refresh_headless,
            login_user=login_user,
            login_pass=login_pass,
        )
    if oc in ("all", "rent"):
        total_added += crawl_one_category(
            cur, conn, session, rent_url, max_dupes, delay_min, delay_max,
            start_page=start_page_rent, stop_if_links_lt=stop_if_links_lt,
            auto_refresh_cookie_on_block=auto_refresh_cookie_on_block,
            cookie_profile_dir=cookie_profile_dir,
            cookie_refresh_headless=cookie_refresh_headless,
            login_user=login_user,
            login_pass=login_pass,
        )

    conn.close()
    print("=" * 50)
    print(f"Crawl Finished. Total new links added: {total_added}")
    print("=" * 50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alonhadat link crawler theo tỉnh (hỗ trợ nhiều tỉnh, phân tách bằng dấu phẩy)")
    parser.add_argument(
        "province_slugs",
        nargs="?",
        default="nghe-an",
        help="Slug tỉnh, có thể nhiều tỉnh bằng dấu phẩy. Ví dụ: nghe-an hoặc quang-ninh,hue,dak-lak"
    )
    parser.add_argument("--max-dupes", type=int, default=MAX_CONSECUTIVE_DUPLICATES, help="Stop sau N duplicate liên tiếp")
    parser.add_argument("--delay-min", type=float, default=PAGE_DELAY_MIN_SECONDS, help="Delay min giữa các page")
    parser.add_argument("--delay-max", type=float, default=PAGE_DELAY_MAX_SECONDS, help="Delay max giữa các page")
    parser.add_argument("--start-page-sale", type=int, default=1, help="Trang bắt đầu cho can-ban-nha-dat")
    parser.add_argument("--start-page-rent", type=int, default=1, help="Trang bắt đầu cho cho-thue-nha-dat")
    parser.add_argument("--only-category", choices=["all", "sale", "rent"], default="all", help="Chỉ crawl 1 category nếu cần")
    parser.add_argument("--stop-if-links-lt", type=int, default=STOP_IF_LINKS_LT, help="Dừng nếu số link/page nhỏ hơn ngưỡng này")
    parser.add_argument(
        "--cookie",
        default=os.getenv("ALONHADAT_COOKIE", "") or DEFAULT_ALONHADAT_COOKIE,
        help="Cookie string (same as detail crawler)",
    )
    parser.add_argument(
        "--auto-refresh-cookie-on-block",
        action="store_true",
        help="When blocked, refresh cookie from Playwright profile and retry once",
    )
    parser.add_argument(
        "--cookie-profile-dir",
        default=os.getenv("ALONHADAT_COOKIE_PROFILE_DIR", DEFAULT_COOKIE_PROFILE_DIR),
        help="Playwright persistent profile dir for auto cookie refresh",
    )
    parser.add_argument(
        "--cookie-refresh-headed",
        action="store_true",
        help="Use headed browser for cookie refresh (default headless)",
    )
    parser.add_argument("--login-user", default=os.getenv("ALONHADAT_LOGIN_USER", ""), help="Auto login username for cookie refresh")
    parser.add_argument("--login-pass", default=os.getenv("ALONHADAT_LOGIN_PASS", ""), help="Auto login password for cookie refresh")
    args = parser.parse_args()

    provinces = parse_province_slugs(args.province_slugs)
    if not provinces:
        raise SystemExit("No valid province slug provided.")

    print(f"Provinces to crawl ({len(provinces)}): {', '.join(provinces)}")
    for i, p in enumerate(provinces, 1):
        print(f"\n########## [{i}/{len(provinces)}] START {p} ##########")
        crawl_alonhadat_links(
            province_slug=p,
            max_dupes=args.max_dupes,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            cookie=args.cookie,
            start_page_sale=args.start_page_sale,
            start_page_rent=args.start_page_rent,
            only_category=args.only_category,
            stop_if_links_lt=args.stop_if_links_lt,
            auto_refresh_cookie_on_block=bool(args.auto_refresh_cookie_on_block),
            cookie_profile_dir=str(args.cookie_profile_dir or "").strip(),
            cookie_refresh_headless=not bool(args.cookie_refresh_headed),
            login_user=str(args.login_user or "").strip(),
            login_pass=str(args.login_pass or "").strip(),
        )
        print(f"########## [{i}/{len(provinces)}] END {p} ##########")
