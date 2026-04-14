import os
import sys
import re
from datetime import datetime
import time
import json
import random
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Callable
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Category Mappings
MOGI_CATEGORIES = {
    "Mua": {
        "Nhà mặt tiền phố": "https://mogi.vn/mua-nha-mat-tien-pho",
        "Nhà biệt thự, liền kề": "https://mogi.vn/mua-nha-biet-thu-lien-ke",
        "Đường nội bộ": "https://mogi.vn/mua-duong-noi-bo",
        "Nhà hẻm ngõ": "https://mogi.vn/mua-nha-hem-ngo",
        "Căn hộ chung cư": "https://mogi.vn/mua-can-ho-chung-cu",
        "Căn hộ tập thể, cư xá": "https://mogi.vn/mua-can-ho-tap-the-cu-xa",
        "Căn hộ Penthouse": "https://mogi.vn/mua-can-ho-penthouse",
        "Căn hộ dịch vụ": "https://mogi.vn/mua-can-ho-dich-vu",
        "Căn hộ Officetel": "https://mogi.vn/mua-can-ho-officetel",
        "Đất thổ cư": "https://mogi.vn/mua-dat-tho-cu",
        "Đất nền dự án": "https://mogi.vn/mua-dat-nen-du-an",
        "Đất nông nghiệp": "https://mogi.vn/mua-dat-nong-nghiep",
        "Đất kho xưởng": "https://mogi.vn/mua-dat-kho-xuong",
    },
    "Thuê": {
        "Nhà mặt tiền phố": "https://mogi.vn/thue-nha-mat-tien-pho",
        "Nhà biệt thự, liền kề": "https://mogi.vn/thue-nha-biet-thu-lien-ke",
        "Đường nội bộ": "https://mogi.vn/thue-duong-noi-bo",
        "Nhà hẻm ngõ": "https://mogi.vn/thue-nha-hem-ngo",
        "Căn hộ chung cư": "https://mogi.vn/thue-can-ho-chung-cu",
        "Căn hộ tập thể, cư xá": "https://mogi.vn/thue-can-ho-tap-the-cu-xa",
        "Căn hộ Penthouse": "https://mogi.vn/thue-can-ho-penthouse",
        "Căn hộ dịch vụ": "https://mogi.vn/thue-can-ho-dich-vu",
        "Căn hộ Officetel": "https://mogi.vn/thue-can-ho-officetel",
        "Phòng trọ, nhà trọ": "https://mogi.vn/thue-phong-tro-nha-tro",
        "Văn phòng": "https://mogi.vn/thue-van-phong",
        "Nhà xưởng, kho bãi": "https://mogi.vn/thue-nha-xuong-kho-bai-dat",
    }
}

class MogiCrawler:
    def __init__(self, db_connector=None):
        self.db = db_connector
        self.should_stop = False
        self._log_lock = threading.Lock()

    def stop(self):
        self.should_stop = True

    def build_url(self, base_url: str, page: int) -> str:
        parsed = urlparse(base_url)
        query = parse_qs(parsed.query)
        query['cp'] = [str(page)]
        new_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def _start_driver(self, proxy: Optional[str] = None):
        # Each thread gets its own driver
        # SeleniumBase Driver supports 'proxy' arg (format: "ip:port" or "user:pass@ip:port")
        # If user provided full url "http://...", strip it or pass as is? SeleniumBase usually wants "host:port" logic or --proxy-server.
        # Driver(proxy="ip:port") works.
        return Driver(uc=True, headless=True, proxy=proxy)

    def crawl_task(self, 
                   base_url: str, 
                   trade_type: str,
                   cat_name: str,
                   start_page: int, 
                   stop_on_error_count: int,
                   log_queue,
                   proxy: Optional[str] = None): # Added proxy
        
        task_id = f"[{cat_name}]"
        # Helper to put log
        def log(msg):
            log_queue.put(msg)

        log(f"{task_id} Starting...")
        if proxy:
             log(f"{task_id} Using Proxy: {proxy}")
        
        driver = None
        page = start_page
        consecutive_errors = 0
        total_collected = 0
        
        try:
            driver = self._start_driver(proxy=proxy)
            
            while not self.should_stop:
                url = self.build_url(base_url, page)
                
                # Retry logic for current page
                page_retries = 3
                found_on_page = False
                
                for attempt in range(page_retries):
                    try:
                        driver.get(url)
                        time.sleep(random.uniform(2, 4))
                        
                        page_source = driver.page_source.lower()
                        title = driver.title.lower()
                        
                        if "404" in title or "page not found" in page_source:
                            log(f"{task_id} -> 404/Not Found at page {page}")
                            # 404 is fatal for this page, usually end of list or bad url. No retry needed usually?
                            # But lets treat it as fatal for this attempt.
                            break
                        elif "cloudflare" in title or "attention required" in title:
                            log(f"{task_id} -> Cloudflare detected! Waiting 10s... (Attempt {attempt+1}/{page_retries})")
                            time.sleep(10)
                            continue # Retry
                        else:
                            # Selector: ul.props .link-overlay
                            elements = driver.find_elements(By.CSS_SELECTOR, "ul.props .link-overlay")
                            
                            links_batch = []
                            for el in elements:
                                try:
                                    href = el.get_attribute("href")
                                    if href and "/news/" not in href:
                                        if "mogi.vn" not in href and href.startswith("/"):
                                            href = "https://mogi.vn" + href
                                        links_batch.append(href)
                                except:
                                    pass
                            
                            count = len(links_batch)
                            if count == 0:
                                log(f"{task_id} -> Page {page} loaded but 0 items. (Attempt {attempt+1}/{page_retries})")
                                if attempt < page_retries - 1:
                                    time.sleep(2)
                                    continue # Retry this page
                            else:
                                found_on_page = True
                                consecutive_errors = 0 # OK
                                total_collected += count
                                log(f"{task_id} -> Page {page}: Found {count} links")
                                
                                if self.db:
                                    self.db.add_collected_links(
                                        links_list=links_batch,
                                        domain='mogi.vn',
                                        loaihinh=cat_name,
                                        trade_type=trade_type.lower()
                                    )
                                break # Done with this page, move next
                                
                    except Exception as e:
                        log(f"{task_id} Error page {page}: {str(e)[:100]}. (Attempt {attempt+1}/{page_retries})")
                        time.sleep(2)
                        # Retry loop continues
                
                if not found_on_page:
                    # Exhausted retries or 404
                    log(f"{task_id} -> Failed to get data for page {page} after {page_retries} attempts.")
                    consecutive_errors += 1
                    
                    # If failed due to timeout/errors, restart driver to be safe
                    try:
                        log(f"{task_id} -> Restarting driver to clear potential zombies...")
                        driver.quit()
                        time.sleep(2)
                        driver = self._start_driver()
                        log(f"{task_id} -> Driver restarted successfully.")
                    except Exception as restart_err:
                        log(f"{task_id} -> Driver restart failed: {restart_err}")

                if consecutive_errors >= stop_on_error_count:
                    log(f"{task_id} Stopping: {consecutive_errors} consecutive errors.")
                    break
                
                page += 1
                
        except Exception as e:
            import traceback
            log(f"{task_id} Critical Driver Error: {e} \n {traceback.format_exc()}")
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            log(f"{task_id} Finished. Total: {total_collected}")

    def run_batch(self, 
                  tasks: List[Dict], 
                  max_threads: int,
                  progress_callback: Callable[[str], None],
                  proxy: Optional[str] = None): # Added proxy
        
        self.should_stop = False
        import queue
        log_queue = queue.Queue()
        
        progress_callback(f"Starting batch with {len(tasks)} tasks, {max_threads} threads.")
        if proxy:
            progress_callback(f"Global Proxy Configured: {proxy}")
        
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = []
            for t in tasks:
                futures.append(executor.submit(
                    self.crawl_task,
                    base_url=t['url'],
                    trade_type=t['trade_type'],
                    cat_name=t['cat_name'],
                    start_page=t.get('start_page', 1),
                    stop_on_error_count=t.get('stop_error', 5),
                    log_queue=log_queue,
                    proxy=proxy 
                ))
            
            # Monitoring Loop
            while True:
                # Drain Queue
                try:
                    while True:
                        msg = log_queue.get_nowait()
                        progress_callback(msg)
                except queue.Empty:
                    pass
                
                # Check if all futures are done
                all_done = all(f.done() for f in futures)
                if all_done:
                    break
                
                if self.should_stop:
                    break
                    
                time.sleep(0.5)

            # Collect results (just to catch remaining exceptions if any)
            for f in futures:
                try:
                    f.result()
                except Exception as e:
                    import traceback
                    progress_callback(f"Task exception in future: {e} \n {traceback.format_exc()}")

# Update rendering to match new signature is NOT needed as run_batch signature is same regarding external call (Wait, no log_func param change in crawl_task invocation inside run_batch)


def render_mogi_ui(st_module, db_connector=None):
    st = st_module
    st.header("Mogi.vn Crawler (SeleniumBase Multi-Thread)")
    
    col1, col2 = st.columns(2)
    with col1:
        trade_type = st.radio("Loại giao dịch", ["Mua", "Thuê"], horizontal=True)
    with col2:
        cats = MOGI_CATEGORIES.get(trade_type, {})
        # Multi-select UI
        all_cats = list(cats.keys())
        container = st.container()
        all = st.checkbox("Chọn tất cả (Select All)", value=False)
        
        if all:
            selected_cats = container.multiselect("Chọn chuyên mục", all_cats, default=all_cats)
        else:
            selected_cats = container.multiselect("Chọn chuyên mục", all_cats)
            
    st.divider()
    # Proxy Config
    p_col1, p_col2 = st.columns([3, 1])
    with p_col1:
        proxy_input = st.text_input("Proxy (Optional)", value="", placeholder="http://100.53.5.135:3128")
    with p_col2:
        st.write("") # Spacer
        st.write("") # Spacer
        if st.button("Test Proxy"):
            if not proxy_input.strip():
                st.warning("Vui lòng nhập Proxy để test!")
            else:
                with st.spinner("Testing Proxy..."):
                    try:
                        # Test Driver
                        td = Driver(uc=True, headless=True, proxy=proxy_input.strip())
                        td.get("https://api.ipify.org?format=json")
                        time.sleep(2)
                        content = td.find_element(By.TAG_NAME, "body").text
                        td.quit()
                        st.success(f"Proxy OK! IP Public: {content}")
                    except Exception as e:
                        st.error(f"Proxy Lỗi: {str(e)[:100]}")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        start_page = st.number_input("Start Page", min_value=1, value=1)
    with c2:
        stop_error = st.number_input("Stop after N errors", min_value=1, value=5)
    with c3:
        concurrency = st.number_input("Max Threads", min_value=1, max_value=10, value=3)

    if st.button("Start Crawling Immediately", type="primary"):
        if not selected_cats:
            st.error("Vui lòng chọn ít nhất 1 chuyên mục!")
            return

        status_area = st.empty()
        log_area = st.code("Initializing...")
        logs = []
        
        def log_callback(msg):
            # Trim log to last 20 lines to avoid UI lag
            logs.append(msg)
            if len(logs) > 20:
                logs.pop(0)
            log_area.code("\n".join(logs))
        
        # Build Task List from selection
        tasks_to_run = []
        for name in selected_cats:
            url = cats.get(name)
            tasks_to_run.append({
                'url': url,
                'trade_type': trade_type,
                'cat_name': name,
                'start_page': start_page,
                'stop_error': stop_error
            })
            
        crawler = MogiCrawler(db_connector=db_connector)
        
        # Clean proxy input
        final_proxy = proxy_input.strip() if proxy_input.strip() else None
        
        with st.spinner(f"Running {len(tasks_to_run)} tasks with {concurrency} threads..."):
            crawler.run_batch(
                tasks=tasks_to_run,
                max_threads=concurrency,
                progress_callback=log_callback,
                proxy=final_proxy
            )
        st.success("Batch Processing Completed!")

    # --- QUEUE MANAGEMENT SYSTEM (NEW) ---
    st.markdown("---")
    st.subheader("📋 Task Queue Manager (Chạy nhiều Job khác nhau)")
    
    if 'mogi_task_queue' not in st.session_state:
        st.session_state.mogi_task_queue = []
        
    c_q1, c_q2 = st.columns([1, 1])
    with c_q1:
        if st.button("➕ Add Selection to Queue"):
            if not selected_cats:
                st.warning("⚠️ Chưa chọn chuyên mục nào để thêm!")
            else:
                count_added = 0
                for name in selected_cats:
                    url = cats.get(name)
                    # Add to session queue
                    st.session_state.mogi_task_queue.append({
                        'url': url,
                        'trade_type': trade_type,
                        'cat_name': name,
                        'start_page': start_page,
                        'stop_error': stop_error
                    })
                    count_added += 1
                st.success(f"✅ Đã thêm {count_added} task(s) vào hàng đợi! (Start Page: {start_page})")

    with c_q2:
        if st.button("🗑️ Clear Queue"):
            st.session_state.mogi_task_queue = []
            st.rerun()

    # Display Queue
    if st.session_state.mogi_task_queue:
        with st.expander(f"Xem hàng đợi ({len(st.session_state.mogi_task_queue)} tasks)", expanded=True):
            # Show simple table
            q_data = []
            for i, t in enumerate(st.session_state.mogi_task_queue):
                q_data.append({
                    "Index": i+1,
                    "Loại": t['trade_type'],
                    "Chuyên mục": t['cat_name'],
                    "Start Page": t['start_page']
                })
            st.table(q_data)
            
            # Queue Concurrency Control
            col_q_run1, col_q_run2 = st.columns([1, 2])
            with col_q_run1:
                queue_concurrency = st.number_input(
                    "Số luồng chạy đồng thời (Concurrent Tasks)", 
                    min_value=1, max_value=10, value=3,
                    key="queue_concurrency_input",
                    help="Số lượng task sẽ chạy cùng một lúc."
                )
            
            # Run Queue Button
            with col_q_run2:
                st.write("") # Spacer for alignment
                if st.button("🚀 CHẠY TẤT CẢ (RUN ALL QUEUE)", type="primary", use_container_width=True):
                    status_area_q = st.empty()
                    log_area_q = st.code("Initializing Queue...")
                    logs_q = []
                    
                    def log_callback_q(msg):
                        logs_q.append(msg)
                        if len(logs_q) > 20:
                            logs_q.pop(0)
                        log_area_q.code("\n".join(logs_q))
                    
                    crawler_q = MogiCrawler(db_connector=db_connector)
                    final_proxy_q = proxy_input.strip() if proxy_input.strip() else None
                    
                    tasks_queue = list(st.session_state.mogi_task_queue)
                    
                    with st.spinner(f"Đang chạy {len(tasks_queue)} tasks với {queue_concurrency} luồng song song..."):
                        crawler_q.run_batch(
                            tasks=tasks_queue,
                            max_threads=queue_concurrency,
                            progress_callback=log_callback_q,
                            proxy=final_proxy_q
                        )
                    st.success("✅ Đã chạy xong toàn bộ hàng đợi!")
                    st.session_state.mogi_task_queue = [] # Clear after run

    # --- BACKGROUND JOB MANAGER ---
    st.markdown("---")
    st.subheader("🤖 Background Job Manager & Scheduler")
    st.info("Quản lý việc chạy tự động hàng ngày và các job đang chạy ngầm.")
    
    # Path to log file
    log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_mogi.log")

    # 1. TRẠNG THÁI & KÍCH HOẠT THỦ CÔNG
    st.write("#### 1. Trạng thái & Chạy ngay")
    bg_c1, bg_c2, bg_c3 = st.columns([1, 1, 1])
    
    # Check if running
    is_running = False
    running_pid = None
    try:
        # Check if daily_mogi_crawl.py is in process list and get PID
        # Use -a to see full command line for filtering
        check_cmd = "pgrep -a -f daily_mogi_crawl.py"
        res = subprocess.run(check_cmd, shell=True, stdout=subprocess.PIPE, text=True)
        if res.returncode == 0 and res.stdout:
            lines = res.stdout.strip().split('\n')
            for line in lines:
                # Filter out the pgrep command itself and shell wrappers
                if "pgrep" in line or "/bin/sh" in line or "grep" in line:
                    continue
                    
                # Real process should be like "python daily_mogi_crawl.py"
                parts = line.strip().split(' ', 1)
                if len(parts) > 0:
                    pid = parts[0]
                    # Double check it is integer
                    if pid.isdigit():
                        is_running = True
                        running_pid = pid
                        break
    except:
        pass

    with bg_c1:
        if is_running:
            st.success(f"🟢 Job đang chạy (PID: {running_pid})")
        else:
            st.warning("⚪ Job đang nghỉ")

    with bg_c2:
        if st.button("🚀 Chạy Ngay Lập Tức", type="primary", disabled=is_running):
            try:
                script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_mogi_crawl.py")
                cmd = [sys.executable, script_path]
                
                # Ensure log file exists
                if not os.path.exists(log_file_path):
                    with open(log_file_path, "w", encoding="utf-8") as f:
                        f.write("") # Create empty file
                
                # Redirect output to log file
                f_log = open(log_file_path, "a", encoding="utf-8")
                f_log.write(f"\n\n=== STARTED AT {datetime.now()} ===\n")
                f_log.flush()
                
                # Use subprocess.Popen with detached state
                subprocess.Popen(cmd, stdout=f_log, stderr=f_log, start_new_session=True)
                
                st.toast("Đã gửi lệnh chạy ngầm thành công!", icon="🚀")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Lỗi khi khởi động job: {e}")

    with bg_c3:
        if st.button("🛑 DỪNG (KILL JOB)", type="secondary", disabled=not is_running):
            try:
                subprocess.run("pkill -f daily_mogi_crawl.py", shell=True)
                subprocess.run("pkill -f daily_mogi_crawl.sh", shell=True)
                # Kill detail crawler too just in case
                subprocess.run("pkill -f mogi_detail_crawler.py", shell=True)
                
                st.toast("Đã gửi lệnh dừng!", icon="🛑")
                time.sleep(2) # Wait a bit for process to die
                st.rerun()
            except Exception as e:
                st.error(f"Lỗi khi dừng: {e}")

    st.divider()

    # 2. HẸN GIỜ (SCHEDULER)
    st.write("#### 2. Cài Đặt Lịch Chạy Tự Động")
    
    # Get current schedule from crontab
    current_schedule = "08:00" # Default
    try:
        res = subprocess.run("crontab -l", shell=True, stdout=subprocess.PIPE, text=True)
        if res.stdout:
            import re
            # Find line with daily_mogi_crawl.sh
            # Format: MIN HOUR * * * /path/to/script
            match = re.search(r'(\d+)\s+(\d+)\s+\*\s+\*\s+\*\s+.*daily_mogi_crawl\.sh', res.stdout)
            if match:
                minute = int(match.group(1))
                hour = int(match.group(2))
                current_schedule = f"{hour:02d}:{minute:02d}"
    except:
        pass

    # Parse current schedule to get default H/M
    default_hour = 8
    default_minute = 0
    try:
        if current_schedule and ":" in current_schedule:
            parts = current_schedule.split(":")
            default_hour = int(parts[0])
            default_minute = int(parts[1])
    except:
        pass

    sch_c1, sch_c2, sch_c3 = st.columns([1, 1, 2])
    with sch_c1:
        sel_hour = st.selectbox("Giờ (Hour)", options=list(range(24)), index=default_hour)
    with sch_c2:
        sel_minute = st.selectbox("Phút (Minute)", options=list(range(60)), index=default_minute)
    
    with sch_c3:
        st.write("") # Spacer
        st.write("") 
        if st.button("💾 Lưu Lịch Chạy"):
            try:
                # 1. Get existing crontab excluding our script
                res = subprocess.run("crontab -l", shell=True, stdout=subprocess.PIPE, text=True)
                current_cron = res.stdout or ""
                lines = current_cron.splitlines()
                # Remove old config for this script
                lines = [l for l in lines if "daily_mogi_crawl.sh" not in l]
                
                # 2. Add new config
                sh_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_mogi_crawl.sh")
                new_line = f"{sel_minute} {sel_hour} * * * {sh_path}"
                lines.append(new_line)
                
                # 3. Write back
                new_cron_content = "\n".join(lines) + "\n"
                process = subprocess.Popen(['crontab', '-'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                stdout, stderr = process.communicate(input=new_cron_content)
                
                if process.returncode == 0:
                    st.success(f"Đã cập nhật lịch chạy: {sel_hour:02d}:{sel_minute:02d} hàng ngày!")
                else:
                    st.error(f"Lỗi crontab: {stderr}")
            except Exception as e:
                st.error(f"Lỗi lưu lịch: {e}")

    st.divider()

    # 3. LOG VIEWER
    st.write("#### 3. Logs (daily_mogi.log)")
    log_c1, log_c2 = st.columns([4, 1])
    with log_c2:
        if st.button("🔄 Làm mới Log"):
            st.rerun()
            
    log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_mogi.log")
    log_content = "Chưa có log..."
    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                if lines:
                    log_content = "".join(lines[-100:])
                else:
                    log_content = "Log file is empty."
        except Exception as e:
            log_content = f"Error reading log: {e}"
    
    st.code(log_content, language="text", line_numbers=True)
