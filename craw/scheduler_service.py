"""
Background scheduler service for listing -> detail pipeline.
Run: python scheduler_service.py
"""

import asyncio
import json
import os
import time
import threading
import hashlib
import random
import signal
import sys
import atexit
from datetime import datetime, timedelta, date
from typing import Optional, List

import requests

from database import Database

# Global flag để track signal - dùng để graceful shutdown
_shutdown_requested = False
_signal_count = 0
_last_signal_time = 0

def signal_handler(signum, frame):
    """
    Log khi nhận signal từ bất kỳ nguồn nào.
    Chromium/Playwright có thể gửi SIGINT khi khởi động - ta ignore signal đơn lẻ.
    Chỉ shutdown khi user nhấn Ctrl+C 2 lần trong 3 giây.
    """
    global _shutdown_requested, _signal_count, _last_signal_time
    sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    current_time = time.time()
    
    # Reset counter nếu signal cách nhau > 3 giây
    if current_time - _last_signal_time > 3:
        _signal_count = 0
    
    _signal_count += 1
    _last_signal_time = current_time
    
    print(f"\n[SIGNAL] Received {sig_name} (count={_signal_count})")
    
    if _signal_count >= 2:
        print("[SIGNAL] Multiple signals received - shutting down...")
        _shutdown_requested = True
    else:
        print("[SIGNAL] Single signal - possibly from Playwright/Chromium. Press Ctrl+C again to quit.")

# Đăng ký signal handler
signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)

from listing_crawler import crawl_listing
from scraper_core import scrape_url
from web_scraper import WebScraper


async def _sleep_with_cancel(total_seconds: float, cancel_callback=None, step: float = 0.5) -> bool:
    if not total_seconds or total_seconds <= 0:
        return True
    remaining = float(total_seconds)
    while remaining > 0:
        if cancel_callback and cancel_callback():
            return False
        chunk = step if remaining > step else remaining
        await asyncio.sleep(chunk)
        remaining -= chunk
    return True


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except Exception:
        return False


def _acquire_service_lock(lock_path: str) -> bool:
    if os.path.exists(lock_path):
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            old_pid = int(content) if content else None
        except Exception:
            old_pid = None
        if old_pid and _is_pid_alive(old_pid):
            print(f"[Scheduler] Another scheduler_service is running (pid={old_pid}). Exit.")
            return False
        try:
            os.remove(lock_path)
        except Exception:
            pass
    try:
        with open(lock_path, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
    except Exception:
        return False
    def _cleanup():
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except Exception:
            pass
    atexit.register(_cleanup)
    return True


def run_async_safe(coro):
    """Chạy async coroutine an toàn trong thread, tránh signal conflict trên Windows"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    except Exception as e:
        print(f"[AsyncSafe] Error: {e}")
        raise


def load_json(path: str) -> Optional[dict]:
    if not path:
        return None
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def parse_run_times(run_times: Optional[str]) -> List[str]:
    if not run_times:
        return ["08:00", "20:00"]
    parts = [p.strip() for p in run_times.split(',') if p.strip()]
    return parts or ["08:00", "20:00"]


def compute_next_run(task: dict, now: datetime) -> datetime:
    schedule_type = (task.get('schedule_type') or 'interval').lower()
    if schedule_type == 'daily':
        times = parse_run_times(task.get('run_times'))
        candidates = []
        for t in times:
            try:
                h, m = t.split(':', 1)
                candidate = now.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
                candidates.append(candidate)
            except Exception:
                continue
        candidates = sorted(candidates)
        for c in candidates:
            if c > now:
                return c
        if candidates:
            return candidates[0] + timedelta(days=1)
        return now + timedelta(days=1)
    else:
        mins = int(task.get('interval_minutes') or 30)
        return now + timedelta(minutes=max(mins, 1))


def send_telegram_message(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20
        )
    except Exception:
        pass


def _fetch_failed_images(db: Database, limit: int = 200, domain: str = None):
    conn = db.get_connection()
    cursor = conn.cursor(dictionary=True) if db.use_mysql_connector else conn.cursor()
    try:
        if domain:
            cursor.execute(
                '''
                SELECT di.id, di.detail_id, di.image_url, di.idx, di.status, di.created_at
                FROM scraped_detail_images di
                JOIN scraped_details_flat df ON df.id = di.detail_id
                WHERE di.status = 'FAILED'
                  AND df.domain = %s
                ORDER BY di.id ASC
                LIMIT %s
                ''',
                (domain, limit)
            )
        else:
            cursor.execute(
                '''
                SELECT id, detail_id, image_url, idx, status, created_at
                FROM scraped_detail_images
                WHERE status = 'FAILED'
                ORDER BY id ASC
                LIMIT %s
                ''',
                (limit,)
            )
        rows = cursor.fetchall()
        if db.use_mysql_connector:
            return rows
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        return results
    finally:
        cursor.close()
        conn.close()


def _save_image_bytes(image_bytes: bytes, file_path: str, max_width: int = 1100):
    try:
        from io import BytesIO
        from PIL import Image
    except Exception:
        with open(file_path, 'wb') as f:
            f.write(image_bytes)
        return

    try:
        img = Image.open(BytesIO(image_bytes))
        width, height = img.size
        if width > max_width:
            new_height = max(int(height * max_width / width), 1)
            img = img.resize((max_width, new_height), Image.LANCZOS)
        ext = os.path.splitext(file_path)[1].lower()
        fmt = img.format
        if not fmt:
            fmt = 'PNG' if ext == '.png' else 'JPEG'
        if fmt.upper() == 'JPEG' and img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        img.save(file_path, format=fmt)
    except Exception:
        with open(file_path, 'wb') as f:
            f.write(image_bytes)


def _download_image_rows(db: Database, rows: list, image_dir: str, interval: float):
    ok = 0
    fail = 0
    for row in rows:
        url = row.get('image_url')
        image_id = row.get('id')
        if not url:
            continue
        file_path = None
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            ext = os.path.splitext(url.split('?')[0])[1] or '.jpg'
            filename = f"{hashlib.md5(url.encode()).hexdigest()}{ext}"
            file_path = os.path.join(image_dir, filename)
            _save_image_bytes(resp.content, file_path, max_width=1100)
            ok += 1
            db.add_downloaded_image(url, file_path, "SUCCESS", None, None)
            if image_id:
                db.update_detail_image_status(image_id, "DOWNLOADED")
        except Exception as e:
            fail += 1
            db.add_downloaded_image(url, file_path, "FAILED", None, str(e))
            if image_id:
                db.update_detail_image_status(image_id, "FAILED")
        time.sleep(interval)
    return ok, fail


def download_images(
    db: Database,
    image_dir: str,
    images_per_minute: int = 30,
    batch_limit: int = 200,
    domain: str = None,
    status: str = None,
    log_callback=None,
):
    os.makedirs(image_dir, exist_ok=True)
    interval = 60.0 / max(images_per_minute, 1)
    try:
        db.sync_detail_image_statuses()
    except Exception:
        pass
    status_filter = (status or "").strip().upper() or None
    if status_filter and status_filter not in ("PENDING", "FAILED"):
        msg = f"[Scheduler Image] Status filter '{status_filter}' not supported for download."
        print(msg)
        if log_callback:
            log_callback(msg)
        return 0, 0, 0, 0, 0, 0
    rows = db.get_undownloaded_detail_images(limit=batch_limit, domain=domain) if status_filter != "FAILED" else []
    pending_ok = 0
    pending_fail = 0
    pending_total = 0
    if rows and status_filter != "FAILED":
        pending_total = len(rows)
        msg = f"[Scheduler Image] Pending images: {pending_total}"
        print(msg)
        if log_callback:
            log_callback(msg)
        pending_ok, pending_fail = _download_image_rows(db, rows, image_dir, interval)
        msg = f"[Scheduler Image] Done pending: ok={pending_ok}, failed={pending_fail}, total={pending_total}"
        print(msg)
        if log_callback:
            log_callback(msg)
    else:
        msg = "[Scheduler Image] No PENDING images to download."
        print(msg)
        if log_callback:
            log_callback(msg)

    retry_ok = 0
    retry_fail = 0
    retry_total = 0
    if status_filter != "PENDING":
        for attempt in range(1, 4):
            failed_rows = _fetch_failed_images(db, limit=batch_limit, domain=domain)
            if not failed_rows:
                msg = "[Scheduler Image] No FAILED images to retry."
                print(msg)
                if log_callback:
                    log_callback(msg)
                break
            msg = f"[Scheduler Image] Retry FAILED attempt {attempt}: {len(failed_rows)} image(s)"
            print(msg)
            if log_callback:
                log_callback(msg)
            ok, fail = _download_image_rows(db, failed_rows, image_dir, interval)
            retry_ok += ok
            retry_fail += fail
            retry_total += len(failed_rows)
            msg = f"[Scheduler Image] Retry FAILED attempt {attempt} done: ok={ok}, failed={fail}"
            print(msg)
            if log_callback:
                log_callback(msg)
        if retry_total:
            msg = f"[Scheduler Image] Retry FAILED total: ok={retry_ok}, failed={retry_fail}, total={retry_total}"
            print(msg)
            if log_callback:
                log_callback(msg)
    return pending_ok, pending_fail, pending_total, retry_ok, retry_fail, retry_total



async def scrape_pending_links(
    links: list,
    template: dict,
    db: Database,
    task_id: Optional[int] = None,
    detail_show_browser: bool = False,
    detail_fake_hover: bool = True,
    detail_fake_scroll: bool = True,
    detail_wait_load_min: float = 2.0,
    detail_wait_load_max: float = 5.0,
    detail_delay_min: float = 2.0,
    detail_delay_max: float = 3.0,
    log_callback=None,
    cancel_callback=None,
    max_retries: int = 2,
    stop_on_block: bool = True,
    get_more_links_callback=None,  # Callback để lấy thêm links từ DB
):
    ok_count = 0
    try:
        import database as _db_mod
        print(f"[detail observe] database module: {_db_mod.__file__}")
    except Exception:
        pass
    fail_count = 0
    total_links = len(links)
    if log_callback:
        log_callback(f"Start detail: {total_links} pending link(s)")
    if cancel_callback and cancel_callback():
        if log_callback:
            log_callback("Cancel requested before starting detail")
        return ok_count, fail_count, total_links, False

    # Mỗi task dùng profile riêng theo task_id để tránh conflict
    # Profile chính (không có suffix) chỉ dùng làm nguồn copy cookie
    base_profile_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "playwright_profile_tab3_detail"
    )
    os.makedirs(base_profile_dir, exist_ok=True)
    
    # Task dùng profile riêng của mình
    if task_id:
        profile_dir = base_profile_dir + f"_{task_id}"
    else:
        profile_dir = base_profile_dir + f"_{int(time.time())}"
    
    has_lock = False  # Không cần lock nữa vì mỗi task có profile riêng
    
    # Kiểm tra xem profile task đã có cookie chưa
    import shutil
    cookie_copied = False
    task_cookie_path = os.path.join(profile_dir, "Default", "Network", "Cookies")
    
    if os.path.exists(task_cookie_path):
        # Profile task đã có cookie, dùng luôn
        print(f"[detail] Task {task_id}: Using existing profile with cookies: {profile_dir}")
        if log_callback:
            log_callback(f"Using existing profile: playwright_profile_tab3_detail_{task_id}")
        cookie_copied = True
    else:
        # Profile task chưa có cookie, copy từ profile chính
        os.makedirs(profile_dir, exist_ok=True)
        
        # Copy các thư mục quan trọng từ profile chính
        dirs_to_copy = [
            "Default/Network",          # Chứa Cookies
            "Default/Local Storage",    # Local Storage
            "Default/Session Storage",  # Session Storage
        ]
        files_to_copy = [
            "Default/Preferences",
            "Local State",
        ]
        copy_errors = []
        
        for rel_path in dirs_to_copy:
            src = os.path.join(base_profile_dir, rel_path)
            dst = os.path.join(profile_dir, rel_path)
            try:
                if os.path.isdir(src):
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)
                    print(f"[detail] Copied {rel_path}")
                    if rel_path == "Default/Network":
                        cookie_copied = True
            except Exception as e:
                copy_errors.append(f"{rel_path}: {e}")
                print(f"[detail] Warning: cannot copy {rel_path}: {e}")
        
        for rel_path in files_to_copy:
            src = os.path.join(base_profile_dir, rel_path)
            dst = os.path.join(profile_dir, rel_path)
            try:
                if os.path.isfile(src):
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.copy2(src, dst)
            except Exception as e:
                copy_errors.append(f"{rel_path}: {e}")
                print(f"[detail] Warning: cannot copy {rel_path}: {e}")
        
        if cookie_copied:
            print(f"[detail] Created alternate profile with cookies: {profile_dir}")
            if log_callback:
                log_callback(f"Created profile with cookies: {profile_dir}")
        else:
            print(f"[detail] WARNING: Created profile WITHOUT cookies (copy failed): {profile_dir}")
            if log_callback:
                log_callback(f"WARNING: Profile has NO cookies - may fail to access protected pages!")
            if copy_errors:
                print(f"[detail] Copy errors: {copy_errors}")
    
    # Tắt managed_browser để Crawl4AI dùng trực tiếp browser, không tạo browser ẩn riêng
    use_managed_browser = False
    print(f"[detail observe] Using profile: {profile_dir}")

    try:
        async with WebScraper(
            headless=not detail_show_browser,
            verbose=False,
            keep_open=True,
            user_data_dir=profile_dir,
            use_managed_browser=use_managed_browser
        ) as scraper:
            shared_page = None

            # Khởi tạo browser bằng cách fetch một trang đơn giản
            try:
                if detail_show_browser or detail_fake_scroll or detail_fake_hover:
                    print("[detail observe] Warming up browser...")
                    if log_callback:
                        log_callback("Warming up browser with example.com...")
                    
                    # Thử warmup, có retry nếu thất bại
                    warmup_success = False
                    for warmup_attempt in range(3):
                        try:
                            await scraper.scrape_simple("https://example.com", bypass_cache=True)
                            warmup_success = True
                            break
                        except Exception as warmup_err:
                            print(f"[detail observe] Warmup attempt {warmup_attempt + 1} failed: {warmup_err}")
                            if warmup_attempt < 2:
                                await asyncio.sleep(2)
                    
                    if not warmup_success:
                        print("[detail observe] All warmup attempts failed")
                        if log_callback:
                            log_callback("ERROR: Warmup failed after 3 attempts")
                    
                    # Lấy page thực sự từ Crawl4AI
                    shared_page = await scraper.get_active_page()
                    if shared_page:
                        scraper.display_page = shared_page
                        # Log thông tin page để debug
                        try:
                            page_url = await shared_page.evaluate("window.location.href")
                            print(f"[detail observe] Got active page from profile {profile_dir}, URL: {page_url}")
                            if log_callback:
                                log_callback(f"Browser ready, got active page (profile: playwright_profile_tab3_detail_{task_id})")
                        except Exception as url_err:
                            print(f"[detail observe] Got active page but cannot get URL: {url_err}")
                            if log_callback:
                                log_callback(f"Browser ready, got active page")
                        try:
                            await shared_page.bring_to_front()
                        except Exception:
                            pass
                    else:
                        print("[detail observe] WARNING: Could not get active page from Crawl4AI")
                        if log_callback:
                            log_callback("WARNING: Could not get active page - browser may not work!")
            except Exception as e:
                print(f"[detail observe] Warmup error: {e}")
                if log_callback:
                    log_callback(f"Warmup error: {e}")

            if detail_show_browser and shared_page is None:
                if log_callback:
                    log_callback("[detail observe] STOP - cannot create display_page; stopping detail stage")
                return ok_count, fail_count, total_links, False

            # Hiển thị thông báo sẵn sàng
            if shared_page:
                try:
                    await shared_page.goto("data:text/html,<html><body style='background:#111;color:#0f0;font-size:28px;font-family:sans-serif;padding:50px'>CRAWL WINDOW READY - Starting...</body></html>")
                    await shared_page.bring_to_front()
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

            def _is_cloudflare_block(html_text: str, err_text: str, status_code: Optional[str]) -> bool:
                text = (html_text or "")[:5000].lower()
                err_lower = (err_text or "").lower()
                
                # Pattern phải cụ thể hơn để tránh false positive
                # Ví dụ: footer có "Protected by Cloudflare" không phải là block
                block_patterns = [
                    "attention required! | cloudflare",  # Title khi bị block
                    "cf-error-details",
                    "cf-chl-bypass", 
                    "verify you are human",
                    "checking your browser",
                    "just a moment...",  # Cloudflare challenge page
                    "ray id:",  # Cloudflare block page có Ray ID
                    "please wait while we verify",
                    "enable javascript and cookies",
                ]
                
                # Kiểm tra xem có phải trang block thực sự không
                # Chỉ return True nếu match pattern block cụ thể
                for p in block_patterns:
                    if p in text:
                        return True
                    if p in err_lower:
                        return True
                
                # Status code 403/503/429 CHỈ khi không có nội dung thực
                # Nếu có HTML content dài (>1000 chars) thì có thể là trang bình thường
                if status_code in ("403", "503", "429"):
                    if len(html_text or "") < 1000:
                        return True
                
                return False

            try:
                # Dùng loop lấy links từ callback thay vì dùng list cố định
                # Như vậy mỗi lần chỉ "giữ" 10 links IN_PROGRESS
                current_links = list(links)  # Copy list ban đầu
                global_idx = 0  # Đếm tổng số link đã xử lý
                
                while current_links:
                    for link in current_links:
                        global_idx += 1
                        if cancel_callback and cancel_callback():
                            if log_callback:
                                log_callback(f"Cancel requested mid-run at link {global_idx}, ok={ok_count}, fail={fail_count}")
                            return ok_count, fail_count, total_links, False

                        url = link.get('url')
                        link_id = link.get('id')
                        if not url:
                            db.add_scheduler_log(None, "detail", "SKIP", "Empty URL")
                            continue

                        try:
                            if log_callback:
                                log_callback(f"[Link {global_idx}] Crawling detail: {url[:120]}")

                            attempt = 1
                            while attempt <= max_retries:
                                if cancel_callback and cancel_callback():
                                    if log_callback:
                                        log_callback(f"Cancel requested mid-run at link {global_idx}, ok={ok_count}, fail={fail_count}")
                                    return ok_count, fail_count, total_links, False

                                if detail_delay_min is not None and detail_delay_max is not None and detail_delay_max >= detail_delay_min:
                                    wait_time = random.uniform(detail_delay_min, detail_delay_max)
                                    if wait_time > 0:
                                        ok_sleep = await _sleep_with_cancel(wait_time, cancel_callback)
                                        if not ok_sleep:
                                            if log_callback:
                                                log_callback(f"Cancel requested during delay at link {global_idx}, ok={ok_count}, fail={fail_count}")
                                            return ok_count, fail_count, total_links, False

                                # Kiểm tra và khôi phục page nếu bị đóng
                                if shared_page is None:
                                    shared_page = await scraper.get_active_page()
                                    if shared_page is None:
                                        print(f"[detail] Cannot get page, browser may be closed. Stopping task.")
                                        if log_callback:
                                            log_callback(f"[Link {global_idx}] STOP - Browser closed, cannot continue")
                                        return ok_count, fail_count, total_links, False

                                # Kiểm tra page còn sống không
                                try:
                                    if shared_page:
                                        _ = shared_page.url
                                except Exception as page_err:
                                    print(f"[detail] Page closed ({page_err}), trying to get new page...")
                                    shared_page = await scraper.get_active_page()
                                    if shared_page is None:
                                        print(f"[detail] Cannot recover page, browser context may be closed. Stopping task.")
                                        if log_callback:
                                            log_callback(f"[Link {global_idx}] STOP - Cannot recover browser, stopping")
                                        return ok_count, fail_count, total_links, False

                                scraper.display_page = shared_page
                                
                                # Log trạng thái display_page trước khi scrape
                                if shared_page is None:
                                    print(f"[detail] WARNING: display_page is None before scrape_url!")
                                    if log_callback:
                                        log_callback(f"[Link {global_idx}] WARNING: display_page is None - will use headless mode")

                                result = await scrape_url(
                                    url,
                                    template,
                                    scraper=scraper,
                                    wait_load_min=detail_wait_load_min,
                                    wait_load_max=detail_wait_load_max,
                                    show_browser=detail_show_browser,
                                    fake_scroll=bool(detail_fake_scroll),
                                    fake_hover=bool(detail_fake_hover),
                                )
                                if cancel_callback and cancel_callback():
                                    if log_callback:
                                        log_callback(f"Cancel requested after scrape at link {global_idx}, ok={ok_count}, fail={fail_count}")
                                    return ok_count, fail_count, total_links, False

                                last_error = result.get('error')
                                html_content = result.get('html') if result.get('success') else ""

                                if last_error and ("closed" in str(last_error).lower() or "target" in str(last_error).lower()):
                                    print(f"[detail] Browser/page closed error, will retry with new page")
                                    shared_page = None
                                    scraper.display_page = None
                                    if attempt < max_retries:
                                        attempt += 1
                                        ok_sleep = await _sleep_with_cancel(2, cancel_callback)
                                        if not ok_sleep:
                                            if log_callback:
                                                log_callback(f"Cancel requested during retry delay at link {global_idx}, ok={ok_count}, fail={fail_count}")
                                            return ok_count, fail_count, total_links, False
                                        continue

                                status_code = None
                                if last_error:
                                    import re as _re
                                    m = _re.search(r"\b(4\d{2}|5\d{2})\b", str(last_error))
                                    if m:
                                        status_code = m.group(1)

                                if _is_cloudflare_block(html_content, last_error, status_code) and stop_on_block:
                                    db.update_link_status(url, 'ERROR')
                                    fail_count += 1
                                    if log_callback:
                                        log_callback(f"[Link {global_idx}] CANCEL Cloudflare/anti-bot detected (code={status_code or 'n/a'})")
                                    return ok_count, fail_count, total_links, True

                                if result.get('success'):
                                    data = result.get('data')
                                    print(f"[detail] Saving to DB: url={url[:60]}, data_keys={list(data.keys()) if data else 'None'}")
                                    detail_id = db.add_scraped_detail_flat(
                                        url=url,
                                        data=data,
                                        domain=link.get('domain'),
                                        link_id=link_id
                                    )
                                    print(f"[detail] Saved detail_id={detail_id}")
                                    imgs = data.get('img') if isinstance(data, dict) else None
                                    if detail_id and imgs:
                                        if isinstance(imgs, list):
                                            db.add_detail_images(detail_id, imgs)
                                        elif isinstance(imgs, str):
                                            db.add_detail_images(detail_id, [imgs])
                                    db.update_link_status(url, 'CRAWLED')
                                    ok_count += 1
                                    if log_callback:
                                        log_callback(f"[Link {global_idx}] OK - saved detail_id={detail_id}")
                                    break
                                else:
                                    should_retry = attempt < max_retries
                                    if status_code and status_code.startswith("4") and status_code not in ("429",):
                                        should_retry = False
                                    if log_callback:
                                        log_callback(f"[Link {global_idx}] FAIL{f' HTTP {status_code}' if status_code else ''}: {last_error or 'Unknown error'}" + (f" (retry {attempt}/{max_retries})" if should_retry else ""))
                                    if not should_retry:
                                        db.update_link_status(url, 'ERROR')
                                        fail_count += 1
                                        break
                                    attempt += 1
                                    ok_sleep = await _sleep_with_cancel(random.uniform(1, 5), cancel_callback)
                                    if not ok_sleep:
                                        if log_callback:
                                            log_callback(f"Cancel requested during retry delay at link {global_idx}, ok={ok_count}, fail={fail_count}")
                                        return ok_count, fail_count, total_links, False
                        except Exception as e:
                            db.update_link_status(url, 'ERROR')
                            fail_count += 1
                            if log_callback:
                                log_callback(f"[Link {global_idx}] ERROR: {e}")
                    
                    # Sau khi xử lý xong batch, lấy thêm links nếu có callback
                    if get_more_links_callback:
                        current_links = get_more_links_callback()
                        if current_links:
                            total_links += len(current_links)
                            if log_callback:
                                log_callback(f"Fetched {len(current_links)} more pending links...")
                    else:
                        # Không có callback, dừng sau batch đầu tiên
                        current_links = []
            finally:
                # KHÔNG đóng shared_page ở đây vì:
                # 1. WebScraper context manager (__aexit__) sẽ tự xử lý khi keep_open=False
                # 2. Khi keep_open=True, browser được giữ mở cho đến khi close() được gọi
                # 3. Việc đóng page ở đây có thể gây lỗi nếu Crawl4AI share browser giữa các instance
                # Trước đây code gọi shared_page.close() gây đóng nhầm browser của task khác
                if shared_page and log_callback:
                    try:
                        page_url = await shared_page.evaluate("window.location.href")
                        log_callback(f"Task finished, browser page at: {page_url[:80] if page_url else 'N/A'}")
                    except Exception:
                        log_callback(f"Task finished, browser page already closed or inaccessible")
    finally:
        # Không cần release lock nữa vì mỗi task có profile riêng
        pass

    if log_callback:
        remaining = max(total_links - (ok_count + fail_count), 0)
        log_callback(f"Detail summary: ok={ok_count}, fail={fail_count}, remaining={remaining}")
    return ok_count, fail_count, len(links), False



def _as_bool(val, default=False):
    try:
        if val is None:
            return default
        if isinstance(val, bool):
            return val
        return bool(int(val))
    except Exception:
        return default
def run_task(db: Database, task: dict):
    task_id = task.get('id')
    name = task.get('name', 'task')
    now = datetime.now()
    print(f"[Scheduler] Run task {task_id} - {name}")
    finish_reason = "Completed"

    # Heartbeat: cập nhật updated_at định kỳ để tránh bị reset bởi reset_stale_running_tasks()
    _last_heartbeat = [time.time()]
    def heartbeat():
        """Cập nhật updated_at để báo task còn sống, gọi mỗi 2 phút"""
        current = time.time()
        if current - _last_heartbeat[0] >= 120:  # 2 phút
            try:
                db.update_scheduler_task(task_id, {'is_running': 1})  # Trigger UPDATE để cập nhật updated_at
                _last_heartbeat[0] = current
            except Exception:
                pass

    def is_cancel_requested() -> bool:
        try:
            heartbeat()  # Gọi heartbeat mỗi lần check cancel
            return db.is_task_cancel_requested(task_id)
        except Exception:
            return False

    def finish_cancel(msg: str):
        try:
            next_run = compute_next_run(task, now)
        except Exception:
            next_run = now + timedelta(minutes=int(task.get('interval_minutes') or 30))
        try:
            db.update_task_run(task_id, now, next_run)
        except Exception:
            pass
        db.update_scheduler_task(task_id, {'is_running': 0, 'cancel_requested': 0, 'run_now': 0})
        db.add_scheduler_log(task_id, "task", "CANCEL", f"{msg}. Next run at {next_run}")

    if is_cancel_requested():
        finish_cancel("Cancel requested before start")
        return
    if task.get('run_now'):
        db.update_scheduler_task(task_id, {'run_now': 0})
        db.add_scheduler_log(task_id, "task", "RUN_NOW", "Run now acknowledged")
    db.update_scheduler_task(task_id, {'is_running': 1, 'cancel_requested': 0})
    db.add_scheduler_log(task_id, "task", "START", f"Start {name}")
    try:
        # Stage 1: listing
        try:
            print("[Scheduler] Stage listing")
            if not _as_bool(task.get('enable_listing', 1)):
                db.add_scheduler_log(task_id, "listing", "SKIP", "Disabled")
            elif task.get('listing_template_path') and task.get('start_url'):
                template = load_json(task.get('listing_template_path'))
                if template:
                    try:
                        item_selector = template.get('itemSelector') or ''
                        next_selector = template.get('nextPageSelector') or ''
                        print(f"[Scheduler Listing] Template: {task.get('listing_template_path')}")
                        print(f"[Scheduler Listing] itemSelector: {item_selector}")
                        print(f"[Scheduler Listing] nextPageSelector: {next_selector}")
                        db.add_scheduler_log(task_id, "listing", "INFO", f"Start URL: {task.get('start_url')}")
                    except Exception:
                        pass
                    def _log_listing(msg: str):
                        db.add_scheduler_log(task_id, "listing", "INFO", msg)

                    res = run_async_safe(
                            crawl_listing(
                                task.get('start_url'),
                                template,
                                int(task.get('max_pages') or 1),
                                db,
                                None,
                                log_callback=_log_listing,
                                domain=task.get('domain'),
                                loaihinh=task.get('loaihinh'),
                                trade_type=task.get('trade_type'),
                                city_id=task.get('city_id'),
                            city_name=task.get('city_name'),
                            ward_id=task.get('ward_id'),
                            ward_name=task.get('ward_name'),
                            new_city_id=task.get('new_city_id'),
                            new_city_name=task.get('new_city_name'),
                            new_ward_id=task.get('new_ward_id'),
                            new_ward_name=task.get('new_ward_name'),
                            show_browser=bool(int(task.get('listing_show_browser') or 0)),
                            enable_fake_scroll=bool(int(task.get('listing_fake_scroll') or 1)),
                            enable_fake_hover=bool(int(task.get('listing_fake_hover') or 0)),
                            wait_load_min=float(task.get('listing_wait_load_min') or 20),
                            wait_load_max=float(task.get('listing_wait_load_max') or 30),
                            wait_next_min=float(task.get('listing_wait_next_min') or 10),
                            wait_next_max=float(task.get('listing_wait_next_max') or 20),
                            profile_suffix=str(task_id) if task_id else None,
                            cancel_callback=is_cancel_requested,
                        )
                    )
                    if res.get('canceled'):
                        db.add_scheduler_log(
                            task_id,
                            "listing",
                            "CANCEL",
                            f"Canceled at page {res.get('pages_crawled', 0)} with {res.get('total_links', 0)} links collected"
                        )
                        finish_cancel("Canceled during listing stage")
                        return
                    else:
                        db.add_scheduler_log(
                            task_id,
                            "listing",
                            "OK",
                            f"Found {res.get('total_links', 0)} links across {res.get('pages_crawled', 0)} pages"
                        )
                else:
                    db.add_scheduler_log(task_id, "listing", "SKIP", "Template not found")
            else:
                db.add_scheduler_log(task_id, "listing", "SKIP", "Missing template or start_url")
        except Exception as e:
            db.add_scheduler_log(task_id, "listing", "ERROR", str(e))
            if is_cancel_requested():
                finish_cancel("Canceled during listing stage")
                return

        # Stage 2: detail
        try:
            print("[Scheduler] Stage detail")
            if is_cancel_requested():
                finish_cancel("Cancel requested before detail stage")
                return
            if not _as_bool(task.get('enable_detail', 1)):
                db.add_scheduler_log(task_id, "detail", "SKIP", "Disabled")
            elif task.get('detail_template_path'):
                template = load_json(task.get('detail_template_path'))
                if template:
                    total_ok = 0
                    total_fail = 0
                    total_seen = 0
                    
                    # Lấy batch nhỏ links (10 links/lần) để chia sẻ với các task khác
                    # Task sẽ loop lấy thêm links đến khi hết pending
                    BATCH_SIZE = 10
                    all_pending = db.get_pending_links(
                        limit=BATCH_SIZE,
                        domain=task.get('domain'),
                        loaihinh=task.get('loaihinh'),
                        trade_type=task.get('trade_type'),
                    )
                    
                    if not all_pending:
                        db.add_scheduler_log(task_id, "detail", "SKIP", "No PENDING links")
                        finish_reason = "No pending links"
                    else:
                        total_seen = len(all_pending)
                        db.add_scheduler_log(task_id, "detail", "INFO", f"Found {total_seen} pending links (batch {BATCH_SIZE}), starting with 1 browser...")

                        def _log_detail(msg: str):
                            db.add_scheduler_log(task_id, "detail", "INFO", msg)
                        
                        # Callback để lấy thêm links từ DB (10 links mỗi lần)
                        # Như vậy mỗi lần chỉ "giữ" 10 links IN_PROGRESS, task khác vẫn có thể lấy được
                        def _get_more_links():
                            return db.get_pending_links(
                                limit=BATCH_SIZE,
                                domain=task.get('domain'),
                                loaihinh=task.get('loaihinh'),
                                trade_type=task.get('trade_type'),
                            )

                        # Chạy với callback để lấy thêm links liên tục
                        # Browser mở 1 lần, loop lấy links đến khi hết
                        ok, fail, total, blocked = run_async_safe(
                            scrape_pending_links(
                                all_pending,
                                template,
                                db,
                                task_id=task_id,
                                detail_show_browser=bool(task.get('detail_show_browser', 0)),
                                detail_fake_hover=bool(task.get('detail_fake_hover', 1)),
                                detail_fake_scroll=bool(task.get('detail_fake_scroll', 1)),
                                detail_wait_load_min=float(task.get('detail_wait_load_min') or 2),
                                detail_wait_load_max=float(task.get('detail_wait_load_max') or 5),
                                detail_delay_min=float(task.get('detail_delay_min') or 2),
                                detail_delay_max=float(task.get('detail_delay_max') or 3),
                                log_callback=_log_detail,
                                cancel_callback=is_cancel_requested,
                                max_retries=2,
                                stop_on_block=True,
                                get_more_links_callback=_get_more_links,  # Lấy thêm 10 links sau mỗi batch
                            )
                        )
                        total_ok = ok
                        total_fail = fail
                        
                        if blocked:
                            db.add_scheduler_log(task_id, "detail", "CANCEL", f"Blocked (Cloudflare/anti-bot). ok={ok}, fail={fail}")
                            finish_reason = "Blocked (Cloudflare/anti-bot)"
                            finish_cancel("Cloudflare/anti-bot block detected")
                            return
                        if is_cancel_requested():
                            db.add_scheduler_log(task_id, "detail", "CANCEL", f"Canceled after ok={ok}, fail={fail}")
                            finish_reason = "Canceled by user"
                            finish_cancel("Canceled during detail stage")
                            return
                        
                        db.add_scheduler_log(task_id, "detail", "OK", f"Total scraped: ok={total_ok}, fail={total_fail}")
                        finish_reason = "Detail batch finished"
                else:
                    db.add_scheduler_log(task_id, "detail", "SKIP", "Template not found")
                    finish_reason = "Detail template not found"
            else:
                db.add_scheduler_log(task_id, "detail", "SKIP", "No detail template")
                finish_reason = "No detail template"
        except Exception as e:
            db.add_scheduler_log(task_id, "detail", "ERROR", str(e))
            finish_reason = f"Detail error: {e}"
            if is_cancel_requested():
                finish_cancel("Canceled during detail stage")
                return

        # Stage 3: images (optional)
        try:
            print("[Scheduler] Stage image")
            if is_cancel_requested():
                finish_cancel("Cancel requested before image stage")
                return
            if not task.get('enable_image', 0):
                db.add_scheduler_log(task_id, "image", "SKIP", "Disabled")
            else:
                image_dir = task.get('image_dir')
                if image_dir:
                    def _log_image(msg: str):
                        db.add_scheduler_log(task_id, "image", "INFO", msg)

                    ok, fail, total, retry_ok, retry_fail, retry_total = download_images(
                        db,
                        image_dir,
                        int(task.get('images_per_minute') or 30),
                        domain=task.get('image_domain'),
                        status=task.get('image_status'),
                        log_callback=_log_image,
                    )
                    if total == 0 and retry_total == 0:
                        db.add_scheduler_log(task_id, "image", "SKIP", "No PENDING or FAILED images")
                    else:
                        summary = f"Pending {ok}/{total}, failed {fail}; Retry {retry_ok}/{retry_total}, failed {retry_fail}"
                        db.add_scheduler_log(task_id, "image", "OK", summary)
                else:
                    db.add_scheduler_log(task_id, "image", "SKIP", "No image_dir")
        except Exception as e:
            db.add_scheduler_log(task_id, "image", "ERROR", str(e))
            finish_reason = f"Image error: {e}"

        next_run = compute_next_run(task, now)
        db.update_task_run(task_id, now, next_run)
        db.add_scheduler_log(task_id, "task", "DONE", f"{finish_reason}. Next run at {next_run}")
        send_telegram_message(f"Task {name} done. Next: {next_run}")
    finally:
        db.update_scheduler_task(task_id, {'is_running': 0, 'cancel_requested': 0, 'run_now': 0})


def maybe_daily_report(db: Database, output_dir: str = "output/reports"):
    if os.getenv("ENABLE_DAILY_REPORT") != "1":
        return
    os.makedirs(output_dir, exist_ok=True)
    marker = os.path.join(output_dir, "last_report_date.txt")
    today = date.today().isoformat()
    if os.path.exists(marker):
        last = open(marker, "r", encoding="utf-8").read().strip()
        if last == today:
            return
    if datetime.now().hour < 23:
        return
    # Export CSV of today's scraped_details_flat
    conn = db.get_connection()
    cursor = conn.cursor()
    start = datetime.combine(date.today(), datetime.min.time())
    end = start + timedelta(days=1)
    cursor.execute('''
        SELECT id, url, title, khoanggia, dientich, ngaydang, ngayhethan, diachi, created_at
        FROM scraped_details_flat
        WHERE created_at >= %s AND created_at < %s
        ORDER BY id DESC
    ''', (start, end))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    report_path = os.path.join(output_dir, f"report_{today}.csv")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("id,url,title,khoanggia,dientich,ngaydang,ngayhethan,diachi,created_at\n")
        for row in rows:
            if isinstance(row, tuple):
                line = [str(c).replace(",", " ") if c is not None else "" for c in row]
                f.write(",".join(line) + "\n")
    with open(marker, "w", encoding="utf-8") as f:
        f.write(today)


def run_scheduler_loop():
    db = Database(host="localhost", user="root", password="", database="craw_db")
    worker_threads = []

    def launch_task_thread(task_dict: dict):
        # Mỗi task chạy thread riêng, mỗi thread tự mở kết nối DB để tránh tranh chấp.
        t_task = dict(task_dict) if isinstance(task_dict, dict) else task_dict
        def _worker():
            task_id = t_task.get('id')
            task_name = t_task.get('name')
            print(f"[Worker] Starting task {task_id} - {task_name}")
            db_worker = Database(host="localhost", user="root", password="", database="craw_db")
            try:
                run_task(db_worker, t_task)
                print(f"[Worker] Finished task {task_id} - {task_name}")
            except Exception as e:
                import traceback
                print(f"[Worker] Task {task_id} crashed: {e}")
                traceback.print_exc()
                try:
                    db_worker.add_scheduler_log(task_id, "task", "ERROR", f"Worker crashed: {e}")
                    db_worker.update_scheduler_task(task_id, {'is_running': 0, 'cancel_requested': 0})
                except Exception:
                    pass
        th = threading.Thread(target=_worker, daemon=True)
        th.start()
        worker_threads.append(th)

    def reset_stale_running_tasks():
        """Reset các task bị kẹt trạng thái is_running=1 quá 60 phút (tăng từ 5 phút để tránh reset task đang chạy lâu)"""
        try:
            conn = db.get_connection()
            cursor = conn.cursor()
            # Tìm các task đang running nhưng updated_at quá 60 phút trước (task chạy lâu như listing nhiều trang)
            cursor.execute('''
                UPDATE scheduler_tasks 
                SET is_running = 0, cancel_requested = 0 
                WHERE is_running = 1 
                  AND updated_at < DATE_SUB(NOW(), INTERVAL 60 MINUTE)
            ''')
            affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            if affected > 0:
                print(f"[Scheduler] Reset {affected} stale running task(s)")
        except Exception as e:
            print(f"[Scheduler] Error resetting stale tasks: {e}")

    print("[Scheduler] Starting main loop...")
    while not _shutdown_requested:
        try:
            # Kiểm tra shutdown flag
            if _shutdown_requested:
                break
                
            # Dọn những thread đã xong để không phình bộ nhớ
            alive_count = len([t for t in worker_threads if t.is_alive()])
            worker_threads[:] = [t for t in worker_threads if t.is_alive()]

            # Reset các task bị kẹt trạng thái "đang chạy"
            reset_stale_running_tasks()

            now = datetime.now()
            due = db.get_due_tasks(now)
            for i, task in enumerate(due):
                if _shutdown_requested:
                    break
                    
                # Double-check: Kiểm tra lại is_running trong DB trước khi pick
                # Tránh trường hợp task đang chạy bị pick lại do race condition
                try:
                    conn_check = db.get_connection()
                    cursor_check = conn_check.cursor()
                    cursor_check.execute('SELECT is_running FROM scheduler_tasks WHERE id = %s', (task['id'],))
                    row = cursor_check.fetchone()
                    cursor_check.close()
                    conn_check.close()
                    if row and row[0] == 1:
                        print(f"[Scheduler] Skip task {task['id']} - {task['name']} (already running)")
                        continue
                except Exception:
                    pass
                
                print(f"[Scheduler] Picking task {task['id']} - {task['name']}")
                # Đánh dấu đang chạy sớm để tránh pick trùng nếu vòng lặp kế tiếp chạy lại
                try:
                    db.update_scheduler_task(task['id'], {'is_running': 1})
                    db.add_scheduler_log(task['id'], "task", "QUEUED", "Queued for parallel run")
                except Exception:
                    pass
                launch_task_thread(task)
                # Delay 10 giây giữa các task để tránh rate limit từ website
                if i < len(due) - 1:
                    time.sleep(10)
                
            maybe_daily_report(db)
            
            # Sleep ngắn để check shutdown flag thường xuyên hơn
            for _ in range(20):  # 20 x 0.1s = 2s
                if _shutdown_requested:
                    break
                time.sleep(0.1)
        except Exception as e:
            print(f"[Scheduler] Loop error: {e}")
            import traceback
            traceback.print_exc()
            try:
                send_telegram_message(f"Scheduler error: {e}")
            except Exception:
                pass
            time.sleep(5)

    # Graceful shutdown
    print("\n[Scheduler] Shutting down...")
    # Reset all running tasks trước khi thoát
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE scheduler_tasks SET is_running = 0, cancel_requested = 0 WHERE is_running = 1")
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[Scheduler] Reset {affected} running task(s) before exit")
    except Exception:
        pass
    print("[Scheduler] Stopped.")


if __name__ == "__main__":
    lock_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".scheduler_service.lock")
    if _acquire_service_lock(lock_path):
        run_scheduler_loop()
