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


def _fetch_failed_images(db: Database, limit: int = 200):
    conn = db.get_connection()
    cursor = conn.cursor(dictionary=True) if db.use_mysql_connector else conn.cursor()
    try:
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
    log_callback=None,
):
    os.makedirs(image_dir, exist_ok=True)
    interval = 60.0 / max(images_per_minute, 1)
    try:
        db.sync_detail_image_statuses()
    except Exception:
        pass
    rows = db.get_undownloaded_detail_images(limit=batch_limit)
    pending_ok = 0
    pending_fail = 0
    pending_total = 0
    if rows:
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
    for attempt in range(1, 4):
        failed_rows = _fetch_failed_images(db, limit=batch_limit)
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

    # Dùng profile detail chính nếu đang trống; nếu đang bị dùng thì tạo profile mới (copy cookie)
    base_profile_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "playwright_profile_tab3_detail"
    )
    os.makedirs(base_profile_dir, exist_ok=True)
    profile_dir = base_profile_dir
    lock_path = os.path.join(base_profile_dir, ".in_use.lock")
    has_lock = False
    
    # Timeout cho stale lock
    LOCK_TIMEOUT_SECONDS = 5 * 60  # 5 phút
    
    # Kiểm tra và xóa stale lock
    if os.path.exists(lock_path):
        try:
            with open(lock_path, 'r') as f:
                content = f.read().strip()
            parts = content.split()
            if len(parts) >= 2:
                old_pid = int(parts[0])
                old_time = int(parts[1])
                age = int(time.time()) - old_time
                
                # Kiểm tra process còn chạy không
                process_alive = False
                try:
                    os.kill(old_pid, 0)  # Signal 0 = chỉ kiểm tra, không kill
                    process_alive = True
                except (OSError, ProcessLookupError):
                    process_alive = False
                
                # Xóa lock nếu process đã chết hoặc lock quá cũ
                if not process_alive or age > LOCK_TIMEOUT_SECONDS:
                    print(f"[detail] Removing stale lock (pid={old_pid}, age={age}s, alive={process_alive})")
                    os.remove(lock_path)
        except Exception as e:
            print(f"[detail] Error checking stale lock: {e}")
            try:
                os.remove(lock_path)
            except Exception:
                pass
    
    # Thử lấy lock cho profile chính
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, f"{os.getpid()} {int(time.time())}".encode("utf-8"))
        finally:
            os.close(fd)
        has_lock = True
        print(f"[detail] Acquired lock for profile: {base_profile_dir}")
    except FileExistsError:
        # Profile đang bị dùng, tạo profile mới và COPY COOKIE từ profile chính
        suffix = f"_{task_id}" if task_id else f"_{int(time.time())}"
        profile_dir = base_profile_dir + suffix
        
        # Copy cookie và data quan trọng từ profile chính sang profile mới
        import shutil
        if not os.path.exists(profile_dir) or not os.path.exists(os.path.join(profile_dir, "Default", "Network", "Cookies")):
            os.makedirs(profile_dir, exist_ok=True)
            # Copy các thư mục quan trọng: Network (Cookies), Local Storage, Session Storage
            dirs_to_copy = [
                "Default/Network",          # Chứa Cookies
                "Default/Local Storage",    # Local Storage
                "Default/Session Storage",  # Session Storage
            ]
            files_to_copy = [
                "Default/Preferences",
                "Local State",
            ]
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
                except Exception as e:
                    print(f"[detail] Warning: cannot copy {rel_path}: {e}")
            for rel_path in files_to_copy:
                src = os.path.join(base_profile_dir, rel_path)
                dst = os.path.join(profile_dir, rel_path)
                try:
                    if os.path.isfile(src):
                        os.makedirs(os.path.dirname(dst), exist_ok=True)
                        shutil.copy2(src, dst)
                except Exception as e:
                    print(f"[detail] Warning: cannot copy {rel_path}: {e}")
            print(f"[detail] Created alternate profile with cookies: {profile_dir}")
        else:
            print(f"[detail] Using existing alternate profile: {profile_dir}")
    
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
                    await scraper.scrape_simple("https://example.com", bypass_cache=True)
                    # Lấy page thực sự từ Crawl4AI
                    shared_page = await scraper.get_active_page()
                    if shared_page:
                        scraper.display_page = shared_page
                        print(f"[detail observe] Got active page: {shared_page}")
                        try:
                            await shared_page.bring_to_front()
                        except Exception:
                            pass
                    else:
                        print("[detail observe] WARNING: Could not get active page from Crawl4AI")
            except Exception as e:
                print(f"[detail observe] Warmup error: {e}")

            if detail_show_browser and shared_page is None:
                if log_callback:
                    log_callback("[detail observe] cannot create display_page; stopping detail stage")
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
                patterns = [
                    "attention required",
                    "cloudflare",
                    "cf-error-details",
                    "cf-chl-bypass",
                    "verify you are human",
                    "checking your browser",
                ]
                if any(p in text for p in patterns):
                    return True
                if any(p in err_lower for p in patterns):
                    return True
                if status_code in ("403", "503", "429"):
                    return True
                return False

            try:
                for idx, link in enumerate(links, 1):
                    if cancel_callback and cancel_callback():
                        if log_callback:
                            remaining = total_links - (ok_count + fail_count)
                            log_callback(f"Cancel requested mid-run at {idx}/{total_links}, ok={ok_count}, fail={fail_count}, remaining={remaining}")
                        return ok_count, fail_count, total_links, False

                    url = link.get('url')
                    link_id = link.get('id')
                    if not url:
                        db.add_scheduler_log(None, "detail", "SKIP", "Empty URL")
                        continue

                    try:
                        if log_callback:
                            log_callback(f"[{idx}/{total_links}] Crawling detail: {url[:120]}")

                        attempt = 1
                        while attempt <= max_retries:
                            if cancel_callback and cancel_callback():
                                if log_callback:
                                    remaining = total_links - (ok_count + fail_count)
                                    log_callback(f"Cancel requested mid-run at {idx}/{total_links}, ok={ok_count}, fail={fail_count}, remaining={remaining}")
                                return ok_count, fail_count, total_links, False

                            if detail_delay_min is not None and detail_delay_max is not None and detail_delay_max >= detail_delay_min:
                                wait_time = random.uniform(detail_delay_min, detail_delay_max)
                                if wait_time > 0:
                                    ok_sleep = await _sleep_with_cancel(wait_time, cancel_callback)
                                    if not ok_sleep:
                                        if log_callback:
                                            remaining = total_links - (ok_count + fail_count)
                                            log_callback(f"Cancel requested during delay at {idx}/{total_links}, ok={ok_count}, fail={fail_count}, remaining={remaining}")
                                        return ok_count, fail_count, total_links, False

                            if shared_page is None:
                                shared_page = await scraper.get_active_page()

                            try:
                                if shared_page:
                                    _ = shared_page.url
                            except Exception:
                                print(f"[detail] Page closed, recreating...")
                                shared_page = await scraper.get_active_page()

                            scraper.display_page = shared_page

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
                                    remaining = total_links - (ok_count + fail_count)
                                    log_callback(f"Cancel requested after scrape at {idx}/{total_links}, ok={ok_count}, fail={fail_count}, remaining={remaining}")
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
                                            remaining = total_links - (ok_count + fail_count)
                                            log_callback(f"Cancel requested during retry delay at {idx}/{total_links}, ok={ok_count}, fail={fail_count}, remaining={remaining}")
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
                                    log_callback(f"[{idx}/{total_links}] CANCEL Cloudflare/anti-bot detected (code={status_code or 'n/a'})")
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
                                    log_callback(f"[{idx}/{total_links}] OK - saved detail_id={detail_id}")
                                break
                            else:
                                should_retry = attempt < max_retries
                                if status_code and status_code.startswith("4") and status_code not in ("429",):
                                    should_retry = False
                                if log_callback:
                                    log_callback(f"[{idx}/{total_links}] FAIL{f' HTTP {status_code}' if status_code else ''}: {last_error or 'Unknown error'}" + (f" (retry {attempt}/{max_retries})" if should_retry else ""))
                                if not should_retry:
                                    db.update_link_status(url, 'ERROR')
                                    fail_count += 1
                                    break
                                attempt += 1
                                ok_sleep = await _sleep_with_cancel(random.uniform(1, 5), cancel_callback)
                                if not ok_sleep:
                                    if log_callback:
                                        remaining = total_links - (ok_count + fail_count)
                                        log_callback(f"Cancel requested during retry delay at {idx}/{total_links}, ok={ok_count}, fail={fail_count}, remaining={remaining}")
                                    return ok_count, fail_count, total_links, False
                    except Exception as e:
                        db.update_link_status(url, 'ERROR')
                        fail_count += 1
                        if log_callback:
                            log_callback(f"[{idx}/{total_links}] ERROR: {e}")
            finally:
                if shared_page:
                    try:
                        await shared_page.close()
                    except Exception:
                        pass
    finally:
        if has_lock:
            try:
                os.remove(lock_path)
            except Exception:
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

    def is_cancel_requested() -> bool:
        try:
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
                    pending = db.get_pending_links(limit=200, domain=task.get('domain'), loaihinh=task.get('loaihinh'))
                    if pending:
                        def _log_detail(msg: str):
                            db.add_scheduler_log(task_id, "detail", "INFO", msg)

                        ok, fail, total, blocked = run_async_safe(
                            scrape_pending_links(
                                pending,
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
                            )
                        )
                        if blocked:
                            remaining = max(total - (ok + fail), 0)
                            db.add_scheduler_log(task_id, "detail", "CANCEL", f"Blocked (Cloudflare/anti-bot). ok={ok}, fail={fail}, remaining={remaining}")
                            finish_cancel("Cloudflare/anti-bot block detected")
                            return
                        if is_cancel_requested():
                            remaining = max(total - (ok + fail), 0)
                            db.add_scheduler_log(task_id, "detail", "CANCEL", f"Canceled after ok={ok}, fail={fail}, remaining={remaining}")
                            finish_cancel("Canceled during detail stage")
                            return
                        db.add_scheduler_log(task_id, "detail", "OK", f"Scraped {ok}/{total}, failed {fail}")
                    else:
                        db.add_scheduler_log(task_id, "detail", "SKIP", "No PENDING links")
                else:
                    db.add_scheduler_log(task_id, "detail", "SKIP", "Template not found")
            else:
                db.add_scheduler_log(task_id, "detail", "SKIP", "No detail template")
        except Exception as e:
            db.add_scheduler_log(task_id, "detail", "ERROR", str(e))
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

        next_run = compute_next_run(task, now)
        db.update_task_run(task_id, now, next_run)
        db.add_scheduler_log(task_id, "task", "DONE", f"Next run at {next_run}")
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
        """Reset các task bị kẹt trạng thái is_running=1 quá 5 phút"""
        try:
            conn = db.get_connection()
            cursor = conn.cursor()
            # Tìm các task đang running nhưng updated_at quá 5 phút trước
            cursor.execute('''
                UPDATE scheduler_tasks 
                SET is_running = 0, cancel_requested = 0 
                WHERE is_running = 1 
                  AND updated_at < DATE_SUB(NOW(), INTERVAL 5 MINUTE)
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
                print(f"[Scheduler] Picking task {task['id']} - {task['name']}")
                # Đánh dấu đang chạy sớm để tránh pick trùng nếu vòng lặp kế tiếp chạy lại
                try:
                    db.update_scheduler_task(task['id'], {'is_running': 1})
                    db.add_scheduler_log(task['id'], "task", "QUEUED", "Queued for parallel run")
                except Exception:
                    pass
                launch_task_thread(task)
                # Delay 5 giây giữa các task để tránh mở nhiều browser cùng lúc (gây SIGINT conflict)
                if i < len(due) - 1:
                    print(f"[Scheduler] Waiting 5s before launching next task...")
                    time.sleep(5)
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
