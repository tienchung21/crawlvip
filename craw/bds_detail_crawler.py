
from curl_cffi import requests
import json
import time
import random
import os
import sys
from typing import Any

# Disable Env Proxy (Crucial for bypassing Cloudflare on this Server)
os.environ.pop("http_proxy", None)
os.environ.pop("https_proxy", None)
os.environ.pop("HTTP_PROXY", None)
os.environ.pop("HTTPS_PROXY", None)

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
    from craw.database import Database

# API
API_URL = "https://batdongsan.com.vn/microservice-architecture-router/Product/ProductDetail/GetMarkerById"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://batdongsan.com.vn",
    "Referer": "https://batdongsan.com.vn/",
    "Sec-Ch-Ua": "\"Chromium\";v=\"144\", \"Not_A Brand\";v=\"8\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    # Remove manual headers if curl_cffi handles it? User wants THIS code.
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

NET_ERROR_RETRY_AFTER_MINUTES = 60
MAX_NET_ERROR_RETRY = 3

def _is_http2_stream_error(exc: Exception) -> bool:
    msg = str(exc)
    return ("curl: (92)" in msg) or ("HTTP/2 stream" in msg) or ("INTERNAL_ERROR" in msg)

def _is_not_found_payload(resp) -> bool:
    # Some responses are plain text/HTML body that contains this marker.
    body = (resp.text or "").lower()
    if "product is not found" in body:
        return True

    # Some responses may return JSON payload with a message field.
    try:
        payload = resp.json()
    except Exception:
        return False

    if isinstance(payload, dict):
        msg = str(payload.get("message") or payload.get("Message") or "").lower()
        if "product is not found" in msg:
            return True
    return False

def _normalize_trade_type(raw_trade_type, url: str | None = None) -> str | None:
    """
    Normalize trade type to:
      - 's' (sale)
      - 'u' (rent)
    """
    if raw_trade_type is not None:
        t = str(raw_trade_type).strip().lower()
        if t in ("u", "thue", "thuê", "rent", "cho-thue", "cho_thue"):
            return "u"
        if t in ("s", "mua", "ban", "bán", "muaban", "mua-ban", "mua_ban", "sale"):
            return "s"

    if url:
        u = str(url).lower()
        if "/cho-thue-" in u or "/cho-thue" in u:
            return "u"
        if "/ban-" in u or "/ban" in u:
            return "s"

    return None


def _extract_balanced_json_object(text: str) -> str | None:
    """
    Try to extract the first balanced JSON object from a noisy/truncated text.
    """
    if not text:
        return None
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_str = False
    escaped = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == "\"":
                in_str = False
            continue
        if ch == "\"":
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _safe_parse_json_response(resp: Any) -> dict:
    """
    Parse JSON more defensively for occasional malformed/truncated bodies.
    """
    # Fast path
    try:
        obj = resp.json()
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    text = ""
    try:
        text = resp.text or ""
    except Exception:
        text = ""
    if not text:
        try:
            raw = resp.content or b""
            text = raw.decode("utf-8", errors="ignore")
        except Exception:
            text = ""
    if text:
        # Remove NUL which can break json parser
        text = text.replace("\x00", "")
        # Try normal json decode first
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        # Try non-strict parser for control chars
        try:
            obj = json.loads(text, strict=False)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        # Last chance: extract balanced object from noisy payload
        extracted = _extract_balanced_json_object(text)
        if extracted:
            obj = json.loads(extracted, strict=False)
            if isinstance(obj, dict):
                return obj

    raise ValueError("invalid_json_payload")

def main():
    print("Starting BDS Detail Crawler (API)...")
    db = Database()
    conn = db.get_connection()
    if not conn:
        print("DB Failed.")
        return
    cur = conn.cursor()

    # Check Schema (Status + retry_count)
    cur.execute("SHOW COLUMNS FROM collected_links LIKE 'status'")
    if not cur.fetchone():
         print("Adding status column...")
         cur.execute("ALTER TABLE collected_links ADD COLUMN status VARCHAR(20) DEFAULT 'new'")
         conn.commit()
    cur.execute("SHOW COLUMNS FROM collected_links LIKE 'retry_count'")
    if not cur.fetchone():
         print("Adding retry_count column...")
         cur.execute("ALTER TABLE collected_links ADD COLUMN retry_count INT DEFAULT 0")
         conn.commit()

    # Check Schema (Locations in scraped_details_flat)
    print("Checking Details Schema...")
    for col, type_def in [
        ("city_code", "VARCHAR(10)"),
        ("district_id", "INT"),
        ("ward_id", "INT")
    ]:
        cur.execute(f"SHOW COLUMNS FROM scraped_details_flat LIKE '{col}'")
        if not cur.fetchone():
             print(f"Adding {col} column...")
             cur.execute(f"ALTER TABLE scraped_details_flat ADD COLUMN {col} {type_def}")
             conn.commit()

    # Check Schema (Details Extra)
    print("Checking Details Extra Schema...")
    for col, type_def in [
        ("sophongngu", "INT"),
        ("sophongvesinh", "INT"),
        ("loaitin", "VARCHAR(50)"),
        ("loaihinh", "VARCHAR(255)"),
        ("trade_type", "VARCHAR(50)")
    ]:
        cur.execute(f"SHOW COLUMNS FROM scraped_details_flat LIKE '{col}'")
        if not cur.fetchone():
             print(f"Adding {col} column...")
             cur.execute(f"ALTER TABLE scraped_details_flat ADD COLUMN {col} {type_def}")
             conn.commit()

    # 1. Fetch IDs (Newest First)
    # Exclude IDs already in scraped_details_flat?
    # For now, just fetch all from collected_links with prj_id IS NOT NULL
    # and maybe check existence in scraped_details_flat to skip effectively.
    
    # Loop Indefinitely
    while True:
        # We select Batch of IDs.
        limit = 1000
        print(f"Fetching {limit} IDs from collected_links (Newest batch first)...")

        # Always prioritize latest sitemap batch_date first, but fallback
        # to older batches when newest has no PENDING rows left.
        cur.execute(
            """
            SELECT DISTINCT batch_date
            FROM collected_links
            WHERE domain = 'batdongsan.com.vn'
              AND batch_date IS NOT NULL
              AND prj_id IS NOT NULL
              AND prj_id <> 0
              AND COALESCE(status, 'PENDING') NOT IN ('done', 'failed', 'failed_500', 'expired', 'net_error', 'blocked_cf', 'POSTAGAIN')
            ORDER BY batch_date DESC
            LIMIT 365
            """
        )
        batch_rows = cur.fetchall()
        candidate_batches = []
        for b in batch_rows:
            if isinstance(b, dict):
                candidate_batches.append(b.get("batch_date"))
            else:
                candidate_batches.append(b[0])
        print(f"Candidate batches (new -> old): {candidate_batches[:6]}{'...' if len(candidate_batches) > 6 else ''}")
        
        # Join with scraped_details_flat to find MISSING
        # Also Select loaihinh from collected_links
        query = """
            SELECT c.id, c.url, c.prj_id, c.batch_date, c.loaihinh, c.trade_type, COALESCE(c.crawl_bo_sung, 0) AS crawl_bo_sung
            FROM collected_links c
            LEFT JOIN scraped_details_flat s ON c.prj_id = s.link_id AND s.domain = 'batdongsan.com.vn'
            WHERE c.domain = 'batdongsan.com.vn' 
              AND c.prj_id IS NOT NULL 
              AND c.prj_id != 0
              AND c.batch_date = %s
              -- Do not re-crawl dead IDs / blocked IDs / temporary network failures.
              -- This avoids looping old broken IDs and helps move on to fresh IDs.
              -- Do not crawl POSTAGAIN per operator request.
              AND COALESCE(c.status, 'PENDING') NOT IN ('done', 'failed', 'failed_500', 'expired', 'net_error', 'blocked_cf', 'POSTAGAIN')
              AND s.id IS NULL
            ORDER BY COALESCE(c.crawl_bo_sung, 0) DESC, c.batch_date DESC, c.prj_id DESC
            LIMIT %s
        """
        rows = []
        selected_batch = None
        for batch_date in candidate_batches:
            cur.execute(query, (batch_date, limit))
            found = cur.fetchall()
            if found:
                rows = found
                selected_batch = batch_date
                break

        if selected_batch:
            print(f"Selected batch_date: {selected_batch}")
        print(f"Found {len(rows)} new listings to crawl.")
        
        if not rows:
            print("No new listings found. Sleeping 15 minutes...")
            time.sleep(15 * 60)
            continue

        consecutive_500 = 0
        i = 0
        while i < len(rows):
            row = rows[i]
            # Handle row being dict or tuple
            if isinstance(row, dict):
                c_id = row['id']
                url = row['url']
                prj_id = row['prj_id']
                batch_date = row['batch_date']
                loaihinh = row['loaihinh']
                trade_type = row.get('trade_type')
                crawl_bo_sung = int(row.get('crawl_bo_sung') or 0)
            else:
                c_id = row[0]
                url = row[1]
                prj_id = row[2]
                batch_date = row[3]
                loaihinh = row[4]
                trade_type = row[5] if len(row) > 5 else None
                crawl_bo_sung = int(row[6]) if len(row) > 6 and row[6] is not None else 0
                
            print(f"[{i+1}/{len(rows)}] Crawling ID: {prj_id} (Batch: {batch_date})...")
            
            # Call API
            target_url = f"{API_URL}?productId={prj_id}"
            
            try:
                # Revert to chrome124 as requested
                try:
                    resp = requests.get(target_url, headers=HEADERS, impersonate="chrome124", timeout=30)
                except Exception as e:
                    # Common failure mode: HTTP/2 stream INTERNAL_ERROR (curl: 92).
                    # Fallback to HTTP/1.1 for this request to avoid repeated failures.
                    if _is_http2_stream_error(e):
                        print("  Detected HTTP/2 stream error (curl 92). Retrying with HTTP/1.1...")
                        resp = requests.get(
                            target_url,
                            headers=HEADERS,
                            impersonate="chrome124",
                            timeout=30,
                            http_version="v1",
                        )
                    else:
                        raise
                
                if resp.status_code == 200:
                    if _is_not_found_payload(resp):
                        print(f"  -> API says Product is not found. Marking ID {prj_id} as expired.")
                        cur.execute("UPDATE collected_links SET status='expired' WHERE id=%s", (c_id,))
                        conn.commit()
                        i += 1
                        continue
                    try:
                        data = _safe_parse_json_response(resp)
                        # Save to DB
                        save_to_db(cur, conn, data, c_id, url, batch_date, loaihinh, trade_type, crawl_bo_sung)
                        # Reset counter on success
                        if consecutive_500 > 0:
                            print(f"  -> Success. Resetting 500 counter (was {consecutive_500}).")
                        consecutive_500 = 0
                    except Exception as e:
                        # Retry once using HTTP/1.1 when body is malformed/truncated.
                        print(f"  JSON Error: {e} | First Content: {(resp.text or '')[:140]}")
                        try:
                            resp_retry = requests.get(
                                target_url,
                                headers=HEADERS,
                                impersonate="chrome124",
                                timeout=30,
                                http_version="v1",
                            )
                            if resp_retry.status_code == 200:
                                data_retry = _safe_parse_json_response(resp_retry)
                                save_to_db(cur, conn, data_retry, c_id, url, batch_date, loaihinh, trade_type, crawl_bo_sung)
                                if consecutive_500 > 0:
                                    print(f"  -> Success after JSON retry. Resetting 500 counter (was {consecutive_500}).")
                                consecutive_500 = 0
                            else:
                                print(f"  JSON Retry failed status={resp_retry.status_code}. Marking net_error.")
                                cur.execute("UPDATE collected_links SET status = 'net_error' WHERE id = %s", (c_id,))
                                conn.commit()
                        except Exception as e2:
                            print(f"  JSON Retry Error: {e2}. Marking net_error.")
                            cur.execute("UPDATE collected_links SET status = 'net_error' WHERE id = %s", (c_id,))
                            conn.commit()
                else:
                    print(f"  Status {resp.status_code}. Content Preview: {resp.text[:200]}")
                    print("  Skipping.")

                    # If 403, Blocked -> Wait
                    if resp.status_code == 403:
                        print("  Blocked 403. Waiting 15m...")
                        time.sleep(15 * 60)
                        # Retry this item? Or skip? Usually blocked means we need to stop.
                        # Original code waited and broke inner loop (which just moves to next batch? no, break inner loop iterates? No, break breaks the for/while)
                        # Let's keep original behavior: sleep then stop batch.
                        break 
                        
                    # If 500 -> Handle Consecutive Logic
                    if resp.status_code == 500:
                        consecutive_500 += 1
                        print(f"  -> Consecutive 500 Count: {consecutive_500}")
                        
                        if consecutive_500 == 3:
                            print("  !!! 3 Consecutive 500s. Skipping next 10 items. !!!")
                            i += 10
                        elif consecutive_500 == 6:
                            print("  !!! 6 Consecutive 500s. Skipping next 20 items. !!!")
                            i += 20
                        elif consecutive_500 == 7:
                            print("  !!! 7 Consecutive 500s. Skipping next 35 items. !!!")
                            i += 35
                        elif consecutive_500 == 8:
                            print("  !!! 8 Consecutive 500s. Skipping next 40 items. !!!")
                            i += 40
                        elif consecutive_500 > 8:
                            # Increase by 5 for each additional error
                            # 9 -> 45, 10 -> 50...
                            extra_skip = 40 + (consecutive_500 - 8) * 5
                            print(f"  !!! {consecutive_500} Consecutive 500s. Skipping next {extra_skip} items. !!!")
                            i += extra_skip
                            
                    # If 500 or 404 or others -> Mark Failed in DB
                    if resp.status_code in [500, 404, 400]:
                        status_val = 'failed'
                        if resp.status_code == 500:
                            status_val = 'failed_500'
                            print(f"  Marking ID {prj_id} as {status_val} (Permanent Skip).")
                        elif resp.status_code == 404:
                            status_val = 'expired'
                            print(f"  Marking ID {prj_id} as {status_val} (Listing removed/expired).")
                        else:
                            print(f"  Marking ID {prj_id} as {status_val}.")
                            
                        update_fail_sql = "UPDATE collected_links SET status = %s WHERE id = %s"
                        cur.execute(update_fail_sql, (status_val, c_id))
                        conn.commit()

            except Exception as e:
                print(f"  Request Error: {e}")
                # Track network/challenge retries and classify permanently blocked IDs.
                try:
                    cur.execute(
                        """
                        UPDATE collected_links
                        SET
                            retry_count = COALESCE(retry_count, 0) + 1,
                            status = CASE
                                WHEN COALESCE(retry_count, 0) + 1 >= %s THEN 'blocked_cf'
                                ELSE 'net_error'
                            END
                        WHERE id = %s
                        """,
                        (MAX_NET_ERROR_RETRY, c_id),
                    )
                    conn.commit()
                except Exception:
                    pass
                # Network/transport errors are treated similar to 500 streaks for skip logic.
                consecutive_500 += 1
                if consecutive_500 == 3:
                    print("  !!! 3 Consecutive Errors. Skipping next 10 items. !!!")
                    i += 10
                elif consecutive_500 == 6:
                    print("  !!! 6 Consecutive Errors. Skipping next 20 items. !!!")
                    i += 20
                elif consecutive_500 == 7:
                    print("  !!! 7 Consecutive Errors. Skipping next 35 items. !!!")
                    i += 35
                elif consecutive_500 == 8:
                    print("  !!! 8 Consecutive Errors. Skipping next 40 items. !!!")
                    i += 40
                elif consecutive_500 > 8:
                    extra_skip = 40 + (consecutive_500 - 8) * 5
                    print(f"  !!! {consecutive_500} Consecutive Errors. Skipping next {extra_skip} items. !!!")
                    i += extra_skip
                
            # Delay between detail requests
            delay = random.uniform(3, 5)
            print(f"  Sleeping {delay:.2f}s...")
            time.sleep(delay)
            
            # Increment index
            i += 1

        conn.commit() # Ensure commits
        print("Batch finished. Sleeping 15 minutes...")
        time.sleep(15 * 60)

    conn.close()

def save_to_db(cur, conn, data, c_id, original_url, batch_date, loaihinh_link, trade_type_link=None, crawl_bo_sung=0):
    # Mapping
    # "productId" -> matin (AND link_id for join consistency)
    # "title" -> title
    # "price" -> khoanggia
    # "area" -> dientich
    # "lastUpDateTime" -> created_at
    # "batch_date" -> ngaydang
    # "rooms" -> sophongngu
    # "toilets" -> sophongvesinh
    # "vipTypes" -> loaitin
    # collected_links.loaihinh -> loaihinh
    
    p_id = data.get("productId")
    title = data.get("title")
    price = data.get("price")
    area = data.get("area")
    
    last_update_raw = data.get("lastUpDateTime")
    # Convert ISO Date
    last_update = None
    last_update = None
    if last_update_raw:
        try:
             # Basic Parse: 2026-01-28T07:07:13.752+07:00
             from dateutil import parser
             dt = parser.parse(last_update_raw)
             if dt.year < 1900:
                 last_update = None
             else:
                 last_update = dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
             last_update = None 

             
    contact = data.get("contactName")
    mobile = data.get("contactMobile")
    api_url = data.get("url")
    thumbnail_image_url = data.get("thumbnailImageUrl")
    
    full_url = f"https://batdongsan.com.vn{api_url}" if api_url else original_url
    trade_type_norm = _normalize_trade_type(trade_type_link, full_url)
    
    # Location
    city_code = data.get("cityCode")
    dist_id = data.get("districtId")
    ward_id = data.get("wardId")

    # Extra Fields
    rooms = data.get("rooms")
    toilets = data.get("toilets")
    vip_type = data.get("vipTypes")
    
    # Insert
    # We map 'matin' = p_id
    # 'link_id' = p_id (to maintain Join logic)
    # 'created_at' = last_update (User request)
    # 'ngaydang' = batch_date by default
    # For crawl_bo_sung=1, ngaydang uses lastUpDateTime date if available.
    ngaydang_value = batch_date
    if crawl_bo_sung and last_update:
        try:
            ngaydang_value = last_update[:10].replace("-", "")
        except Exception:
            ngaydang_value = batch_date
    
    img_count = 1 if thumbnail_image_url else None

    insert_sql = """
        INSERT INTO scraped_details_flat (
            link_id, matin, title, url, domain, 
            khoanggia, dientich, ngaydang, 
            tenmoigioi, sodienthoai, 
            created_at,
            city_code, district_id, ward_id,
            sophongngu, sophongvesinh, loaitin, loaihinh, trade_type, img_count
        ) VALUES (
            %s, %s, %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, 
            %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
    """
    
    try:
        cur.execute(insert_sql, (
            p_id, p_id, title, full_url, 'batdongsan.com.vn',
            price, area, ngaydang_value,
            contact, mobile,
            last_update,
            city_code, dist_id, ward_id,
            rooms, toilets, vip_type, loaihinh_link, trade_type_norm, img_count
        ))
        conn.commit()
        print("  -> Saved to DB.")

        detail_id = cur.lastrowid
        if detail_id and thumbnail_image_url:
            db = Database()
            db.add_detail_images(detail_id, [thumbnail_image_url])
            print("  -> Saved thumbnail image to scraped_detail_images.")
        
        # Update Status in Collected Links (User Request)
        update_link_sql = "UPDATE collected_links SET status = 'done' WHERE id = %s"
        cur.execute(update_link_sql, (c_id,))
        conn.commit()
        print("  -> Updated Link Status to 'done'.")
        
    except Exception as e:
        msg = str(e)
        conn.rollback()

        if "Duplicate entry" in msg and "idx_sdf_link_unique" in msg:
            try:
                cur.execute(
                    """
                    SELECT id
                    FROM scraped_details_flat
                    WHERE link_id = %s AND domain = 'batdongsan.com.vn'
                    LIMIT 1
                    """,
                    (p_id,)
                )
                row = cur.fetchone()
                detail_id = row["id"] if isinstance(row, dict) else (row[0] if row else None)
                if detail_id and thumbnail_image_url:
                    db = Database()
                    db.add_detail_images(detail_id, [thumbnail_image_url])
                    cur.execute(
                        "UPDATE scraped_details_flat SET img_count = 1 WHERE id = %s AND (img_count IS NULL OR img_count = 0)",
                        (detail_id,)
                    )
                # Detail already exists, so mark collected_links as done to keep it out of the queue.
                cur.execute("UPDATE collected_links SET status = 'done' WHERE id = %s", (c_id,))
                conn.commit()
                print("  -> Detail already existed. Saved thumbnail image to scraped_detail_images and marked link as done.")
                return
            except Exception as img_e:
                conn.rollback()
                print(f"  -> Duplicate row found but saving thumbnail failed: {img_e}")

        print(f"  -> Save Failed: {e}")

if __name__ == "__main__":
    main()
