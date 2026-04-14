#!/usr/bin/env python3
"""
FTP Image Processor
Download images from data_full, resize, watermark, and upload to FTP.

Usage:
    python3 ftp_image_processor.py [options]

Options:
    --batch N          Number of images per batch (default: 100)
    --workers N        Number of parallel workers (default: 10)
    --max-width N      Max width for main image (default: 1100)
    --thumb-width N    Width for thumbnail (default: 750)
    --logo PATH        Path to logo file for watermark
    --logo-scale N     Logo scale percentage (default: 15)
    --logo-opacity F   Logo opacity 0.0-1.0 (default: 0.7)
    --logo-position P  Logo position: top-left, top-right, bottom-left, bottom-right, center (default: bottom-right)
    --test-connection  Test FTP connection only
    --dry-run          Process but don't upload
    --start-id N       Start from data_full ID
    --end-id N         End at data_full ID
"""

import os
import sys
import re
import time
import argparse
import logging
from io import BytesIO
from ftplib import FTP
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
from PIL import Image, PngImagePlugin

# Setup path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import Database

# ============================================
# FTP Configuration
# ============================================
FTP_CONFIG = {
    'host': 'static2.cafeland.vn',
    'username': 'uploadernhadat',
    'password': 'aUvluckHS2H6Hu',
    'remote_dir': '/cafeland/static01/sgd/cnews',  # Base directory on FTP
}
PUBLIC_BASE_URL = f"https://{FTP_CONFIG['host']}"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

ALLOWED_TABLES = {"data_full", "data_no_full"}


def _validate_table_name(table_name: str) -> str:
    table_name = (table_name or "data_full").strip()
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported table: {table_name}")
    return table_name


# ============================================
# Image Processing Functions
# ============================================

def resize_image(img: Image.Image, max_width: int) -> Image.Image:
    """Resize image to max_width maintaining aspect ratio."""
    width, height = img.size
    if width > max_width:
        new_height = max(int(height * max_width / width), 1)
        img = img.resize((max_width, new_height), Image.LANCZOS)
    return img


def apply_watermark(base_img: Image.Image, logo_img: Image.Image, 
                    position: str, scale_pct: int, opacity: float, margin: int = 20) -> Image.Image:
    """Apply watermark logo to image."""
    base = base_img.convert("RGBA")
    logo = logo_img.convert("RGBA")
    
    bw, bh = base.size
    if bw <= 0 or bh <= 0:
        return base
    
    # Scale logo
    target_w = max(int(bw * (scale_pct / 100.0)), 1)
    lw, lh = logo.size
    if lw > 0:
        target_h = max(int(lh * (target_w / lw)), 1)
    else:
        target_h = max(int(bh * (scale_pct / 100.0)), 1)
    
    logo = logo.resize((target_w, target_h), Image.LANCZOS)
    
    # Apply opacity
    if opacity < 1.0:
        alpha = logo.split()[-1]
        alpha = alpha.point(lambda p: int(p * opacity))
        logo.putalpha(alpha)
    
    # Calculate position
    x = margin
    y = margin
    if position == "top-right":
        x = max(bw - target_w - margin, margin)
        y = margin
    elif position == "bottom-left":
        x = margin
        y = max(bh - target_h - margin, margin)
    elif position == "bottom-right":
        x = max(bw - target_w - margin, margin)
        y = max(bh - target_h - margin, margin)
    elif position == "center":
        x = max(int((bw - target_w) / 2), margin)
        y = max(int((bh - target_h) / 2), margin)
    
    base.paste(logo, (x, y), logo)
    return base


def has_watermark_marker(img: Image.Image) -> bool:
    """Check if image already has watermark marker in metadata."""
    info = getattr(img, "info", {}) or {}
    marker = None
    if isinstance(info, dict):
        marker = info.get("watermarked") or info.get("Watermarked") or info.get("comment")
    if isinstance(marker, bytes):
        try:
            marker = marker.decode("utf-8", errors="ignore")
        except Exception:
            marker = None
    if marker and "WATERMARKED=1" in str(marker):
        return True
    return False


def add_watermark_marker(fmt: str, save_kwargs: dict):
    """Add watermark marker to image metadata."""
    marker_text = "WATERMARKED=1"
    if fmt.upper() == "PNG":
        try:
            pnginfo = PngImagePlugin.PngInfo()
            pnginfo.add_text("watermarked", marker_text)
            save_kwargs["pnginfo"] = pnginfo
        except Exception:
            pass
    else:
        save_kwargs["comment"] = marker_text.encode("utf-8")


# ============================================
# FTP Functions
# ============================================

def connect_ftp() -> FTP:
    """Connect to FTP server."""
    ftp = FTP()
    ftp.connect(FTP_CONFIG['host'], 21, timeout=30)
    ftp.login(FTP_CONFIG['username'], FTP_CONFIG['password'])
    ftp.set_pasv(True)
    return ftp


def ensure_ftp_dir(ftp: FTP, path: str):
    """Ensure directory exists on FTP, create if needed."""
    dirs = path.strip('/').split('/')
    current = ''
    for d in dirs:
        current = f"{current}/{d}"
        try:
            ftp.cwd(current)
        except Exception:
            try:
                ftp.mkd(current)
                ftp.cwd(current)
            except Exception:
                pass


def upload_to_ftp(ftp: FTP, file_buffer: BytesIO, remote_path: str) -> bool:
    """Upload file buffer to FTP."""
    try:
        # Ensure directory exists
        remote_dir = os.path.dirname(remote_path)
        if remote_dir:
            ensure_ftp_dir(ftp, remote_dir)
        
        # Go to root and upload
        ftp.cwd('/')
        file_buffer.seek(0)
        ftp.storbinary(f'STOR {remote_path}', file_buffer)
        return True
    except Exception as e:
        logger.error(f"FTP upload failed: {e}")
        return False


def verify_ftp_file_exists(ftp: FTP, remote_path: str) -> bool:
    """Verify the uploaded file exists on FTP and has non-zero size."""
    try:
        remote_dir = os.path.dirname(remote_path)
        filename = os.path.basename(remote_path)
        ftp.cwd(remote_dir)
        ftp.voidcmd('TYPE I')
        size = ftp.size(filename)
        ftp.cwd('/')
        return bool(size and size > 0)
    except Exception as e:
        logger.warning(f"FTP verify failed: {remote_path} ({e})")
        try:
            ftp.cwd('/')
        except Exception:
            pass
        return False


def verify_public_url_exists(web_path: str, timeout: int = 20) -> bool:
    """Verify the public CDN URL is reachable."""
    public_url = urljoin(PUBLIC_BASE_URL, web_path)
    try:
        response = requests.head(public_url, timeout=timeout, allow_redirects=True)
        if response.status_code == 200:
            return True
        if response.status_code in {403, 405}:
            response = requests.get(public_url, timeout=timeout, stream=True)
            try:
                return response.status_code == 200
            finally:
                response.close()
        return False
    except Exception as e:
        logger.warning(f"Public verify failed: {public_url} ({e})")
        return False


def verify_uploaded_file(ftp: FTP, remote_path: str, web_path: str, retries: int = 3, sleep_s: float = 1.5) -> bool:
    """Require both FTP presence and public URL reachability before marking uploaded."""
    if not verify_ftp_file_exists(ftp, remote_path):
        return False
    for attempt in range(1, retries + 1):
        if verify_public_url_exists(web_path):
            return True
        if attempt < retries:
            time.sleep(sleep_s)
    return False


# ============================================
# Database Functions
# ============================================

def get_pending_images(
    db: Database,
    start_id: int = 0,
    end_id: int = 0,
    limit: int = 100,
    offset: int = 0,
    table_name: str = "data_full",
) -> list:
    """Claim a small batch of pending images and mark them PROCESSING.

    This avoids wide UPDATE ... JOIN ... ORDER BY ... LIMIT locks on large tables.
    """
    table_name = _validate_table_name(table_name)
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # Step 1: Pick candidate image IDs (read only).
        # offset is intentionally ignored because we are claiming by status.
        sql_pick = f"""
            SELECT sdi.id
            FROM scraped_detail_images sdi
            JOIN {table_name} df ON df.id_img = sdi.detail_id
            WHERE sdi.status = 'PENDING'
        """
        params = []
        if start_id > 0:
            sql_pick += " AND df.id >= %s"
            params.append(start_id)
        if end_id > 0:
            sql_pick += " AND df.id <= %s"
            params.append(end_id)
        sql_pick += " ORDER BY df.id, sdi.idx, sdi.id LIMIT %s"
        params.append(limit)
        cursor.execute(sql_pick, params)
        picked = [int(r["id"]) for r in cursor.fetchall()]
        if not picked:
            return []

        # Step 2: Claim picked rows only.
        ph = ",".join(["%s"] * len(picked))
        sql_claim = f"""
            UPDATE scraped_detail_images
            SET status = 'PROCESSING'
            WHERE status = 'PENDING' AND id IN ({ph})
        """
        cursor.execute(sql_claim, picked)
        conn.commit()

        # Step 3: Return exactly the claimed rows.
        sql_select = f"""
            SELECT 
                df.id as listing_id,
                df.id_img,
                df.slug_name,
                sdi.id as image_id,
                sdi.image_url,
                sdi.idx
            FROM scraped_detail_images sdi
            JOIN {table_name} df ON df.id_img = sdi.detail_id
            WHERE sdi.status = 'PROCESSING'
              AND sdi.id IN ({ph})
        """
        cursor.execute(sql_select + " ORDER BY df.id, sdi.idx, sdi.id", picked)
        rows = cursor.fetchall()
        
        return rows
        
    finally:
        cursor.close()
        conn.close()


def update_image_status(db: Database, image_id: int, status: str, ftp_path: str = None, error: str = None):
    """Update image status and ftp_path in database."""
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        if ftp_path:
            cursor.execute(
                "UPDATE scraped_detail_images SET status = %s, ftp_path = %s WHERE id = %s",
                (status, ftp_path, image_id)
            )
        else:
            cursor.execute(
                "UPDATE scraped_detail_images SET status = %s WHERE id = %s",
                (status, image_id)
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to update status: {e}")
    finally:
        cursor.close()
        conn.close()


def check_and_mark_images_ready(db: Database, detail_id: int, table_name: str = "data_full"):
    """Check if all images for a listing are uploaded and mark it as IMAGES_READY."""
    table_name = _validate_table_name(table_name)
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        # Count pending/processing images
        cursor.execute("""
            SELECT COUNT(*) as pending FROM scraped_detail_images 
            WHERE detail_id = %s AND status NOT IN ('UPLOADED', 'FAILED', 'DUPLICATE')
        """, (detail_id,))
        result = cursor.fetchone()
        pending = result['pending'] if isinstance(result, dict) else result[0]
        
        if pending == 0:
            # All images done - mark listing as IMAGES_READY
            cursor.execute(f"""
                UPDATE {table_name} SET images_status = 'IMAGES_READY' 
                WHERE id_img = %s AND (images_status IS NULL OR images_status = 'PENDING')
            """, (detail_id,))
            if cursor.rowcount > 0:
                logger.info(f"  ✅ {table_name} listing id_img={detail_id} -> IMAGES_READY")
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to check images ready: {e}")
    finally:
        cursor.close()
        conn.close()


def reconcile_images_ready(db: Database, table_name: str = "data_full") -> int:
    """Backfill IMAGES_READY for rows whose images are all finished.

    This handles rows created after their images were already uploaded, where
    no worker will call `check_and_mark_images_ready(detail_id)` again.
    """
    table_name = _validate_table_name(table_name)
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            UPDATE {table_name} d
            LEFT JOIN (
                SELECT detail_id, COUNT(*) AS unfinished
                FROM scraped_detail_images
                WHERE status NOT IN ('UPLOADED', 'FAILED', 'DUPLICATE')
                GROUP BY detail_id
            ) p ON p.detail_id = d.id_img
            SET d.images_status = 'IMAGES_READY'
            WHERE (d.images_status IS NULL OR d.images_status = 'PENDING')
              AND d.id_img IS NOT NULL
              AND COALESCE(p.unfinished, 0) = 0
              AND EXISTS (
                  SELECT 1
                  FROM scraped_detail_images s
                  WHERE s.detail_id = d.id_img
              )
        """)
        updated = cursor.rowcount
        conn.commit()
        if updated > 0:
            logger.info(f"Reconciled IMAGES_READY rows in {table_name}: {updated}")
        return updated
    except Exception as e:
        logger.error(f"Failed to reconcile IMAGES_READY in {table_name}: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()


# ============================================
# Main Processing
# ============================================

def generate_filename(id_detail: int, slug_name: str, idx: int, data_full_id: int) -> str:
    """Generate filename for image.
    Format: {id_detail}-{slug_name}-{index}-nhadat.cafeland.vn.jpg
    """
    slug = slug_name or f"detail-{data_full_id}"
    slug = re.sub(r'[^a-z0-9-]+', '-', slug.lower()).strip('-')
    if len(slug) > 100:
        slug = slug[:100].rstrip('-')
    
    index = (idx or 0) + 1
    return f"{id_detail}-{slug}-{index}-nhadat.cafeland.vn.jpg"


def process_single_image(row: dict, logo_img: Image.Image, args, ftp: FTP, 
                         db: Database, stats: dict, stats_lock: Lock) -> tuple:
    """Process a single image: download, resize, watermark, upload."""
    image_url = row.get('image_url') if isinstance(row, dict) else row[4]
    image_id = row.get('image_id') if isinstance(row, dict) else row[3]
    listing_id = row.get('listing_id') if isinstance(row, dict) else row[0]
    slug_name = row.get('slug_name') if isinstance(row, dict) else row[2]
    idx = row.get('idx') if isinstance(row, dict) else row[5]
    
    try:
        # Download image
        resp = requests.get(image_url, timeout=30)
        resp.raise_for_status()
        
        # Open and process
        img = Image.open(BytesIO(resp.content))
        
        # Resize
        img = resize_image(img, args.max_width)
        
        # Watermark
        if logo_img and not has_watermark_marker(img):
            img = apply_watermark(
                img, logo_img, 
                args.logo_position, 
                args.logo_scale, 
                args.logo_opacity
            )
        
        # Convert to RGB for JPEG
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        
        # Save to buffer
        buffer = BytesIO()
        save_kwargs = {'format': 'JPEG', 'quality': 85}
        if logo_img:
            add_watermark_marker('JPEG', save_kwargs)
        img.save(buffer, **save_kwargs)
        
        # Generate filename and path
        id_img = row.get('id_img') if isinstance(row, dict) else row[1]
        filename = generate_filename(id_img, slug_name, idx, listing_id)
        
        # Save to year/month folder (no day)
        from datetime import datetime
        now = datetime.now()
        date_folder = f"{now.year}/{now.month}"
        remote_path = f"{FTP_CONFIG['remote_dir']}/{date_folder}/{filename}"
        
        # Web path for database: /static01/sgd/cnews/... (không có /cafeland prefix)
        # URL sẽ là: https://static2.cafeland.vn/static01/sgd/cnews/...
        web_path = f"/static01/sgd/cnews/{date_folder}/{filename}"
        
        # Upload to FTP
        if not args.dry_run:
            if upload_to_ftp(ftp, buffer, remote_path):
                # Create thumbnail (-sm) for first image (idx=0)
                if idx == 0:
                    thumb_img = resize_image(img.copy(), args.thumb_width)
                    thumb_buffer = BytesIO()
                    thumb_img.save(thumb_buffer, format='JPEG', quality=85)
                    
                    # Generate -sm filename
                    base_name = filename.rsplit('.', 1)[0]
                    thumb_filename = f"{base_name}-sm.jpg"
                    thumb_remote_path = f"{FTP_CONFIG['remote_dir']}/{date_folder}/{thumb_filename}"
                    
                    if upload_to_ftp(ftp, thumb_buffer, thumb_remote_path):
                        logger.info(f"  📷 Thumbnail: {thumb_filename}")

                if verify_uploaded_file(ftp, remote_path, web_path):
                    update_image_status(db, image_id, 'UPLOADED', web_path)
                    check_and_mark_images_ready(db, id_img, args.table)
                else:
                    update_image_status(db, image_id, 'FAILED', None, 'Upload verify failed')
                    with stats_lock:
                        stats['fail'] += 1
                    return ('fail', image_url, remote_path, 'Upload verify failed')
                
                with stats_lock:
                    stats['ok'] += 1
                return ('ok', image_url, remote_path, None)
            else:
                update_image_status(db, image_id, 'FAILED', None, 'FTP upload failed')
                with stats_lock:
                    stats['fail'] += 1
                return ('fail', image_url, None, 'FTP upload failed')
        else:
            logger.info(f"[DRY-RUN] Would upload to: {remote_path}")
            if idx == 0:
                base_name = filename.rsplit('.', 1)[0]
                logger.info(f"[DRY-RUN] Would also upload thumbnail: {base_name}-sm.jpg")
            with stats_lock:
                stats['ok'] += 1
            return ('ok', image_url, remote_path, None)
            
    except Exception as e:
        update_image_status(db, image_id, 'FAILED', None, str(e))
        with stats_lock:
            stats['fail'] += 1
        return ('fail', image_url, None, str(e))


def run_processor(args):
    """Main processing loop."""
    logger.info("=== FTP IMAGE PROCESSOR ===")
    
    # Test connection
    if args.test_connection:
        try:
            ftp = connect_ftp()
            logger.info(f"✅ FTP connection successful to {FTP_CONFIG['host']}")
            ftp.quit()
            return
        except Exception as e:
            logger.error(f"❌ FTP connection failed: {e}")
            return
    
    # Load logo if specified
    logo_img = None
    if args.logo:
        try:
            logo_img = Image.open(args.logo)
            logger.info(f"Loaded logo: {args.logo}")
        except Exception as e:
            logger.error(f"Failed to load logo: {e}")
            return
    
    # Initialize
    db = Database()
    stats = {'ok': 0, 'fail': 0}
    stats_lock = Lock()
    
    # Thread-local FTP connections
    import threading
    ftp_connections = threading.local()
    
    def get_thread_ftp():
        """Get or create FTP connection for current thread."""
        if not hasattr(ftp_connections, 'ftp') or ftp_connections.ftp is None:
            ftp_connections.ftp = connect_ftp()
        return ftp_connections.ftp
    
    def process_with_thread_ftp(row):
        """Wrapper to process image with thread-local FTP."""
        try:
            ftp = get_thread_ftp()
            return process_single_image(row, logo_img, args, ftp, db, stats, stats_lock)
        except Exception as e:
            # Reconnect on error
            try:
                ftp_connections.ftp = connect_ftp()
                ftp = ftp_connections.ftp
                return process_single_image(row, logo_img, args, ftp, db, stats, stats_lock)
            except Exception as e2:
                return ('fail', str(row), None, str(e2))
    
    try:
        offset = 0
        total_processed = 0
        
        while True:
            try:
                rows = get_pending_images(
                    db, 
                    start_id=args.start_id,
                    end_id=args.end_id,
                    limit=args.batch,
                    offset=offset,
                    table_name=args.table,
                )
            except Exception as e:
                # InnoDB lock pressure / transient DB issue: backoff and retry loop.
                msg = str(e)
                if "1206" in msg or "lock table size" in msg.lower():
                    logger.warning("DB lock pressure (1206). Sleep 5s and retry batch claim...")
                    time.sleep(5)
                    continue
                raise
            
            if not rows:
                reconcile_images_ready(db, args.table)
                logger.info("No more images to process")
                break
            
            logger.info(f"Processing batch: {len(rows)} images (offset: {offset})")
            
            # Track processed URLs in this batch to avoid redundant uploads
            batch_urls = set()
            unique_rows = []
            
            for row in rows:
                image_url = row['image_url'] if isinstance(row, dict) else row[4]
                if image_url in batch_urls:
                    # Duplicate in batch - mark as SKIPPED or just ignore?
                    # Better to mark as DUPLICATE to avoid reprocessing
                    image_id = row['image_id'] if isinstance(row, dict) else row[3] # id in scraped_detail_images
                    # But if we mark as duplicate, check_and_mark_images_ready might fail if it counts valid images?
                    # check_and_mark_images_ready counts "status NOT IN ('UPLOADED', 'FAILED')"
                    # So if we mark passed duplicates as 'DUPLICATE', count will decrease.
                    # Let's verify check_and_mark_images_ready logic:
                    # SELECT COUNT(*) as pending FROM scraped_detail_images WHERE detail_id = %s AND status NOT IN ('UPLOADED', 'FAILED')
                    # So 'DUPLICATE' is considered "Done". Good.
                    update_image_status(db, image_id, 'DUPLICATE', None, 'Duplicate URL in batch')
                    continue
                
                batch_urls.add(image_url)
                unique_rows.append(row)
            
            if not unique_rows:
                reconcile_images_ready(db, args.table)
                logger.info("All images in batch were duplicates.")
                total_processed += len(rows)
                continue
                
            logger.info(f"Unique images in batch: {len(unique_rows)} / {len(rows)}")
            
            if args.dry_run:
                # Dry run - sequential
                for row in unique_rows:
                    status, url, path, error = process_single_image(
                        row, logo_img, args, None, db, stats, stats_lock
                    )
                    if status == 'ok':
                        logger.info(f"✅ {url[:50]}... -> {path}")
            else:
                # Real run - parallel with thread-local FTP
                with ThreadPoolExecutor(max_workers=args.workers) as executor:
                    futures = [executor.submit(process_with_thread_ftp, row) for row in unique_rows]
                    
                    for future in as_completed(futures):
                        try:
                            status, url, path, error = future.result()
                            if status == 'ok':
                                logger.info(f"✅ {url[:50]}... -> {path}")
                            else:
                                logger.warning(f"❌ {url[:50]}... Error: {error}")
                        except Exception as e:
                            logger.error(f"Worker error: {e}")

            reconcile_images_ready(db, args.table)
            
            total_processed += len(rows)
            
            # Check limit
            if args.limit > 0 and total_processed >= args.limit:
                logger.info(f"Reached limit of {args.limit} images")
                break
            
            offset += args.batch
            
            # Brief pause between batches
            time.sleep(0.3)
        
        logger.info("=" * 50)
        logger.info(f"COMPLETED. Total: {total_processed}, OK: {stats['ok']}, FAIL: {stats['fail']}")
        
    finally:
        pass  # Thread-local connections will be cleaned up automatically


def main():
    parser = argparse.ArgumentParser(description='FTP Image Processor')
    parser.add_argument('--batch', type=int, default=100, help='Batch size')
    parser.add_argument('--workers', type=int, default=10, help='Number of workers')
    parser.add_argument('--max-width', type=int, default=1100, help='Max image width')
    parser.add_argument('--thumb-width', type=int, default=750, help='Thumbnail width')
    parser.add_argument('--logo', type=str, default='/home/chungnt/crawlvip/output/logo/domain-nhadat-cafeland.png', help='Logo file path')
    parser.add_argument('--logo-scale', type=int, default=24, help='Logo scale %')
    parser.add_argument('--logo-opacity', type=float, default=1.0, help='Logo opacity')
    parser.add_argument('--logo-position', type=str, default='bottom-right',
                        choices=['top-left', 'top-right', 'bottom-left', 'bottom-right', 'center'])
    parser.add_argument('--test-connection', action='store_true', help='Test FTP connection')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without uploading')
    parser.add_argument('--start-id', type=int, default=0, help='Start from data_full ID')
    parser.add_argument('--end-id', type=int, default=0, help='End at data_full ID')
    parser.add_argument('--limit', type=int, default=0, help='Max total images to process (0=unlimited)')
    parser.add_argument('--table', type=str, default='data_full', choices=['data_full', 'data_no_full'], help='Source listing table')
    
    args = parser.parse_args()
    run_processor(args)


if __name__ == '__main__':
    main()
