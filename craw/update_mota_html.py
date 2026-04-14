#!/usr/bin/env python3
"""
Update mota column in scraped_details_flat to include HTML (with <br> tags).
Re-fetches Mogi detail pages and extracts the info-content-body div's inner HTML.

Usage:
  python update_mota_html.py --test         # Test 1 listing
  python update_mota_html.py --run          # Run all (last 6 months)
  python update_mota_html.py --run --threads 5  # Multi-threaded
"""

import os
import sys
import time
import random
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict

import requests
from lxml import html as lxml_html

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Lock for DB operations
db_lock = threading.Lock()

# Stats
stats = {'total': 0, 'updated': 0, 'skipped': 0, 'error': 0}
stats_lock = threading.Lock()

# Shared session headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
}

# XPath selector for mota (same as template)
MOTA_XPATH = '//div[contains(@class, "main-info")]/h2[contains(@class, "info-title")][contains(text(), "Giới thiệu")]/following-sibling::div[contains(@class, "info-content-body")][1]'


def extract_mota_html(html_content: str) -> Optional[str]:
    """Extract mota as inner HTML from page content."""
    try:
        tree = lxml_html.fromstring(html_content)
        elements = tree.xpath(MOTA_XPATH)
        if not elements:
            return None
        element = elements[0]
        # Get inner HTML (preserves <br> tags)
        raw = lxml_html.tostring(element, encoding='unicode', with_tail=False).strip()
        return raw
    except Exception as e:
        logger.debug(f"Extract error: {e}")
        return None


def fetch_and_extract(session: requests.Session, url: str) -> Optional[str]:
    """Fetch a page and extract mota HTML."""
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"HTTP {resp.status_code} for {url[:80]}")
            return None
        return extract_mota_html(resp.text)
    except requests.RequestException as e:
        logger.warning(f"Request error for {url[:80]}: {e}")
        return None


def get_listings_to_update(db: Database, limit: int = 0) -> List[Dict]:
    """Get listings from last 6 months that need mota update."""
    sql = """
        SELECT id, url, matin
        FROM scraped_details_flat
        WHERE domain = 'mogi'
          AND full = 1
          AND STR_TO_DATE(ngaydang, '%d/%m/%Y') >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
          AND mota IS NOT NULL
          AND mota != ''
          AND mota NOT LIKE '%<br%'
        ORDER BY id DESC
    """
    if limit > 0:
        sql += f" LIMIT {limit}"
    
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    # pymysql DictCursor returns list of dicts, mysql.connector returns tuples
    if rows and isinstance(rows[0], dict):
        return rows
    # Fallback for tuple-based cursor
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def update_mota(db: Database, detail_id: int, new_mota: str) -> bool:
    """Update mota column for a specific record."""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE scraped_details_flat SET mota = %s WHERE id = %s",
            (new_mota, detail_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"DB update error for id {detail_id}: {e}")
        return False


def process_batch(listings: List[Dict], db: Database, delay_range=(0.5, 1.5)):
    """Process a batch of listings."""
    session = requests.Session()
    session.headers.update(HEADERS)
    
    for listing in listings:
        detail_id = listing['id']
        url = listing['url']
        matin = listing.get('matin', '?')
        
        new_mota = fetch_and_extract(session, url)
        
        if new_mota and '<br' in new_mota.lower():
            with db_lock:
                success = update_mota(db, detail_id, new_mota)
            
            if success:
                with stats_lock:
                    stats['updated'] += 1
                logger.debug(f"✓ Updated id={detail_id} matin={matin}")
            else:
                with stats_lock:
                    stats['error'] += 1
        elif new_mota:
            # Got mota but no <br> in it (some listings genuinely have no line breaks)
            # Still update with HTML version (it wraps in <div>)
            with db_lock:
                success = update_mota(db, detail_id, new_mota)
            if success:
                with stats_lock:
                    stats['updated'] += 1
                logger.debug(f"✓ Updated id={detail_id} (no br) matin={matin}")
            else:
                with stats_lock:
                    stats['error'] += 1
        else:
            with stats_lock:
                stats['skipped'] += 1
            logger.debug(f"✗ Skipped id={detail_id} matin={matin} (page fetch failed or no mota)")
        
        with stats_lock:
            stats['total'] += 1
            t = stats['total']
            if t % 100 == 0:
                logger.info(f"Progress: {t} processed, {stats['updated']} updated, {stats['skipped']} skipped, {stats['error']} errors")
        
        # Rate limit
        time.sleep(random.uniform(*delay_range))


def run_test(db: Database):
    """Test with 1 listing."""
    listings = get_listings_to_update(db, limit=1)
    if not listings:
        logger.info("No listings to update!")
        return
    
    listing = listings[0]
    logger.info(f"Test listing: id={listing['id']}, matin={listing['matin']}, url={listing['url'][:80]}...")
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    new_mota = fetch_and_extract(session, listing['url'])
    
    if new_mota:
        # Show preview
        preview = new_mota[:500]
        logger.info(f"\n--- Extracted mota HTML (first 500 chars) ---")
        print(preview)
        logger.info(f"--- End preview ---")
        
        has_br = '<br' in new_mota.lower()
        logger.info(f"Contains <br>: {has_br}")
        logger.info(f"Total length: {len(new_mota)} chars")
        
        # Ask to update
        confirm = input("\nUpdate this record? (y/n): ").strip().lower()
        if confirm == 'y':
            success = update_mota(db, listing['id'], new_mota)
            if success:
                logger.info(f"✓ Updated id={listing['id']}")
            else:
                logger.error(f"✗ Failed to update id={listing['id']}")
    else:
        logger.warning("Could not extract mota from page!")


def run_all(db: Database, threads: int = 3, delay_min: float = 0.5, delay_max: float = 1.5):
    """Run for all listings in last 6 months."""
    logger.info("Fetching listings to update...")
    listings = get_listings_to_update(db)
    total = len(listings)
    logger.info(f"Found {total} listings needing mota update")
    
    if total == 0:
        return
    
    # Split into chunks for threads
    chunk_size = (total + threads - 1) // threads
    chunks = [listings[i:i+chunk_size] for i in range(0, total, chunk_size)]
    
    logger.info(f"Starting {len(chunks)} workers, ~{chunk_size} listings each")
    logger.info(f"Delay: {delay_min}-{delay_max}s between requests")
    
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for chunk in chunks:
            future = executor.submit(process_batch, chunk, db, (delay_min, delay_max))
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Worker error: {e}")
    
    logger.info("=" * 50)
    logger.info("COMPLETED")
    logger.info(f"Total: {stats['total']}")
    logger.info(f"Updated: {stats['updated']}")
    logger.info(f"Skipped: {stats['skipped']}")
    logger.info(f"Errors: {stats['error']}")
    logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description='Update mota column with HTML (br tags)')
    parser.add_argument('--test', action='store_true', help='Test with 1 listing')
    parser.add_argument('--run', action='store_true', help='Run for all listings (last 6 months)')
    parser.add_argument('--threads', type=int, default=3, help='Number of threads (default 3)')
    parser.add_argument('--delay-min', type=float, default=0.5, help='Min delay between requests')
    parser.add_argument('--delay-max', type=float, default=1.5, help='Max delay between requests')
    
    args = parser.parse_args()
    
    if not args.test and not args.run:
        parser.print_help()
        return
    
    db = Database()
    
    if args.test:
        run_test(db)
    elif args.run:
        run_all(db, threads=args.threads, delay_min=args.delay_min, delay_max=args.delay_max)


if __name__ == '__main__':
    main()
