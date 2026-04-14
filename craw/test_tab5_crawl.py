#!/usr/bin/env python3
"""
Test script for Tab 5 crawl functionality
Usage: python test_tab5_crawl.py
"""

import asyncio
import sys
import os

# Add craw directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from listing_simple_core import crawl_listing_simple
from database import Database

async def main():
    print("=" * 60)
    print("Testing Tab 5 Crawl - listing_simple_core.py")
    print("=" * 60)
    
    # Test configuration
    target_url = "https://batdongsan.com.vn/nha-dat-ban"
    item_selector = ".js__product-link-for-product-id"
    next_selector = (
        ".re__pagination a[aria-label*='Sau'], "
        ".re__pagination a[aria-label*='sau'], "
        ".re__pagination a[rel='next'], "
        "a.re__pagination-icon[href][aria-label*='sau'], "
        "a.re__pagination-icon[href][aria-label*='next']"
    )
    max_pages = 2
    
    print(f"\nConfiguration:")
    print(f"  URL: {target_url}")
    print(f"  Item selector: {item_selector}")
    print(f"  Next selector: {next_selector}")
    print(f"  Max pages: {max_pages}")
    print(f"  Show browser: True (for debugging)")
    print(f"  Fake scroll: True")
    print(f"  Fake hover: True")
    print()
    
    # Initialize database
    db = Database(host="localhost", user="root", password="", database="craw_db")
    
    # Run crawl
    result = await crawl_listing_simple(
        target_url=target_url,
        item_selector=item_selector,
        next_selector=next_selector,
        max_pages=max_pages,
        db=db,
        domain="batdongsan",
        loaihinh="Ban nha rieng",
        trade_type="muaban",
        show_browser=True,  # IMPORTANT: Show browser to see what's happening
        enable_fake_scroll=True,
        enable_fake_hover=True,
        wait_load_min=5,
        wait_load_max=8,
        wait_next_min=3,
        wait_next_max=5,
        profile_suffix="test"
    )
    
    print("\n" + "=" * 60)
    print("RESULT:")
    print("=" * 60)
    print(f"Success: {result.get('success')}")
    print(f"Total links: {result.get('total_links')}")
    print(f"Pages crawled: {result.get('pages_crawled')}")
    print(f"New links added: {result.get('new_links_added')}")
    if not result.get('success'):
        print(f"Error: {result.get('error')}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
