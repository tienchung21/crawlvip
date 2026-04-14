import asyncio
import os
import sys
import subprocess
import logging

# Setup path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)
from database import Database
# IMPORT ManualMogiCrawler instead of listing_crawler
from manual_mogi_rent import ManualMogiCrawler

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def run_listing_phase_manual_style():
    db = Database()
    logging.info("=== STARTING DAILY LISTING CRAWL (MANUAL/URL STYLE) ===")
    
    # Try proxy from env default
    proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    
    crawler = ManualMogiCrawler(db)
    # Run with 5 threads
    # This will crawl ALL categories in MOGI_CATEGORIES (Mua + Thuê)
    try:
        crawler.run_crawling(threads=5, proxy=proxy)
    except Exception as e:
        logging.error(f"Error in listing phase: {e}")
            
    logging.info("=== LISTING CRAWL FINISHED ===")

def run_detail_phase():
    logging.info("=== STARTING DETAIL CRAWL ===")
    # Run the detail crawler as a subprocess
    # Using 5 threads, 1-2.5s delay as configured by user
    detail_script = os.path.join(SCRIPT_DIR, "mogi_detail_crawler.py")
    cmd = [
        sys.executable, 
        detail_script,
        "--threads", "5", 
        "--batch", "20", 
        "--delay-min", "1", 
        "--delay-max", "2.5"
    ]
    
    try:
        # Run and wait for completion
        subprocess.run(cmd, check=True, cwd=SCRIPT_DIR)
        logging.info("Detail crawl completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Detail crawl failed with code {e.returncode}")
    except Exception as e:
        logging.error(f"Detail crawl execution error: {e}")

if __name__ == "__main__":
    # 1. Run Listing (Manual/Simple Style)
    run_listing_phase_manual_style()
    
    # 2. Run Detail
    run_detail_phase()
