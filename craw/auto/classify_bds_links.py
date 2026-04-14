
import os
import sys
import argparse
import time

# Add project root to path
sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    # Try adding parent dir if run from subdir
    sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
    from craw.database import Database

def get_mapping():
    # Longest keys first to prevent partial matches
    # e.g. ban-can-ho-chung-cu-mini before ban-can-ho-chung-cu
    
    # Format: Prefix -> (TradeType, PropertyType)
    # TradeType: 'Bán' / 'Thuê'
    
    mapping = {
        # --- BÁN ---
        'ban-can-ho-chung-cu-mini': ('Bán', 'Căn hộ chung cư mini'),
        'ban-can-ho-chung-cu': ('Bán', 'Căn hộ chung cư'),
        'ban-nha-rieng': ('Bán', 'Nhà riêng'),
        'ban-nha-biet-thu-lien-ke': ('Bán', 'Biệt thự liền kề'),
        'ban-nha-mat-pho': ('Bán', 'Nhà mặt phố'),
        'ban-shophouse-nha-pho-thuong-mai': ('Bán', 'Shophouse'),
        'ban-dat-nen-du-an': ('Bán', 'Đất nền dự án'),
        'ban-dat': ('Bán', 'Đất'),
        'ban-trang-trai-khu-nghi-duong': ('Bán', 'Trang trại/Khu nghỉ dưỡng'),
        'ban-condotel': ('Bán', 'Condotel'),
        'ban-kho-nha-xuong': ('Bán', 'Kho, nhà xưởng'),
        'ban-loai-bat-dong-san-khac': ('Bán', 'BĐS khác'),
        
        # --- THUÊ ---
        'cho-thue-can-ho-chung-cu-mini': ('Thuê', 'Căn hộ chung cư mini'),
        'cho-thue-can-ho-chung-cu': ('Thuê', 'Căn hộ chung cư'),
        'cho-thue-nha-rieng': ('Thuê', 'Nhà riêng'),
        'cho-thue-nha-biet-thu-lien-ke': ('Thuê', 'Biệt thự liền kề'),
        'cho-thue-nha-mat-pho': ('Thuê', 'Nhà mặt phố'),
        'cho-thue-shophouse-nha-pho-thuong-mai': ('Thuê', 'Shophouse'),
        'cho-thue-nha-tro-phong-tro': ('Thuê', 'Nhà trọ, phòng trọ'),
        'cho-thue-van-phong': ('Thuê', 'Văn phòng'),
        'cho-thue-sang-nhuong-cua-hang-ki-ot': ('Thuê', 'Cửa hàng, Ki-ốt'),
        'cho-thue-kho-nha-xuong-dat': ('Thuê', 'Kho, nhà xưởng, đất'),
        'cho-thue-loai-bat-dong-san-khac': ('Thuê', 'BĐS khác'),
    }
    
    # Sort by length descending
    return dict(sorted(mapping.items(), key=lambda x: len(x[0]), reverse=True))

def main():
    parser = argparse.ArgumentParser(description="Classify Batdongsan Links")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without updating DB")
    parser.add_argument("--limit", type=int, default=10000, help="Batch size limit")
    args = parser.parse_args()
    
    db = Database()
    conn = db.get_connection()
    if not conn:
        print("DB FAILED")
        return
        
    cur = conn.cursor()
    
    # Get Unclassified Batdongsan Links
    # Using 'batdongsan.com.vn' domain
    # Only select where loaihinh IS NULL or Empty to save time?
    print("Fetching unclassified links...")
    start_t = time.time()
    
    # Use Cursor Iteration for Large Dataset
    # But since we update, we need IDs.
    query = """
        SELECT id, url 
        FROM collected_links 
        WHERE (domain = 'batdongsan.com.vn' OR url LIKE '%%batdongsan.com.vn%%')
          AND (loaihinh IS NULL OR loaihinh = '')
        LIMIT %s
    """
    cur.execute(query, (args.limit,))
    rows = cur.fetchall() # Might be large
    print(f"Fetched {len(rows)} rows in {time.time() - start_t:.2f}s")
    
    if not rows:
        print("No unclassified links found.")
        return

    mapping = get_mapping()
    updates = [] # List of (loaihinh, trade_type, id)
    
    matches = 0
    for row in rows:
        # Handle tuple or dict
        if isinstance(row, dict):
            lid = row['id']
            url = row['url']
        else:
            lid = row[0]
            url = row[1]
            
        # Parse URL Path
        # Remove https://batdongsan.com.vn/
        path = url.replace("https://batdongsan.com.vn/", "").replace("https://www.batdongsan.com.vn/", "")
        
        found = False
        for prefix, (trade, ptype) in mapping.items():
            if path.startswith(prefix + "-") or path == prefix:
                updates.append((ptype, trade, lid))
                found = True
                matches += 1
                if args.dry_run and matches <= 10:
                    print(f"[PREVIEW] {url} -> Type: {ptype}, Trade: {trade}")
                break
        
        # if not found and args.dry_run:
        #    print(f"[Unmatched] {url}")

    print(f"Matched {len(updates)} / {len(rows)} links.")
    
    if args.dry_run:
        print("Dry Run Finished. No DB changes.")
    else:
        if updates:
            print(f"Updating DB ({len(updates)} rows)...")
            
            # Helper to chunk list
            batch_size = 5000
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                update_query = """
                    UPDATE collected_links 
                    SET loaihinh = %s, trade_type = %s, updated_at = NOW()
                    WHERE id = %s
                """
                try:
                    cur.executemany(update_query, batch)
                    conn.commit()
                    print(f" - Updated batch {i}-{i+len(batch)}: {cur.rowcount} rows.")
                except Exception as e:
                    print(f"Batch Update Failed: {e}")
                    conn.rollback()
        else:
            print("Nothing to update.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
