import sys
import os
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from database import Database
except ImportError:
    from craw.database import Database

def update_full_status():
    print("=== UPDATING 'FULL' STATUS FOR EXISTING RECORDS ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # SQL Logic matching the Python logic:
        # 1 if all required fields are present (NOT NULL and NOT EMPTY)
        # Required: title, domain, mota, khoanggia, (dientich OR dientichsudung),
        # diachi, sodienthoai, (loaihinh OR loaibds), trade_type
        
        sql = """
        UPDATE scraped_details_flat
        SET full = CASE
            WHEN (
                (title IS NOT NULL AND TRIM(title) <> '') AND
                (domain IS NOT NULL AND TRIM(domain) <> '') AND
                (mota IS NOT NULL AND TRIM(mota) <> '') AND
                (khoanggia IS NOT NULL AND TRIM(khoanggia) <> '') AND
                (
                    (dientich IS NOT NULL AND TRIM(dientich) <> '') OR 
                    (dientichsudung IS NOT NULL AND TRIM(dientichsudung) <> '')
                ) AND
                (diachi IS NOT NULL AND TRIM(diachi) <> '') AND
                (sodienthoai IS NOT NULL AND TRIM(sodienthoai) <> '') AND
                (
                    (loaihinh IS NOT NULL AND TRIM(loaihinh) <> '') OR
                    (loaibds IS NOT NULL AND TRIM(loaibds) <> '')
                ) AND
                (trade_type IS NOT NULL AND TRIM(trade_type) <> '')
            ) THEN 1
            ELSE 0
        END
        """
        
        print("Executing SQL Update...")
        start_time = time.time()
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        conn.commit()
        end_time = time.time()
        
        print(f"✅ Success! Updated {rows_affected} rows.")
        print(f"⏱️ Time taken: {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    update_full_status()
