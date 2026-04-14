
import os
import sys

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    pass

def main():
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    
    # Types to Exclude (Sale Side)
    # User listed: "Bán trang trại, khu nghỉ dưỡng", "Bán kho, nhà xưởng", "Bán loại bất động sản khác"
    # Corresponding loaihinh:
    # - Trang trại/Khu nghỉ dưỡng
    # - Kho, nhà xưởng
    # - BĐS khác
    
    types = [
        'Trang trại/Khu nghỉ dưỡng',
        'Kho, nhà xưởng',
        'BĐS khác'
    ]
    
    placeholders = ', '.join(['%s'] * len(types))
    query = f"""
        UPDATE collected_links 
        SET status = 'done' 
        WHERE domain = 'batdongsan.com.vn'
          AND (status IS NULL OR status != 'done')
          AND url LIKE '%%/ban-%%'
          AND loaihinh IN ({placeholders})
    """
    
    print(f"Executing Update for types: {types} (Sale Only)...")
    cur.execute(query, tuple(types))
    rows = cur.rowcount
    conn.commit()
    
    print(f"Updated {rows} listings to 'done'.")
    conn.close()

if __name__ == "__main__":
    main()
