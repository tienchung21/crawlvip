
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from craw.database import Database

def fix_price():
    print("Fixing Mogi Prices in data_full with Enhanced Logic (Residue) - Direct SQL...")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Using direct string injection for LIKE clauses to avoid pymysql formatting issues
    
    sql = """
    UPDATE data_full df
    JOIN scraped_details_flat sdf ON df.id_img = sdf.id
    SET df.price = CASE
        -- Case 1: TỶ (Matches 'tỷ' somewhere)
        WHEN sdf.khoanggia LIKE '%tỷ%' THEN 
            (CAST(REPLACE(REGEXP_SUBSTR(sdf.khoanggia, '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)) * 1000000000)
            + 
            CASE WHEN sdf.khoanggia LIKE '%triệu%' THEN -- residue 'triệu'
                IFNULL(CAST(REPLACE(REGEXP_SUBSTR(SUBSTRING_INDEX(sdf.khoanggia, 'tỷ', -1), '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)), 0) * 1000000
            ELSE 0 END

        -- Case 2: TRIỆU (Matches 'triệu')
        WHEN sdf.khoanggia LIKE '%triệu%' THEN 
            (CAST(REPLACE(REGEXP_SUBSTR(sdf.khoanggia, '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)) * 1000000)
            +
            CASE WHEN (sdf.khoanggia LIKE '%nghìn%' OR sdf.khoanggia LIKE '%ngàn%') THEN -- residue 'nghìn'/'ngàn'
                IFNULL(CAST(REPLACE(REGEXP_SUBSTR(SUBSTRING_INDEX(sdf.khoanggia, 'triệu', -1), '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)), 0) * 1000
            ELSE 0 END
            
        ELSE df.price
    END
    WHERE df.source = 'mogi'
    """
    
    print("Executing update...")
    cursor.execute(sql)
    rows = cursor.rowcount
    conn.commit()
    print(f"Updated {rows} rows.")
    conn.close()

if __name__ == "__main__":
    fix_price()
