
import pymysql
import time
import unicodedata
import re

BATCH_SIZE = 5000
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_NAME = 'craw_db'


def normalize_text(s):
    if not s:
        return ""
    s = str(s).strip().lower()
    s = s.replace("đ", "d").replace("Ð", "d")
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def main():
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    script_name = "batdongsan_step5_group_median.py"
    print(f"=== Running {script_name} ===")
    start_time = time.time()

    total_scanned = 0
    total_mapped = 0
    
    # 0. Ensure column exists (Just in case, though likely exists from other scripts)
    try:
        cursor.execute("ALTER TABLE data_clean_v1 ADD COLUMN median_group TINYINT NULL")
        conn.commit()
    except Exception:
        pass

    while True:
        # Get Batch (Status = 4 from Step 4)
        sql_get = f"""
            SELECT id, std_trans_type, std_category 
            FROM data_clean_v1 
            WHERE domain = 'batdongsan.com.vn'
              AND process_status = 4 
              AND median_group IS NULL
            LIMIT {BATCH_SIZE}
        """
        cursor.execute(sql_get)
        rows = cursor.fetchall()
        
        if not rows:
            break
            
        batch_scanned = 0
        batch_mapped = 0
        for row in rows:
            record_id = row.get('id')
            t_type = row.get('std_trans_type')
            cat = row.get('std_category')
            
            group = None
            cat_norm = normalize_text(cat)
            
            # Logic Map
            if t_type == 'u':
                # Group 4: All Rent
                group = 4
            elif t_type == 's':
                # Apply mapping formula requested by user (Sale)
                if cat_norm in {
                    "nha rieng",
                    "nha mat pho",
                    "biet thu lien ke",
                    "duong noi bo",
                    "nha biet thu lien ke",
                    "nha hem ngo",
                    "nha mat tien pho",
                }:
                    group = 1
                elif cat_norm in {
                    "can ho chung cu mini",
                    "condotel",
                    "can ho chung cu",
                    "can ho dich vu",
                    "can ho officetel",
                    "can ho penthouse",
                    "can ho tap the cu xa",
                }:
                    group = 2
                elif cat_norm in {
                    "dat",
                    "kho nha xuong dat",
                    "dat kho xuong",
                    "dat nen du an",
                    "dat nong nghiep",
                    "dat tho cu",
                }:
                    group = 3
                # Else: None
            
            # Only finalize Step 5 when median_group is identified.
            # Unmapped rows stay at Step 4 for later rule updates.
            if group is not None:
                sql_update = """
                    UPDATE data_clean_v1 
                    SET median_group = %s, process_status = 5, last_script = %s
                    WHERE id = %s
                """
                cursor.execute(sql_update, (group, script_name, record_id))
                batch_mapped += 1
            batch_scanned += 1
                
        conn.commit()
        total_scanned += batch_scanned
        total_mapped += batch_mapped
        print(
            f"  Batch: scanned={batch_scanned}, mapped={batch_mapped} "
            f"(Totals: scanned={total_scanned}, mapped={total_mapped})"
        )

        if len(rows) < BATCH_SIZE or batch_mapped == 0:
            break

    print(f"-> Grouped Median for {total_mapped} rows (scanned {total_scanned} rows).")
    print("=== Finished ===")

    end_time = time.time()
    print(f"=== Finished in {end_time - start_time:.2f}s ===")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
