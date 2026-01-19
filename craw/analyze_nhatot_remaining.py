import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

output_path = "nhatot_unmatched_analysis.txt"

with open(output_path, "w", encoding="utf-8") as f:
    f.write("=== PHAN TICH: CAC XA NHATOT CHUA MATCH SANG CAFELAND ===\n")
    
    # 1. Get Unmatched (cafeland_id IS NULL)
    cursor.execute("""
        SELECT ward_id, name, area_id 
        FROM location_detail 
        WHERE region_id = 13000 AND level = 3 AND cafeland_id IS NULL
    """)
    unmatched = cursor.fetchall()
    
    f.write(f"Tong so chua match: {len(unmatched)}\n")
    f.write(f"{'NHATOT WARD':<50} | {'NHATOT DISTRICT'}\n")
    f.write("-" * 80 + "\n")
    
    for rid, rname, area_id in unmatched:
        # Get dist name
        cursor.execute(f"SELECT name FROM location_detail WHERE area_id = {area_id} AND level = 2")
        dname = cursor.fetchone()[0]
        f.write(f"{rname:<50} | {dname}\n")

    f.write("\n=== THONG KE SO LUONG XA CAFELAND (HCM) ===\n")
    # Count Total Cafeland Wards
    cursor.execute("""
        SELECT COUNT(*) FROM transaction_city 
        WHERE city_parent_id IN (
            SELECT city_id FROM transaction_city WHERE city_parent_id = 63 -- HCM
        )
    """)
    total_cf = cursor.fetchone()[0]
    
    # Count Matched (Used)
    cursor.execute("SELECT COUNT(DISTINCT cafeland_id) FROM location_detail WHERE region_id = 13000 AND cafeland_id IS NOT NULL")
    matched_distinct = cursor.fetchone()[0]
    
    f.write(f"Tong so xa Cafeland HCM: {total_cf}\n")
    f.write(f"So xa Cafeland da duoc match tu Nhatot: {matched_distinct}\n")
    f.write(f"Ty le bao phu: {matched_distinct}/{total_cf} ({(matched_distinct/total_cf)*100:.1f}%)\n")

cursor.close()
conn.close()
print(f"Saved analysis to {output_path}")
