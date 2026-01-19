import pymysql

conn = pymysql.connect(host='localhost', user='root', password='', database='craw_db', charset='utf8mb4')
cursor = conn.cursor()

print("=== CHECKING location_detail TABLE STRUCTURE AND DATA ===")

# 1. Check Columns
cursor.execute("DESCRIBE location_detail")
cols = [r[0] for r in cursor.fetchall()]
if 'cafeland_id' in cols:
    print("[YES] Column 'cafeland_id' EXISTS.")
else:
    print("[NO] Column 'cafeland_id' MISSING.")

# 2. Check Data Sample
print("\n=== SAMPLE DATA (HCM) ===")
cursor.execute("""
    SELECT ward_id, name, cafeland_id 
    FROM location_detail 
    WHERE region_id = 13000 AND cafeland_id IS NOT NULL 
    LIMIT 10
""")
rows = cursor.fetchall()

if rows:
    print(f"{'NHATOT NAME':<50} | {'CAFELAND ID'}")
    print("-" * 70)
    for r in rows:
        print(f"{r[1]:<50} | {r[2]}")
else:
    print("No data found with cafeland_id populated.")

# 3. Count
cursor.execute("SELECT COUNT(*) FROM location_detail WHERE region_id = 13000 AND cafeland_id IS NOT NULL")
count = cursor.fetchone()[0]
print(f"\nTotal HCM records with Cafeland ID: {count}")

cursor.close()
conn.close()
