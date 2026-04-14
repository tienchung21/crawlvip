
import mysql.connector
from database import Database

def check_progress():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print("--- CRAWL PROGRESS (Estimate) ---")
    print(f"{'CATEGORY (Loai Hinh)':<30} | {'LINKS':<10} | {'EST. PAGES':<10} | {'LATEST TIME'}")
    print("-" * 70)
    
    query = """
        SELECT 
            loaihinh, 
            COUNT(*) as total, 
            MAX(created_at) as last_update 
        FROM collected_links 
        GROUP BY loaihinh
        ORDER BY last_update DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    total_all = 0
    for row in results:
        cat = row[0] if row[0] else "Unknown"
        count = row[1]
        last_update = row[2]
        est_pages = int(count / 15)
        total_all += count
        
        print(f"{cat[:30]:<30} | {count:<10} | {est_pages:<10} | {last_update}")
        
    print("-" * 70)
    print(f"TOTAL LINKS COLLECTED (ALL TIME): {total_all}")
    print("\n" + "="*70 + "\n")
    
    print("--- NEW LINKS TODAY (Since 12:00 PM) ---")
    print(f"{'CATEGORY':<30} | {'NEW LINKS':<10} | {'EST. PAGES':<10}")
    print("-" * 55)
    
    cursor.execute("""
        SELECT loaihinh, COUNT(*) 
        FROM collected_links 
        WHERE created_at >= '2026-01-21 12:00:00'
        GROUP BY loaihinh
    """)
    
    new_results = cursor.fetchall()
    total_new = 0
    for row in new_results:
        cat = row[0] if row[0] else "Unknown"
        count = row[1]
        est_pages = int(count / 15)
        total_new += count
        print(f"{cat[:30]:<30} | {count:<10} | {est_pages:<10}")
        
    print("-" * 55)
    print(f"TOTAL NEW LINKS: {total_new}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_progress()
