
import sys
import os
import time

# Setup path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def check_progress():
    print("=== MOGI DATA QUALITY CHECK (2025-2026) ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Total records for 2025/2026
        sql_total = """
            SELECT COUNT(*) FROM scraped_details_flat 
            WHERE domain='mogi' 
            AND (ngaydang LIKE '%2025%' OR ngaydang LIKE '%2026%')
        """
        cursor.execute(sql_total)
        total = cursor.fetchone()
        total_count = list(total.values())[0] if isinstance(total, dict) else total[0]
        
        # 2. Count with MAP
        sql_map = """
            SELECT COUNT(*) FROM scraped_details_flat 
            WHERE domain='mogi' 
            AND (ngaydang LIKE '%2025%' OR ngaydang LIKE '%2026%')
            AND map IS NOT NULL AND map != ''
        """
        cursor.execute(sql_map)
        res_map = cursor.fetchone()
        map_count = list(res_map.values())[0] if isinstance(res_map, dict) else res_map[0]
        
        # 3. Count with THUOCDUAN (Project Name)
        sql_project = """
            SELECT COUNT(*) FROM scraped_details_flat 
            WHERE domain='mogi' 
            AND (ngaydang LIKE '%2025%' OR ngaydang LIKE '%2026%')
            AND thuocduan IS NOT NULL AND thuocduan != ''
        """
        cursor.execute(sql_project)
        res_project = cursor.fetchone()
        project_count = list(res_project.values())[0] if isinstance(res_project, dict) else res_project[0]
        
        print(f"Total 2025-2026 Records: {total_count}")
        print(f"Have MAP: {map_count} ({map_count/total_count*100:.2f}%)")
        print(f"Have Project Name: {project_count} ({project_count/total_count*100:.2f}%)")
        
        # 4. Show some sample projects
        print("\n--- Sample Projects ---")
        sql_sample = """
            SELECT thuocduan, COUNT(*) as c 
            FROM scraped_details_flat 
            WHERE domain='mogi' 
            AND (ngaydang LIKE '%2025%' OR ngaydang LIKE '%2026%')
            AND thuocduan IS NOT NULL AND thuocduan != ''
            GROUP BY thuocduan
            ORDER BY c DESC
            LIMIT 5
        """
        cursor.execute(sql_sample)
        rows = cursor.fetchall()
        for row in rows:
            if isinstance(row, dict):
                print(f"- {row['thuocduan']} ({row['c']})")
            else:
                print(f"- {row[0]} ({row[1]})")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_progress()
