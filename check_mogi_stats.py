
import os
import sys
from craw.database import DatabaseConnection

def check_stats():
    db = DatabaseConnection()
    conn = db.get_connection()
    cursor = conn.cursor()

    print("--- Stats Check ---")

    # Check total records in data_full for 2025/2026
    sql_total = """
        SELECT COUNT(*) FROM data_full 
        WHERE (created_at >= '2025-01-01' OR posted_date >= '2025-01-01')
        AND source = 'mogi.vn'
    """
    cursor.execute(sql_total)
    total_2025 = cursor.fetchone()[0]
    print(f"Total Mogi records (2025+): {total_2025}")

    # Check project_name coverage
    sql_project = """
        SELECT COUNT(*) FROM data_full 
        WHERE (created_at >= '2025-01-01' OR posted_date >= '2025-01-01')
        AND source = 'mogi.vn'
        AND project_name IS NOT NULL
    """
    cursor.execute(sql_project)
    project_count = cursor.fetchone()[0]
    print(f"Mogi records with project_name: {project_count}")

    # Check lat/long coverage
    sql_latlong = """
        SELECT COUNT(*) FROM data_full 
        WHERE (created_at >= '2025-01-01' OR posted_date >= '2025-01-01')
        AND source = 'mogi.vn'
        AND lat IS NOT NULL AND `long` IS NOT NULL
    """
    cursor.execute(sql_latlong)
    latlong_count = cursor.fetchone()[0]
    print(f"Mogi records with lat/long: {latlong_count}")
    
    # Check id_img coverage
    sql_id_img = """
        SELECT COUNT(*) FROM data_full 
        WHERE (created_at >= '2025-01-01' OR posted_date >= '2025-01-01')
        AND source = 'mogi.vn'
        AND id_img IS NOT NULL
    """
    cursor.execute(sql_id_img)
    id_img_count = cursor.fetchone()[0]
    print(f"Mogi records with id_img: {id_img_count}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_stats()
