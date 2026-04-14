
import os
import sys
import json
from pathlib import Path

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    pass

def main():
    # File Path
    json_path = "/home/chungnt/Downloads/batdongsan_full.json"
    if not os.path.exists(json_path):
        print(f"File not found: {json_path}")
        return

    print("Reading JSON file...")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"Loaded {len(data)} cities.")

    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()

    # 1. Create Tables
    print("Creating Tables...")
    
    # Table 1: location_batdongsan (Ward Level Flattened)
    # Stores: Ward, District, City
    cur.execute("""
        CREATE TABLE IF NOT EXISTS location_batdongsan (
            ward_id INT PRIMARY KEY,
            ward_name VARCHAR(255),
            district_id INT,
            district_name VARCHAR(255),
            city_code VARCHAR(50),
            city_name VARCHAR(255),
            INDEX (district_id),
            INDEX (city_code)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)

    # Table 2: streets_batdongsan
    # Stores: Street, District
    cur.execute("""
        CREATE TABLE IF NOT EXISTS streets_batdongsan (
            street_id INT PRIMARY KEY,
            street_name VARCHAR(255),
            district_id INT,
            INDEX (district_id)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)

    # Table 3: duan_batdongasan
    # Stores: Project, District, Ward, Street
    cur.execute("""
        CREATE TABLE IF NOT EXISTS duan_batdongasan (
            project_id INT PRIMARY KEY,
            project_name VARCHAR(255),
            district_id INT,
            ward_id INT,
            street_id INT,
            lat DOUBLE,
            lng DOUBLE,
            INDEX (district_id),
            INDEX (ward_id),
            INDEX (street_id)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)

    conn.commit()

    # 2. Insert Data
    count_wards = 0
    count_streets = 0
    count_projects = 0
    
    # Prepare Insert Statements
    sql_loc = """
        INSERT INTO location_batdongsan 
        (ward_id, ward_name, district_id, district_name, city_code, city_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            ward_name=VALUES(ward_name), 
            district_name=VALUES(district_name),
            city_name=VALUES(city_name)
    """
    
    sql_street = """
        INSERT INTO streets_batdongsan
        (street_id, street_name, district_id)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            street_name=VALUES(street_name)
    """
    
    sql_project = """
        INSERT INTO duan_batdongasan
        (project_id, project_name, district_id, ward_id, street_id, lat, lng)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            project_name=VALUES(project_name),
            ward_id=VALUES(ward_id),
            street_id=VALUES(street_id),
            lat=VALUES(lat),
            lng=VALUES(lng)
    """

    print("Importing Data...")
    
    for city in data:
        city_name = city.get('city_name')
        city_code = city.get('city_code')
        districts = city.get('districts', [])
        
        for dist in districts:
            d_info = dist.get('district_info', {})
            dist_id = d_info.get('id')
            dist_name = d_info.get('name')
            
            if not dist_id: continue
            
            # Wards
            wards = dist.get('wards', [])
            for w in wards:
                w_id = w.get('id')
                w_name = w.get('name')
                if w_id:
                    cur.execute(sql_loc, (w_id, w_name, dist_id, dist_name, city_code, city_name))
                    count_wards += 1
            
            # Streets
            streets = dist.get('streets', [])
            for s in streets:
                s_id = s.get('id')
                s_name = s.get('name')
                if s_id:
                    cur.execute(sql_street, (s_id, s_name, dist_id))
                    count_streets += 1
            
            # Projects
            projects = dist.get('projects', [])
            for p in projects:
                p_id = p.get('id')
                p_name = p.get('name')
                
                # Check Linked Ward/Street
                curr_ward = p.get('ward') or {}
                curr_street = p.get('street') or {}
                
                # Coords
                lat = p.get('lat')
                lng = p.get('lng')
                
                if p_id:
                    cur.execute(sql_project, (
                        p_id, p_name, 
                        dist_id, 
                        curr_ward.get('id'), 
                        curr_street.get('id'),
                        lat, lng
                    ))
                    count_projects += 1
                    
        conn.commit()
        print(f"Processed City: {city_name}")

    print("="*40)
    print("IMPORT FINISHED")
    print(f"Total Wards: {count_wards}")
    print(f"Total Streets: {count_streets}")
    print(f"Total Projects: {count_projects}")
    
    conn.close()

if __name__ == "__main__":
    main()
