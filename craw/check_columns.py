import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import Database

db = Database()
conn = db.get_connection()
cursor = conn.cursor()
try:
    cursor.execute("SELECT * FROM scraped_details_flat LIMIT 1")
    columns = [desc[0] for desc in cursor.description]
    print("COLUMNS IN scraped_details_flat:")
    for col in columns:
        print(f"- {col}")
    
    if "full" in columns:
        print("\nFOUND 'full' column!")
    else:
        print("\n'full' column NOT found.")
except Exception as e:
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()
