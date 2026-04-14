
import sys
import os

sys.path.append(os.getcwd())
from craw.database import Database

def check_values():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT loaihinh, trade_type, COUNT(*) FROM scraped_details_flat WHERE domain='mogi' GROUP BY loaihinh, trade_type ORDER BY trade_type, loaihinh")
    rows = cursor.fetchall()
    
    print("--- DISTINCT LOAIHINH & TRADE_TYPE ---")
    for row in rows:
        # Check if row is tuple or dict
        if isinstance(row, dict):
             print(f"{row['trade_type']} | {row['loaihinh']} | {row['COUNT(*)']}")
        else:
             print(f"{row[1]} | {row[0]} | {row[2]}")

if __name__ == "__main__":
    check_values()
