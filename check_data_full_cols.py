
import sys
import os

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def check_data_full():
    print("=== CHECKING data_full COLUMNS ===")
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("DESCRIBE data_full")
        columns = []
        rows = cursor.fetchall()
        for row in rows:
            # Handle both dict and tuple cursor
            col_name = list(row.values())[0] if isinstance(row, dict) else row[0]
            columns.append(col_name)
        
        print(f"Columns: {columns}")
        if 'project_name' in columns:
            print("Has project_name")
        elif 'thuocduan' in columns:
            print("Has thuocduan")
        else:
            print("MISSING project info column")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_data_full()
