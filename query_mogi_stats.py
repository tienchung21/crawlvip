
import os
import sys

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
    from craw.database import Database

def main():
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    
    print("Distinct Domains:")
    try:
        cur.execute("SELECT DISTINCT domain FROM scraped_details_flat LIMIT 20;")
        rows = cur.fetchall()
        for r in rows:
            print(f" - {r}")
            
        print("\nChecking Ngaydang Format:")
        cur.execute("SELECT ngaydang FROM scraped_details_flat WHERE domain LIKE '%%mogi%%' LIMIT 20;")
        rows = cur.fetchall()
        for r in rows:
            print(f" - {r}")
        
        print("\nDate Distribution by Year:")
        # Group by Year (Last 4 chars)
        query = """
            SELECT RIGHT(ngaydang, 4) as yy, COUNT(*) 
            FROM scraped_details_flat 
            WHERE domain LIKE '%%mogi%%' 
            GROUP BY yy 
            ORDER BY COUNT(*) DESC 
            LIMIT 20;
        """
        cur.execute(query)
        rows = cur.fetchall()
        for r in rows:
             print(f" - {r}")
        
    except Exception as e:
        print(f"Error: {e}")
        
    conn.close()

if __name__ == "__main__":
    main()
