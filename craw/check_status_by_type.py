
import os
import sys

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    pass

def main():
    db = Database()
    conn = db.get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT 
            loaihinh, 
            status,
            COUNT(*) as cnt
        FROM collected_links 
        WHERE domain = 'batdongsan.com.vn' 
        GROUP BY loaihinh, status
        ORDER BY cnt DESC
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    print("TIẾN ĐỘ THEO LOẠI HÌNH (STATUS):")
    
    # Process into Dict
    stats = {}
    for r in rows:
        if isinstance(r, dict):
            name = r.get('loaihinh', 'Unknown')
            st = r.get('status', 'NULL')
            cnt = r.get('cnt', 0)
        else:
            name = r[0]
            st = r[1]
            cnt = r[2]
            
        if not st: st = 'new'
            
        if name not in stats:
            stats[name] = {'total': 0, 'done': 0, 'new': 0, 'failed': 0, 'other': 0}
            
        stats[name]['total'] += cnt
        if st == 'done':
            stats[name]['done'] += cnt
        elif st == 'new':
            stats[name]['new'] += cnt
        elif st == 'failed':
            stats[name]['failed'] += cnt
        else:
            stats[name]['other'] += cnt

    # Print
    print(f"{'LOẠI HÌNH':<30} | {'TỔNG':<10} | {'DONE':<10} | {'NEW':<10} | {'FAILED':<10}")
    print("-" * 80)
    for name, s in stats.items():
        if s['total'] > 1000: # Filter small
            print(f"{name:<30} | {s['total']:<10} | {s['done']:<10} | {s['new']:<10} | {s['failed']:<10}")

if __name__ == "__main__":
    main()
