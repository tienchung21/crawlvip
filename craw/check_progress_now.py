
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from database import Database
except ImportError:
    # Try adding parent directory if running from subdir
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from craw.database import Database

def check():
    db = Database()
    print("CATEGORY                       | ONLINE TOTAL | DB COUNT   | PROGRESS")
    print("-" * 70)

    # Categories and their roughly estimated online totals
    categories = {
        "Nhà mặt tiền phố": 86396,
        "Nhà biệt thự, liền kề": 15880,
        "Đường nội bộ": 2635,
        "Nhà hẻm ngõ": 21947,
        "Căn hộ chung cư": 151062,
        "Căn hộ tập thể, cư xá": 434,
        "Căn hộ dịch vụ": 103944,
        "Căn hộ Officetel": 2361, 
        "Căn hộ Penthouse": 46,
        "Phòng trọ, nhà trọ": 45435, 
        "Văn phòng": 15000, 
        "Nhà xưởng, kho bãi": 5000 
    }

    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT loaihinh, COUNT(*) FROM collected_links WHERE domain='mogi.vn' AND trade_type='thuê' GROUP BY loaihinh")
    rows = cursor.fetchall()
    
    db_counts = {}
    for row in rows:
        key = row[0]
        val = row[1]
        db_counts[key] = val

    for cat, total in categories.items():
        current = db_counts.get(cat, 0)
        percent = (current / total * 100) if total > 0 else 0
        print(f"{cat:<30} | {total:<12} | {current:<10} | {percent:.1f}%")

    print("-" * 70)
    total_online = sum(categories.values())
    total_crawled = sum(db_counts.values())
    pct = (total_crawled / total_online * 100) if total_online > 0 else 0
    print(f"TOTAL ESTIMATED                | ~{total_online:<11} | {total_crawled:<10} | {pct:.1f}%")

if __name__ == "__main__":
    check()
