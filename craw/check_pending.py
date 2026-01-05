from database import Database

db = Database(host='localhost', user='root', password='', database='craw_db')
conn = db.get_connection()
cur = conn.cursor()

# Check tasks 30, 31
cur.execute('SELECT id, name, domain, loaihinh FROM scheduler_tasks WHERE id IN (30, 31)')
print('Tasks 30, 31:')
for r in cur.fetchall():
    print(r)

# Check collected_links by domain/loaihinh/status
cur.execute("SELECT domain, loaihinh, status, COUNT(*) as cnt FROM collected_links GROUP BY domain, loaihinh, status ORDER BY domain, loaihinh, status")
print('\nCollected links by domain/loaihinh/status:')
for r in cur.fetchall():
    print(f"  domain='{r[0]}', loaihinh='{r[1]}', status='{r[2]}', count={r[3]}")

# Check if there's any pending link for alonhadat
cur.execute("SELECT COUNT(*) FROM collected_links WHERE domain='alonhadat' AND status='pending'")
print(f"\nPending links for alonhadat: {cur.fetchone()[0]}")

# Check distinct loaihinh values in collected_links for alonhadat
cur.execute("SELECT DISTINCT loaihinh FROM collected_links WHERE domain='alonhadat'")
print('\nLoaihinh values for alonhadat in collected_links:')
for r in cur.fetchall():
    print(f"  '{r[0]}'")

conn.close()
