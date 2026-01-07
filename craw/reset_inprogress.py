from database import Database

db = Database()
conn = db.get_connection()
cursor = conn.cursor()
cursor.execute("UPDATE collected_links SET status = 'PENDING' WHERE status = 'IN_PROGRESS'")
conn.commit()
print(f"Reset {cursor.rowcount} links from IN_PROGRESS to PENDING")
cursor.close()
conn.close()
