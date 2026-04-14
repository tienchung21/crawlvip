
import os
import sys
# Bypass proxy for localhost to prevent ERR_TUNNEL_CONNECTION_FAILED 
os.environ['no_proxy'] = '127.0.0.1,localhost'

from database import Database

def check_task():
    db = Database()
    
    # Get Task Info
    print("--- Task 75 Info ---")
    conn = db.get_connection()
    try:
        with conn.cursor() as cursor:
            # Check Schema
            print("--- Table Columns ---")
            cursor.execute("DESCRIBE scheduler_tasks")
            cols = cursor.fetchall()
            for c in cols:
                print(c['Field'])

            # Task Info (Select All for now)
            sql = "SELECT * FROM scheduler_tasks WHERE id = 75"
            cursor.execute(sql)
            task = cursor.fetchone()
            
            if task:
                for k, v in task.items():
                    print(f"{k}: {v}")
            else:
                print("Task 75 not found")
                return

            # Recent Logs
            print("\n--- Recent Logs for Task 75 ---")
            sql_log = "SELECT status, message, created_at FROM scheduler_logs WHERE task_id = 75 AND created_at > '2026-01-28 08:58:00' ORDER BY id DESC LIMIT 100"
            cursor.execute(sql_log)
            logs = cursor.fetchall()
            for log in logs:
                print(f"[{log['created_at']}] {log['status']}: {log['message']}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_task()
