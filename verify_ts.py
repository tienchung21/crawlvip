
from datetime import datetime
import time

ts = 1769568096639 / 1000
dt = datetime.fromtimestamp(ts)

print(f"Timestamp: {ts}")
print(f"Server Local Time: {dt.strftime('%d/%m/%Y %H:%M:%S')}")
print(f"Server Timezone: {time.tzname}")
