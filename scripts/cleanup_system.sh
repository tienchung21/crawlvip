#!/bin/bash

# cleanup_system.sh - Cleans up zombie processes and temporary files

echo "Starting system cleanup..."

# 1. Kill zombie browser processes
echo "Killing chrome/selenium processes..."
pkill -f chrome
pkill -f chromium
pkill -f uc_driver
pkill -f chromedriver
pkill -f "python3 mogi_detail_crawler.py" # Ensure no hung crawlers
# Wait a bit
sleep 2

# 2. Clean /tmp directory (focus on selenium/chrome temp files)
echo "Cleaning /tmp..."
# Delete folders starting with .org.chromium.Chromium* (Selenium temp dirs)
find /tmp -name ".com.google.Chrome*" -type d -mmin +60 -exec rm -rf {} +
find /tmp -name ".org.chromium.Chromium*" -type d -mmin +60 -exec rm -rf {} +
find /tmp -name "scoped_dir*" -type d -mmin +60 -exec rm -rf {} +
find /tmp -name "tmp_addon_*" -type d -mmin +60 -exec rm -rf {} +
# Also clean old pyinstaller/seleniumbase stuff if any
find /tmp -name "seleniumbase*" -type d -mmin +1440 -exec rm -rf {} +

# Check disk usage
df -h /tmp

# 3. Ensure System MariaDB is stopped (using sudo if available, else just check)
# Assuming user runs this as root or has passwordless sudo for services
# But we are 'chungnt'. We might not have sudo. 
# We can try to stop it if we own it, or just log status.
echo "Checking mysql processes..."
ps aux | grep mysql

echo "Cleanup complete."
