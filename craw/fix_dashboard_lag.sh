#!/bin/bash
# Script tổng hợp để fix lag dashboard

echo "========================================="
echo "Fix Dashboard Lag - Crawlvip Project"
echo "========================================="

# 1. Kiểm tra MySQL đang chạy
echo -e "\n[1/4] Checking MySQL status..."
if /opt/lampp/bin/mysql -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "✓ MySQL is running (XAMPP)"
else
    echo "✗ MySQL not running! Please start XAMPP first:"
    echo "  sudo /opt/lampp/lampp start"
    exit 1
fi

# 2. Optimize database
echo -e "\n[2/4] Optimizing database..."
/opt/lampp/bin/mysql -u root craw_db < /home/chungnt/crawlvip/craw/optimize_database.sql
if [ $? -eq 0 ]; then
    echo "✓ Database optimized"
else
    echo "✗ Database optimization failed"
fi

# 3. Kiểm tra scheduler service
echo -e "\n[3/4] Checking scheduler service..."
if pgrep -f "scheduler_service.py" >/dev/null; then
    echo "✓ Scheduler service is running"
else
    echo "⚠ Scheduler service NOT running. To start:"
    echo "  cd /home/chungnt/crawlvip/craw"
    echo "  nohup python scheduler_service.py > scheduler.log 2>&1 &"
fi

# 4. Xóa profile cũ để reset browser session
echo -e "\n[4/4] Cleaning old browser profiles..."
cd /home/chungnt/crawlvip/craw
rm -rf nodriver_profile_listing_*/Default/Cache/* 2>/dev/null
rm -rf playwright_profile_tab3_detail_*/Default/Cache/* 2>/dev/null
echo "✓ Browser caches cleaned"

echo -e "\n========================================="
echo "✓ Dashboard fix completed!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Restart dashboard: streamlit run dashboard.py"
echo "2. Test Tab 5 crawl with 'Show browser = True'"
echo "3. Check logs for any errors"
echo ""
