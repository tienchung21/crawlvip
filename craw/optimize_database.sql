-- Script để optimize database và giảm lag cho dashboard
-- Chạy: /opt/lampp/bin/mysql -u root craw_db < optimize_database.sql

-- 1. Thêm index thiếu để tăng tốc query (chỉ add nếu column tồn tại)
-- ALTER TABLE collected_links ADD INDEX IF NOT EXISTS idx_created_at (created_at);
-- ALTER TABLE ad_listing_detail ADD INDEX IF NOT EXISTS idx_list_time (list_time);
-- Bỏ qua source vì column không tồn tại
-- ALTER TABLE data_clean ADD INDEX IF NOT EXISTS idx_source (source);
-- ALTER TABLE scraped_details_flat ADD INDEX IF NOT EXISTS idx_crawl_date (crawl_date);

-- 2. Optimize các table lớn
OPTIMIZE TABLE collected_links;
OPTIMIZE TABLE ad_listing_detail;
OPTIMIZE TABLE data_clean;
OPTIMIZE TABLE scraped_detail_images;
OPTIMIZE TABLE scraped_details_flat;

-- 3. Xóa dữ liệu cũ/rác nếu cần (UNCOMMENT để sử dụng)
-- DELETE FROM scraped_detail_images WHERE status = 'FAILED' AND created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
-- DELETE FROM collected_links WHERE status = 'FAILED' AND created_at < DATE_SUB(NOW(), INTERVAL 60 DAY);

-- 4. Analyze tables để cập nhật statistics
ANALYZE TABLE collected_links;
ANALYZE TABLE ad_listing_detail;
ANALYZE TABLE data_clean;
ANALYZE TABLE scraped_detail_images;

SELECT 'Database optimization completed!' AS status;
