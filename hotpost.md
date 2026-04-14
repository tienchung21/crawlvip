 
TÀI LIỆU THIẾT KẾ HỆ THỐNG HOTSPOT BẤT ĐỘNG SẢN
 
1. MỤC TIÊU
Xác định khu vực có mật độ tin đăng bất động sản cao nhất trong từng phường.
 
2. DỮ LIỆU ĐẦU VÀO
Bảng tin:
- id
- lat
- lng
- ward_id
 
3. Ý TƯỞNG
Chia bản đồ thành các ô grid và đếm số lượng tin trong mỗi ô.
 
4. CÔNG THỨC GRID
FLOOR(lat / step) * step
FLOOR(lng / step) * step
 
Step khuyến nghị: 0.002 (~200m)
 
5. DATABASE
Bảng: tin_grid_stat
- id
- ward_id
- lat_grid
- lng_grid
- step
- total
- updated_at
 
6. XỬ LÝ DỮ LIỆU
 (xem lại có phù hợp ko )
Sử dụng batch job (cron)
 
BEGIN;
DELETE FROM tin_grid_stat;
 
INSERT INTO tin_grid_stat (...)
SELECT
	ward_id,
	FLOOR(lat / 0.002) * 0.002,
	FLOOR(lng / 0.002) * 0.002,
	0.002,
	COUNT(*)
FROM tin
GROUP BY ward_id, lat_grid, lng_grid;
 
COMMIT;
 
Khuyến nghị: xử lý theo từng ward để tránh lock toàn bảng.
 
7. API
 
SELECT *
FROM tin_grid_stat
WHERE ward_id = ?
ORDER BY total DESC
LIMIT 3;
 

4. Góp ý bổ sung (Nâng cấp)
Để tài liệu này chuyên nghiệp hơn, bạn nên cân nhắc:
Xử lý tọa độ rác: Thêm điều kiện WHERE lat IS NOT NULL AND lng IS NOT NULL để tránh lỗi tính toán.
Trường total: Nếu dữ liệu lớn, hãy cân nhắc chỉ lấy những ô có total > 3 (hoặc một ngưỡng nào đó) để loại bỏ các điểm tin lẻ tẻ, giúp bản đồ Hotspot "sạch" hơn.

