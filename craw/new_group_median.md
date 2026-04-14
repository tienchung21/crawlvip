----- tự tìm id chuẩn loại hình tưng uứng 
1. Domain: nhadat.vn (Cột A)
Danh sách loại hình:
Nhóm Đất (Trừ 0%): Bán đất nền dự án, Bán đất thổ cư, Bán đất nông, lâm nghiệp.
Nhóm Có nhà (Trừ 15%): Bán nhà riêng, Bán nhà phố dự án, Bán biệt thự, Bán kho, nhà xưởng, Bán nhà hàng - Khách sạn.
BỎ: Bán căn hộ chung cư, Bán căn hộ Mini, Dịch vụ.

2. Domain: guland.vn (Cột B)
Danh sách loại hình:
Nhóm Đất (Trừ 0%): Đất.
Nhóm Có nhà (Trừ 15%): Nhà riêng, Kho, nhà xưởng	
BỎ: Căn hộ chung cư, Văn phòng, Khách sạn, Mặt bằng kinh doanh., Nhà trọ, Phòng trọ.

3. Domain: nhatot.com (Cột C)
Danh sách loại hình:
Nhóm Đất (Trừ 0%): 1040 Đất (Đất nền, đất thổ cư).
Nhóm Có nhà (Trừ 15%): 1020 Nhà ở (Nhà phố, biệt thự, nhà hẻm).
BỎ: 1010 Căn hộ/Chung cư, 1030 Văn phòng/Mặt bằng kinh doanh.

4. Domain: mogi.vn (Cột D)
Danh sách loại hình:
Nhóm Đất (Trừ 0%): Thổ cư, Nền dự án, Nông nghiệp.
Nhóm Có nhà (Trừ 15%): Mặt tiền, phố, Biệt thự, liền kề, Đường nội bộ, Hẻm, ngõ, Kho xưởng.
BỎ: Chung cư, Tập thể, cư xá, Penthouse, Căn hộ dịch vụ, Officetel, Các loại hình sang nhượng quán/shop.

5. Domain: https://www.google.com/search?q=batdongsan.com.vn (Cột E)
Danh sách loại hình:
Nhóm Đất (Trừ 0%): Đất kho xưởng, Đất nền dự án, Đất nông nghiệp, Đất thổ cư.
Nhóm Có nhà (Trừ 15%): Đường nội bộ, Nhà biệt thự, liền kề, Nhà hẻm ngõ, Nhà mặt tiền phố.
BỎ: Căn hộ chung cư, Căn hộ dịch vụ, Officetel, Penthouse, Tập thể.

6. Domain: alonhadat.com.vn (Cột F)
Danh sách loại hình:
Nhóm Đất (Trừ 0%): Đất thổ cư, đất ở, Đất nền, liền kề, đất dự án, Đất nông, lâm nghiệp.
Nhóm Có nhà (Trừ 15%): Nhà mặt tiền, Nhà trong hẻm, Biệt thự, nhà liền kề, Kho, xưởng, Trang trại.
BỎ: Căn hộ chung cư, Văn phòng, Nhà hàng, khách sạn, Shop, kiot, quán, Mặt bằng., Phòng trọ, nhà trọ,

LOGIC XỬ LÝ NGOẠI LỆ (ĐỐI CHIẾU XÃ/TỈNH)
Để thực hiện yêu cầu "xấp xỉ bằng thì lấy giá đất luôn", bạn hãy chạy thêm một hàm xử lý sau khi đã phân loại:
Tính giá chuẩn khu vực: Tính Đơn giá trung bình ($UP_{land}$) của các tin thuộc Nhóm Đất trong cùng một Xã (hoặc Huyện/Tỉnh nếu xã không đủ dữ liệu).
So sánh tin Có nhà: Với một tin đăng thuộc Nhóm Có nhà, lấy đơn giá tổng của nó ($UP_{total}$) so sánh với $UP_{land}$ của khu vực.
Điều kiện áp dụng:
Nếu $UP_{total} \approx UP_{land}$ (ví dụ chênh lệch < 5-10%): Coi như đây là nhà nát, tính giá đất = 100% giá trị tin đăng (Trừ 0%).
Ngược lại: Áp dụng mức Trừ 15% như mặc định.
Công thức tổng quát trong code của bạn:
$Giá\_đất = P_{total} \times (1 - \text{Hệ số})$
Hệ số = 0 (Nếu là loại hình Đất).
Hệ số = 0 (Nếu là loại hình Nhà nhưng giá xấp xỉ giá đất khu vực).
Hệ số = 0.15 (Nếu là loại hình Nhà bình thường).



