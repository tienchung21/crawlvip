BÁO CÁO PHÂN TÍCH
CHỈ SỐ THANH KHOẢN BẤT ĐỘNG SẢN
 (Liquidity & Vitality Index)

1. Mục tiêu phân tích
Xây dựng một hệ thống chỉ số nhằm:
Đo mức độ sôi động của thị trường bất động sản


Đánh giá khả năng giao dịch (thanh khoản)


So sánh hiệu quả giữa các khu vực và loại hình


Phân tích được thực hiện theo:
Phường


Tỉnh


Loại hình bất động sản


Dữ liệu đầu vào: dữ liệu tin đăng (Listing Data).

2. Quy trình xử lý dữ liệu
Quy trình gồm 4 bước chính:
Bước 1: Lọc nhiễu dữ liệu
Trong mỗi nhóm (Khu vực + Loại hình):
Loại bỏ 10 phần trăm tin có giá thấp nhất


Loại bỏ 10 phần trăm tin có giá cao nhất


Giữ lại khoảng giá từ P10 đến P90.
Mục đích:
Loại bỏ tin giá ảo


Loại bỏ bất động sản siêu cao cấp không đại diện thị trường


Giảm nhiễu trước khi tính toán



Bước 2: Tính hệ số biến thiên (CV)
Sau khi lọc dữ liệu, tính:
Standard Deviation (độ lệch chuẩn)


Median (giá trung vị)


Công thức:
CV = Standard Deviation / Median
Ý nghĩa:
CV càng nhỏ → giá ổn định → thanh khoản tốt


CV càng lớn → giá biến động mạnh → thanh khoản thấp


Sử dụng Median thay vì Mean để tránh bị ảnh hưởng bởi giá lệch.

Bước 3: Tính điểm Vitality Score 
Công thức:
Vitality Score = N / CV
Trong đó:
n = số lượng tin đăng trong kỳ


Ý nghĩa:
Nhiều tin đăng nhưng giá loạn → điểm trung bình


Nhiều tin đăng và giá ổn định → điểm cao


Ít tin và giá biến động mạnh → điểm thấp



3. Phân loại thanh khoản theo từng tỉnh
Điểm Vitality Score sẽ được so sánh nội bộ trong từng tỉnh.
Phân loại:
Thanh khoản Cao
 → Thuộc Top 25 phần trăm điểm cao nhất trong tỉnh
Thanh khoản Trung bình
 → Thuộc nhóm 25 đến 75 phần trăm
Thanh khoản Thấp
 → Thuộc Bottom 25 phần trăm
Không đủ dữ liệu
 → n nhỏ hơn 10
Phương pháp này đảm bảo so sánh công bằng giữa tỉnh lớn và tỉnh nhỏ.

=> lưu ý : Theo công thức này chỉ tính thanh khoản cao hay thấp theo từng tỉnh

KẾT QUẢ TÍNH VITALITY SCORE
Ward: Hạnh Thông (347)
 Công thức sử dụng:
Vitality Score = N / CV
 (CV sử dụng dạng số thập phân)

Tháng 2025-12
Group 1
 Density (n): 345
 CV: 0.375042
 Vitality Score: 919.90
Group 2
 Density (n): 20
 CV: 0.277844
 Vitality Score: 71.98
Group 3
 Density (n): 7
 CV: 0.266668
 Vitality Score: 26.25
Group 4
 Density (n): 427
 CV: 1.781987
 Vitality Score: 239.62

Tháng 2026-01
Group 1
 Density (n): 411
 CV: 0.373825
 Vitality Score: 1099.45
Group 2
 Density (n): 36
 CV: 0.358207
 Vitality Score: 100.50
Group 3
 Density (n): 5
 CV: 0.155074
 Vitality Score: 32.24
Group 4
 Density (n): 416
 CV: 0.604908
 Vitality Score: 687.71

Tháng 2026-02
Group 1
 Density (n): 51
 CV: 0.404037
 Vitality Score: 126.23
Group 2
 Density (n): 1
 CV: 0
 Vitality Score: NULL (không thể chia cho 0)
Group 3
 Density (n): 1
 CV: 0
 Vitality Score: NULL (không thể chia cho 0)
Group 4
 Density (n): 47
 CV: 0.386584
 Vitality Score: 121.58





lưu ý : dùng python để xử lý các công thức meidan của từng loại hình và khu vực đã có trong bảng data_median ,
chỉ cần tính toán đến bước VITALITY SCORE cho từng group và ward, tỉnh theo từng tháng , quý , năm rồi lưu vào 1 bảng là thanh khoản , 
