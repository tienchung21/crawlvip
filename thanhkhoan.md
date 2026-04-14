

Hệ thống Phân tích Chỉ số Độ sôi động Thị trường (MVI)
1. Định nghĩa & Mục tiêu
Chỉ số Market Vitality Index (MVI) là một thước đo định lượng dùng để đánh giá trạng thái sức khỏe của thị trường bất động sản thông qua dữ liệu tin đăng (Listing Data).
Mục tiêu cốt lõi:
Xác định "Sức nóng" thông qua quy mô nguồn cung (N).
Xác định "Độ nhiễu" thông qua biến động giá (SD và CV).
Hỗ trợ ra quyết định đầu tư dựa trên sự đồng thuận về giá của thị trường.

2. Quy trình Xử lý Dữ liệu & Công thức Toán học
Bước 1: Làm sạch dữ liệu (Data Cleaning)
Loại bỏ nhiễu bằng phương pháp cắt biên (Trimming). Chỉ giữ lại các giá trị trong khoảng từ Percentile 10 (P
10
​
) đến Percentile 90 (P
90
​
):
Price
clean
​
={x∣P
10
​
≤x≤P
90
​
}
Bước 2: Tính toán các chỉ số thống kê
Sau khi có tập dữ liệu sạch, Agent cần tính toán 4 thông số cơ bản:
Số lượng tin đăng (N): Quy mô mẫu.
Giá trung bình (μ hoặc Mean):
μ=
N
∑
i=1
N
​
x
i
​
​
Độ lệch chuẩn (SD - Standard Deviation): Đo mức độ phân tán của giá.
σ=
N
∑
i=1
N
​
(x
i
​
−μ)
2
​

​
Hệ số biến thiên (CV - Coefficient of Variation): Dùng để so sánh độ ổn định giữa các khu vực có mặt bằng giá khác nhau (ví dụ: Quận 1 vs. Cần Giờ).
CV=
μ
σ
​

Getty Images
Bước 3: Công thức tính MVI
Chỉ số MVI được xác định bằng tỉ lệ giữa quy mô nguồn cung và độ lệch chuẩn của giá.
MVI=
σ
N
​
Logic: MVI tỷ lệ thuận với N (Càng nhiều hàng càng sôi động) và tỷ lệ nghịch với σ (Giá càng loạn, chỉ số càng thấp).

3. Logic Giải thích cho Agent (Business Intelligence)
Khi người dùng hỏi "Tại sao?", Agent cần giải thích dựa trên các lập luận sau:
Thông số
Ý nghĩa thực tế
Hệ quả
N cao
Nguồn cung dồi dào.
Thị trường có tính thanh khoản cao.
SD nhỏ
Giá các căn nhà sát nhau.
Người mua tin tưởng, dễ ra quyết định (Thị trường minh bạch).
SD lớn
"Ngáo giá" hoặc loạn giá.
Người mua hoang mang, sợ mua hớ (Thị trường nhiễu).
CV
Tỷ lệ % lệch giá.
Dùng để so sánh công bằng giữa khu vực giá cao và giá thấp.


4. Hệ thống Phân loại (Vitality Tiers)
Dựa trên bảng xếp hạng Percentile của điểm MVI trong cùng một Tỉnh/Thành:
Sôi động (Hot): MVI≥P
75
​
(Top 25% cao nhất).
Ổn định (Stable): P
25
​
≤MVI<P
75
​
.
Trầm lắng (Quiet): MVI<P
25
​
.
Dữ liệu yếu: N<10 (Không đủ độ tin cậy thống kê).

5. Ví dụ Case Study
Tháng
N
SD (σ)
MVI
Trạng thái
12/2025
345
0.45
766.6
Sôi động
02/2026
51
0.42
121.4




