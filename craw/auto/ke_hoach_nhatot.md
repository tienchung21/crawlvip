# Kế hoạch Cập nhật: Chuẩn hóa Nhatot vào `data_clean_v1`

Kế hoạch này được điều chỉnh để chạy trên bảng thử nghiệm `data_clean_v1`, tập trung vào việc quy hoạch lại các cột ID theo yêu cầu: **Phân biệt rõ ID Nguồn (Nhatot) và ID Đích (Cafeland)**.

## 1. Tạo Bảng Thử Nghiệm (`data_clean_v1`)

Chúng ta sẽ tạo một bảng hoàn toàn mới `data_clean_v1` để chứa dữ liệu đã chuẩn hóa. Bảng này sẽ gọn gàng hơn, loại bỏ các cột rác không cần thiết.

### Cấu trúc các cột ID (7 cột cốt lõi)
Tập trung vào mapping vị trí từ cũ sang mới.

| Nhóm | Tên Cột | Kiểu | Nguồn (ad_listing_detail) | Mô tả |
| :--- | :--- | :--- | :--- | :--- |
| **Source IDs** | `src_province_id` | `INT` | `region_v2` | ID Tỉnh (Nhatot) |
| **Source IDs** | `src_district_id` | `INT` | `area_v2` | ID Huyện (Nhatot) |
| **Source IDs** | `src_ward_id` | `INT` | `ward` | ID Xã (Nhatot) |
| **Target IDs** | `cf_province_id` | `INT` | *(Mapping)* | ID Tỉnh (Cafeland) |
| **Target IDs** | `cf_district_id` | `INT` | *(Mapping)* | ID Huyện (Cafeland) |
| **Target IDs** | `cf_ward_id` | `INT` | *(Mapping)* | ID Xã (Cafeland) |
| **Target IDs** | `cf_street_id` | `BIGINT`| *(Mapping)* | ID Đường (Cafeland) - *Nếu map được* |

### Các cột dữ liệu khác (Giữ lại để analysis)
*   **Key**: `ad_id` (Primary Key của tin gốc), `url`.
*   **Attributes**: `price` (Gốc), `price_vnd` (Chuẩn), `size` (Gốc), `std_area` (Chuẩn).
*   **Type**: `src_category` (Gốc), `std_category` (Chuẩn), `std_trans_type` (Ban/Thue).
*   **Time**: `orig_list_time`, `std_date`.
*   **Status**: `process_status` (0-6 steps).

## 2. Quy trình Thực hiện (Script: `run_nhatot_etl_v1.py`)

### Bước 1: Tạo bảng `data_clean_v1`
*   Script sẽ kiểm tra và tạo bảng `data_clean_v1` nếu chưa có.
*   Cấu trúc tinh gọn, không copy thừa cột.

### Bước 2: Ingestion (Load & Transform)
Thay vì Insert rồi Update, ta có thể xử lý Transform ngay lúc Insert hoặc Insert xong chạy từng bước update tùy độ phức tạp. Để dễ debug từng bước (`process_status`), ta giữ nguyên flow Insert -> Update.

1.  **Insert thô**: Lấy `ad_id`, `region_v2`, `area_v2`, `ward`,... từ `ad_listing_detail` đập vào các cột `src_...`.
2.  **Mapping Location**: Dùng dictionary map từ `src_..._id` sang `cf_..._id`.
3.  **Các bước chuẩn hóa khác** (Giá, Diện tích, Thời gian): Tương tự kế hoạch cũ nhưng ghi vào các cột `std_`.

## 3. Chi tiết Mapping ID (Logic quan trọng nhất)

*   **Logic**:
    *   Load map Province: `src_province_id` -> `cf_province_id`.
    *   Load map District: `src_province_id` + `src_district_id` -> `cf_district_id` (Phải kèm province để tránh trùng ID huyện giữa các tỉnh nếu source ID không unique global).
    *   Load map Ward: `src_district_id` + `src_ward_id` -> `cf_ward_id`.

## 4. Kết quả mong đợi
Sau khi chạy xong, bảng `data_clean_v1` sẽ cho thấy rõ sự tương quan:
`src_district_id` (2010) -> `cf_district_id` (55)
Giúp dễ dàng kiểm tra tính chính xác của mapping.
