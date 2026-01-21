# Auto Merge & Sync Scripts Documentation

Thư mục này chứa các script Python dùng để tự động đồng bộ ID, hợp nhất dữ liệu địa chính và chuẩn hóa bảng `data_clean`.

## Danh sách Script và Chức năng

### 1. `merge_new_id_dataclean.py` (Quan trọng nhất)
*   **Chức năng**: Tạo và cập nhật 3 cột mới trong bảng `data_clean` để phục vụ thống kê chính xác:
    *   `cafeland_new_id`: ID Xã chuẩn (lấy từ bảng Merge).
    *   `cafeland_new_name`: Tên Xã chuẩn.
    *   `cafeland_new_parent_id`: ID Huyện (Parent) để group theo khu vực.
*   **Logic**: `data_clean (cafeland_id)` -> `transaction_city_merge (old -> new)` -> `transaction_city (new -> parent)`.
*   **Khi nào chạy**: Chạy khi có dữ liệu crawl mới vào `data_clean` để điền ID chuẩn.

### 2. `sync_all_provinces.py`
*   **Chức năng**: Script chạy đệ quy quét toàn bộ 63 tỉnh thành để khớp ID Xã (Ward) từ Nhatot sang Cafeland.
*   **Logic**: Dùng thuật toán so khớp tên (normalize name) để tìm ID tương ứng.
*   **Output**: Cập nhật bảng `location_detail` hoặc bảng mapping trung gian.

### 3. `sync_levels_1_2.py`
*   **Chức năng**: Đồng bộ ID cấp Tỉnh (Level 1) và Huyện (Level 2).
*   **Logic**: Đảm bảo rằng mọi Tỉnh/Huyện trong `location_detail` đều đã được map sang ID của Cafeland.

### 4. `update_data_clean.py`
*   **Chức năng**: Script ban đầu để đẩy `cafeland_id` vào bảng `data_clean` dựa trên bảng mapping `location_detail`.
*   **Lưu ý**: Đây là bước sơ khởi, trước khi có logic Merge ID mới.

### 5. `merge_id_dataclean.py`
*   **Chức năng**: Script cũ dùng để update incremental `cafeland_id` cho các dòng mới. (Có thể coi là phiên bản cũ của quy trình update ID).

### 6. `check_merge_table.py`
*   **Chức năng**: Kiểm tra cấu trúc (Schema) và dữ liệu mẫu của bảng `transaction_city_merge`.
*   **Dùng để**: Debug xem bảng Merge đang chứa dữ liệu gì.

---
**Quy trình chạy tự động khuyến nghị:**
1.  Chạy Crawl để lấy dữ liệu thô.
2.  Chạy `sync_levels_1_2.py` & `sync_all_provinces.py` (nếu có tỉnh mới chưa map).
3.  Chạy `merge_new_id_dataclean.py` để chuẩn hóa ID trong `data_clean`.
### 7. `update_list_ym.py`
*   **Chức năng**: Tự động điền cột `list_ym` (Tháng đăng tin) dựa trên thời gian đăng tin gốc.
*   **Logic**: 
    *   Ưu tiên dùng `orig_list_time` (chuyển đổi timestamp -> YYYY-MM).
    *   Nếu không có `orig`, dùng `list_time` (fallback).
*   **Khi nào chạy**: Chạy sau bước Crawl để chuẩn hóa thời gian phục vụ thống kê theo tháng.

