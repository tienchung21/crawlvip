# Auto Merge & Sync Scripts Documentation

Thư mục này chứa các script Python tự động hóa quy trình ETL, đồng bộ địa chính và chuẩn hóa dữ liệu.

## A. Nhóm Script MOGI ETL (Quan trọng hàng ngày)

### 1. `run_mogi_etl.py` (Mogi ETL Orchestrator)
*   **Chức năng**: Script CHÍNH để chạy hàng ngày sau khi crawl.
*   **Logic**:
    *   Chuyển dữ liệu từ `scraped_details_flat` (Mogi thô) sang `data_full` (Bảng chuẩn).
    *   Sử dụng **Direct ID Mapping**: Join bảng `location_mogi` để lấy CafeLand ID ngay lập tức.
    *   Tự động tính toán giá, diện tích, quy chuẩn dữ liệu.

### 2. `sql_convert_mogi.sql`
*   **Chức năng**: File SQL chứa câu lệnh logic cho `run_mogi_etl.py`. Chỉ chỉnh sửa khi cần thay đổi logic mapping cột.

### 3. `update_full_status.py`
*   **Chức năng**: Tiện ích chạy bổ sung để tính lại cột `full` (ví dụ: khi thay đổi định nghĩa "thế nào là tin full").

---

## B. Nhóm Sync & Map Địa Chính (Location System)

### 4. `sync_mogi_locations.py`
*   **Chức năng**: **Cào toàn bộ danh sách địa chính** (Tỉnh -> Huyện -> Xã -> Đường) từ API của Mogi.vn về bảng `location_mogi`.
*   **Khi nào chạy**: Khi Mogi cập nhật địa giới hành chính hoặc chạy setup lần đầu.

### 5. `merge_mogi_locations.py`
*   **Chức năng**: **Map ID Mogi sang CafeLand ID**.
*   **Logic**:
    *   Chạy so khớp tên (Fuzzy Matching) giữa `location_mogi` và `transaction_city`.
    *   Điền ID khớp vào cột `cafeland_id` trong bảng `location_mogi`.
    *   Tự động cập nhật bảng `transaction_city_merge`.

### 6. `map_mogi_to_transaction_city.py`
*   **Chức năng**: Script cũ/thử nghiệm logic map địa chỉ (tương tự `merge_mogi_locations.py` nhưng logic khác). Ít dùng hơn.

### 7. `sync_levels_1_2.py`
*   **Chức năng**: Đồng bộ ID cấp Tỉnh/Huyện cho hệ thống Nhatot cũ.

### 8. `sync_all_provinces.py`
*   **Chức năng**: Đồng bộ ID cấp Xã cho hệ thống Nhatot cũ (quét tên xã).

### 9. `check_merge_table.py`
*   **Chức năng**: Kiểm tra nhanh dữ liệu bảng `transaction_city_merge` để debug.

---

## C. Nhóm Data Clean & Nhatot (Hệ thống cũ/Song song)

### 10. `merge_new_id_dataclean.py`
*   **Chức năng**: Cập nhật cột `cafeland_new_id`, `cafeland_new_parent_id` trong bảng `data_clean` dựa trên bảng Merge. (Dùng cho Nhatot/Data Clean).

### 11. `update_median_group.py`
*   **Chức năng**: Phân loại bất động sản (`median_group` 1,2,3,4) dựa trên `type` và `category` trong bảng `data_clean`. Phục vụ tính giá trung vị.

### 12. `recreate_data_clean.py`
*   **Chức năng**: **XÓA TRẮNG** và nạp lại toàn bộ bảng `data_clean` từ `ad_listing_detail` (Nhatot). **Cẩn thận: Dữ liệu lớn sẽ rất lâu.**

### 13. `batch_insert_data_clean.py`
*   **Chức năng**: Giống `recreate_data_clean.py` nhưng chạy theo Batch (từng lô 10k dòng) để tránh treo máy.

### 14. `update_list_ym.py`
*   **Chức năng**: Tính toán cột `list_ym` (Tháng đăng tin) cho `data_clean`.

---

## D. Nhóm Utility (Hỗ trợ)

### 15. `recreate_data_full_table.py`
*   **Chức năng**: Xóa và tạo lại bảng `data_full` (Reset schema). Chỉ dùng khi muốn làm sạch dữ liệu từ đầu.

---

**QUY TRÌNH SETUP HỆ THỐNG MỚI (MOGI):**
1.  Chạy `sync_mogi_locations.py`: Lấy full địa chính Mogi.
2.  Chạy `merge_mogi_locations.py`: Map ID Mogi -> CafeLand.
3.  (Hàng ngày) Chạy `daily_mogi_crawl.py`.
4.  (Hàng ngày - Tự động) Chạy `run_mogi_etl.py`.
