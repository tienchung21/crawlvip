# Mapping Field: ad_listing_detail -> data_clean_v1

Quy trình sẽ được chia làm 2 giai đoạn riêng biệt:
1.  **Giai đoạn 1: Import Raw (Đổ dữ liệu thô)** - Chỉ copy dữ liệu từ nguồn sang các cột `src_` và metadata.
2.  **Giai đoạn 2: Standardization (Chuẩn hóa)** - Chạy logic xử lý để tính toán ra `std_` và `price_vnd`.

## Giai đoạn 1: Import Raw (Thực hiện ngay)

Cần đảm bảo bảng đích `data_clean_v1` có đủ các cột `src_` dưới đây.

| Cột data_clean_v1 | Cột ad_listing_detail | Ghi chú |
| :--- | :--- | :--- |
| **`ad_id`** | `list_id` | Primary Key từ nguồn. |
| **`src_province_id`** | `region_v2` | ID Tỉnh gốc. |
| **`src_district_id`** | `area_v2` | ID Quận/Huyện gốc. |
| **`src_ward_id`** | `ward` | ID Phường/Xã gốc. |
| **`src_size`** | `size` | Diện tích gốc (raw). |
| **`src_price`** | `price_string` | Giá hiển thị gốc (raw). |
| **`src_category_id`** | `category` | ID loại hình gốc (vd: 1010, 1050). |
| **`src_type`** | `type` | Code loại giao dịch gốc (s/u). |
| **`orig_list_time`** | `orig_list_time` | Timestamp tạo tin. |
| **`update_time`** | `list_time` | Timestamp cập nhật tin. |
| **`url`** | *Constructed* | `https://www.nhatot.com/{list_id}.htm` |

*> Lưu ý: Tại bước này, các cột `std_area`, `price_vnd`, `std_date`, `std_category`... sẽ để **NULL**.*

---

## Giai đoạn 2: Standardization (Thực hiện sau)

Các trường này sẽ được tính toán từ các trường `src_` đã import ở trên:

| Cột data_clean_v1 | Nguồn (trong data_clean_v1) | Logic Tuần tự |
| :--- | :--- | :--- |
| **`std_area`** | `src_size` | Parse text -> Float. |
| **`price_vnd`** | `src_price` | Parse text "2,1 tỷ" -> 2100000000 (dùng `src_price`). |
| **`std_date`** | `update_time` | Convert Timestamp -> Date (YYYY-MM-DD). |
| **`std_category`** | `src_category_id` | Map ID (1050 -> 'Phòng trọ', 1010 -> 'Chung cư'). |
| **`std_trans_type`** | `src_type` | Map code ('s' -> 'ban', 'u' -> 'thue'). |
| **`cf_province/district/ward`** | `src_province/district/ward_id` | Map ID hành chính sang bảng config location. |
