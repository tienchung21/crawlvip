# Quy trình Chuẩn hóa Dữ liệu (ETL Pipeline)

## 1. Tổng quan các Bảng Dữ liệu

| STT | Tên bảng | Mô tả | Ghi chú |
| :--- | :--- | :--- | :--- |
| **1** | **detail flat** | Bảng dữ liệu thô lấy về ban đầu (Raw Data). | Dùng để lọc trường cần thiết trước khi chuyển qua `data_clean`. |
| **2** | **data_clean** | Bảng xử lý dữ liệu trung gian. Dữ liệu sẽ được chuẩn hóa qua 6 bước. | Mỗi step sẽ overwrite lại giá trị. Lưu vết script chạy cuối cùng (ví dụ `last_script`). |
| **3** | **data_clean_stats** | Bảng tính toán thống kê (Median). | Tính sẵn median theo khu vực + thời gian để API gọi sử dụng nhanh. |
| **4** | **data_full** | Bảng dữ liệu hoàn chỉnh. | Chứa đầy đủ các trường thông tin chuẩn, sẵn sàng để đăng lại lên site. |

## 2. Chi tiết Quy trình Xử lý `data_clean`

Dữ liệu từ `detail flat` sẽ được chuyển sang `data_clean` và đi qua 6 bước chuẩn hóa (Step 1 -> Step 6). Mỗi bước sẽ tương ứng với một script xử lý riêng biệt.

### Nguyên tắc vận hành:
- Mỗi step sau khi hoàn thành sẽ **overwrite** (ghi đè) lại giá trị vào record.
- Cập nhật trạng thái step (ví dụ: xong step 3 thì lưu `3` để truy vết).
- Lưu tên script xử lý gần nhất vào trường (ví dụ: `last_script = 'area_parse_v2.py'`).

### Các bước thực hiện (6 Steps):

#### **Step 1: Chuẩn hóa Địa chính (Region/Ward)**
*   **Mô tả**: Convert bộ ID khu vực cũ sang hệ ID Tỉnh + Xã mới của Cafeland.
*   **Input**: 4 cột khu vực cũ.
*   **Output**: Lưu vào 2 cột mới: `id_xa_new` và `id_tinh_new`.

#### **Step 2: Tách Giá (Price)**
*   **Mô tả**: Tách giá trị tiền từ chuỗi text, xử lý đơn vị (tỷ, triệu) và chuyển về số nguyên VNĐ.
*   **Input**: Cột `src_price` (ví dụ: "2 tỷ", "800 triệu").
*   **Output**: Lưu vào cột `price_vnd` (ví dụ: 2000000000, 800000000).

#### **Step 3: Tách Diện tích & Tính Đơn giá (Size + Price/m2)**
*   **Mô tả**: Tách số liệu diện tích (m2) từ chuỗi text, sau đó tính đơn giá trên m2.
*   **Input**: Cột `src_size` (ví dụ: "50 m2") và `price_vnd` (từ Step 2).
*   **Output**: 
    *   Cột `std_area` (diện tích chuẩn, ví dụ: 50.0).
    *   Cột `price_m2` (đơn giá/m2 = price_vnd / std_area).

#### **Step 4: Chuẩn hóa Loại hình & Loại giao dịch (Category/Type)**
*   **Mô tả**: Map lại các loại hình (đất, nhà ở, căn hộ...) và loại tin (Bán `s`, Thuê `u`) về chuẩn chung.
*   **Input**: 2 cột gốc (loại hình, loại tin).
*   **Output**: Lưu vào 2 cột loại hình mới quy định chung.

#### **Step 5: Gom nhóm Median**
*   **Mô tả**: Nhóm các tin đăng có tính chất tương đồng (theo loại hình, vị trí...) để phục vụ tính toán median sau này.
*   **Output**: Lưu vào 1 cột mới (nhóm median).

#### **Step 6: Chuẩn hóa Thời gian (Date)**
*   **Mô tả**: Convert thông tin ngày/tháng/năm đăng tin về định dạng chuẩn.
*   **Input**: Sử dụng 3 cột fallback (để xử lý trường hợp thiếu ngày).
*   **Output**: Lưu vào 1 cột thời gian mới.



### lưu y , khi chay step phai kiem tra tin do da hoan thanh status cua step truoc hay chua , neu chua thi bo qua va chay tin tiep theo , chay xong bao log next bao nhieu
- luôn phải kèm domain trong điều kiện để ko thay đổi những domain đã tính toán trước đó rồi  
---
*File created automatically based on user requirements.*
