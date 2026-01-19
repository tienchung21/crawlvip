# Công Thức & Quy Tắc Matching Địa Điểm

Tài liệu này mô tả chi tiết các thuật toán và quy tắc đã được sử dụng để đồng bộ dữ liệu địa điểm giữa **Cafeland** (nguồn) và **Cenhomes** (đích).

## 1. Chuẩn Hóa Tên (Normalization)

Trước khi so sánh, tất cả tên địa điểm đều được xử lý qua các bước sau:
1. **Chuyển về chữ thường** (Lowercase).
2. **Loại bỏ dấu tiếng Việt** (Accent removal): `Hồ Chí Minh` -> `ho chi minh`.
3. **Loại bỏ từ khóa hành chính** (Prefix removal):
   - Tỉnh/TP: `tinh`, `thanh pho`, `tp.`, `tp`
   - Quận/Huyện: `quan`, `huyen`, `thi xa`, `tx.`, `tx`, `phuong`, `xa`, `thitran`, `thi tran`
4. **Xử lý số**:
   - Loại bỏ số 0 ở đầu: `phuong 07` -> `7`, `phuong 7` -> `7` (để khớp giữa `07` và `7`).

## 2. Cấp Tỉnh / Thành Phố (Provinces)

*   **Quy tắc**: So sánh chính xác (Exact Match) trên tên đã chuẩn hóa.
*   **Kết quả**: 63/63 Tỉnh thành khớp 100%.

## 3. Cấp Quận / Huyện (Districts)

Quy trình match diễn ra theo 2 giai đoạn:

### Giai đoạn 1: Match cơ bản
*   **Phạm vi**: Chỉ tìm trong danh sách Quận/Huyện thuộc Tỉnh đã match tương ứng.
*   **Quy tắc**:
    1. **Exact Match**: Tên chuẩn hóa giống hệt nhau.
    2. **Fuzzy Match (Chứa)**: Nếu tên này chứa tên kia (độ dài > 3 ký tự).

### Giai đoạn 2: Fix lỗi nâng cao (Advanced Fix)
Dành cho danh sách chưa match (Unmatched), sử dụng các rule đặc biệt:
1. **Thay thế âm vần đặc biệt**:
   - `la` <-> `ia` (Vd: `Ia Grai` vs `La Grai`)
   - `xi` <-> `si` (Vd: `Si Ma Cai` vs `Xi Ma Cai`)
   - `qui` <-> `quy` (Vd: `Qui Nhon` vs `Quy Nhon`)
   - `dak` <-> `dac` (Vd: `Dak Lak` vs `Dac Lac`)
2. **Tìm kiếm chéo Tỉnh (Cross-Province)**:
   - Quét toàn bộ danh sách Huyện của cả nước để tìm match (xử lý trường hợp Tỉnh bị tách/nhập hoặc dữ liệu nguồn ghi sai tỉnh).
   - Ví dụ: Huyện cũ của Tỉnh Lai Châu nay thuộc Điện Biên.

## 4. Cấp Phường / Xã (Wards)

Đây là cấp phức tạp nhất, quy trình xử lý như sau:

### Giai đoạn 1: Match theo Quận (Strict Match)
*   **Phạm vi**: Chỉ tìm Phường/Xã thuộc đúng Quận/Huyện cha đã match.
*   **Quy tắc**:
    1. **Exact Match**: Giống hệt nhau.
    2. **High Similarity**: Dùng thuật toán `SequenceMatcher`, độ tương đồng > **0.85**.

### Giai đoạn 2: Advanced Fix (Xử lý 1,000+ xã lỗi)
Dành cho các xã chưa match, áp dụng "Công thức" mở rộng:

1. **Tìm kiếm chéo Quận trong cùng Tỉnh (Cross-District)**:
   - **Mục tiêu**: Xử lý các trường hợp Quận bị thay đổi địa giới hành chính (điển hình: Quận 2, Quận 9, Quận Thủ Đức -> TP Thủ Đức).
   - **Logic**: Tìm tên xã trong **toàn bộ các xã của Tỉnh đó**.
   - **Điểm số**:
     - Nếu trùng tên & cùng quận: 1.0 điểm.
     - Nếu trùng tên nhưng khác quận: 0.95 điểm (Chấp nhận match).

2. **Tìm kiếm bao hàm (Containment Search)**:
   - **Mục tiêu**: Xử lý dữ liệu "rác" chứa cả địa chỉ đường phố.
   - **Ví dụ**: Cafeland ghi `683 Âu Cơ, Tân Thành, Tân Phú`. Cenhomes ghi `Tân Thành`.
   - **Logic**: Nếu `Tên_Cenhomes` nằm trong `Tên_Cafeland` (và độ dài tên > 4 ký tự).
   - **Điểm số**: 0.9 điểm -> Chấp nhận match.

3. **Luật bù trừ (Normalization Rules)**:
   - Xử lý các tiền tố kẹt lại như "Phường", "Xã" viết tắt hoặc viết sai chính tả nhẹ thông qua thuật toán so sánh chuỗi mờ (Fuzzy distance).

## Tổng Kết Độ Chính Xác
- **Tỉnh**: 100%
- **Huyện**: ~96% (đã fix hầu hết các lỗi chính tả/vùng miền).
- **Xã**: 96% (~4% còn lại là dữ liệu sai/rác từ nguồn không thể map được).
