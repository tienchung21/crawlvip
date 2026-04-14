# Phân tích tỷ lệ tách giá đất khỏi tổng giá tin đăng

Ngày phân tích: 2026-03-14

## 1. Mục tiêu

Ước tính `giá đất` từ `tổng giá tin đăng` cho các loại hình có nhà/tài sản trên đất, theo domain.

Công thức để áp dụng sau khi chốt tỷ lệ khấu trừ `D%`:

```text
land_value_total = total_price * (1 - D%)
land_unit_price  = land_value_total / area
```

## 2. Dữ liệu và phương pháp

Nguồn dữ liệu dùng để tính:
- `data_full` cho `alonhadat.com.vn`, `guland.vn`
- `data_clean_v1` cho `mogi`, `batdongsan.com.vn`, `nhatot`, `nhadat`

Phạm vi:
- Chỉ lấy tin `bán` (`cat_id in (1,2)` với `data_full`, `std_trans_type='s'` với `data_clean_v1`)
- Giá > 0, diện tích > 0
- Có `province_id + ward_id` chuẩn
- Lấy 365 ngày gần đây
- Loại bỏ outlier giá/m2 quá phi lý: `< 0.2 tr/m2` hoặc `> 2 tỷ/m2`

Baseline đất tham chiếu:
- Dùng `đất thuần` cùng domain
- Ưu tiên median giá/m2 của nhóm đất cùng `ward_id` nếu ward có ít nhất `5` mẫu đất
- Nếu ward không đủ mẫu, fallback về median giá/m2 của nhóm đất cùng `province_id` nếu tỉnh có ít nhất `20` mẫu đất
- Các nhóm đất thuần dùng làm baseline:
  - `alonhadat`: `Đất thổ cư, đất ở`, `Đất nền, liền kề, đất dự án`
  - `guland`: `Đất`
  - `nhatot`: `1040 Đất`
  - `mogi`: `Đất thổ cư`, `Đất nền dự án`
  - `batdongsan`: `Đất`, `Đất nền dự án`
  - `nhadat`: `Bán đất thổ cư`, `Bán đất nền dự án`

Cách suy ra discount:
- Với từng tin có tài sản trên đất:
  - `discount_i = 1 - baseline_land_unit_price / listing_unit_price`
- Lấy median của `discount_i` theo domain + loại hình
- Làm tròn về mức 5% để dễ vận hành

## 3. Mẫu dữ liệu

Số mẫu dùng để phân tích 365 ngày gần đây:
- `data_full`: `58,507` tin bán có vùng chuẩn và giá/diện tích hợp lệ
- `data_clean_v1`: `364,525` tin bán có vùng chuẩn và giá/diện tích hợp lệ
- Tổng hợp lệ để tính: xấp xỉ `423k` tin

## 4. Kết luận nhanh

1. Công thức chatbot cũ dùng sai hướng ở nhiều domain tỉnh/lẻ.
- Đặc biệt là `guland`, `alonhadat`, `nhadat`, `mogi`: các mức `25%` cho nhà là quá thấp.
- `batdongsan` và `nhatot` thì gần hơn với dữ liệu thực tế.

2. Nhóm `căn hộ/chung cư/officetel/penthouse/...` không nên ép tách giá đất bằng 1 tỷ lệ cố định.
- Lý do: đơn giá đang phản ánh giá trị sàn xây dựng/floor area, không phải giá trị lô đất.
- Những nhóm này nên `BỎ`.

3. Nhóm `khách sạn`, `mặt bằng`, `văn phòng`, `mini/dịch vụ` có độ biến động lớn hơn nhà đất ở thông thường.
- Nếu giữ lại vận hành thì phải ghi rõ `low confidence`.

4. Baseline `cùng domain` cho kết quả hợp lý hơn baseline trộn nhiều domain.
- Lý do: mỗi domain lệch phân khúc khách hàng và khu vực khác nhau.
- Vì vậy bảng đề xuất bên dưới dùng `same-domain baseline` làm kết luận chính.

## 5. Bảng đề xuất cuối cùng

### 5.1 Alonhadat

```text
Loại hình raw Alonhadat          | Mẫu match       | Median   | Đề xuất       | Đánh giá
---------------------------------+-----------------+----------+---------------+----------------------------------------------
Đất thổ cư, đất ở                | land baseline   | 0%       | 0%            | giữ nguyên
Đất nền, liền kề, đất dự án      | land baseline   | 0%       | 0%            | giữ nguyên
Đất nông, lâm nghiệp             | không cần tách  | 0%       | 0%            | giữ nguyên
Nhà mặt tiền                     | 400             | 52.8%    | 50%           | chatbot cũ 25% là quá thấp
Nhà trong hẻm                    | 171             | 32.6%    | 35%           | hợp lý hơn 25%
Biệt thự, nhà liền kề            | 127             | 39.0%    | 40%           | khá ổn
Kho, xưởng                       | 4               | 20.1%    | 20% tạm thời  | mẫu quá ít, low confidence
Nhà hàng, khách sạn              | 39              | 63.9%    | 60%           | biến động cao, nhưng rõ ràng > 45%
Phòng trọ, nhà trọ               | 17              | 13.3%    | BỎ            | mẫu ít, nhóm bị trộn và độ ổn định kém
Văn phòng                        | 2               | 44.0%    | BỎ            | quá ít mẫu
Shop, kiot, quán                 | 0               | -        | BỎ            | không đủ mẫu
Mặt bằng                         | 1               | 64.8%    | BỎ            | không đủ mẫu
Căn hộ chung cư                  | 36              | 28.9%    | BỎ            | không nên tách giá đất cố định
Căn hộ Mini, Dịch vụ             | 35 all-history  | 20.7%    | BỎ            | nhóm trộn, không ổn định
```

### 5.2 Guland

```text
Loại hình raw Guland             | Mẫu match      | Median  | Đề xuất        | Đánh giá
---------------------------------+----------------+---------+----------------+---------------------------------------------
Đất                              | land baseline  | 0%      | 0%             | giữ nguyên
Nhà riêng                        | 7,090          | 56.6%   | 55%            | chatbot cũ 25% là quá thấp rất nhiều
Kho, nhà xưởng                   | 261            | 46.6%   | 45%            | chatbot cũ 15% là quá thấp
Khách sạn                        | 68             | 26.5%   | 30% / manual   | q1 ~ 0%, q3 ~ 63%, biến động lớn
Nhà trọ                          | 40             | 0.6%    | BỎ             | 50% mẫu âm/không ổn định
Văn phòng                        | 104            | 42.1%   | 40%            | dữ liệu thực tế đang nghiêng về tài sản gắn đất
Phòng trọ                        | 0              | -       | BỎ             | không có mẫu sale tách riêng
Mặt bằng kinh doanh              | 0              | -       | BỎ             | không có mẫu sale tách riêng
Căn hộ chung cư                  | 4,170          | 13.9%   | BỎ             | không phải lô đất
Căn hộ Mini, Dịch vụ             | 143            | 31.1%   | BỎ             | bucket bị trộn, không nên ép 1 tỷ lệ
```

### 5.3 Nhatot

```text
Loại hình Nhatot                 | Mẫu match      | Median  | Đề xuất       | Đánh giá
---------------------------------+----------------+---------+---------------+-----------------------------------------------
1040 Đất                         | land baseline  | 0%      | 0%            | giữ nguyên
1020 Nhà ở                       | 101,325        | 29.3%   | 30%           | kết quả rất gần dữ liệu, tốt hơn mức 25% cũ
1030 Văn phòng/Mặt bằng          | 1,239          | 37.7%   | 40% tạm thời  | nên tách tiếp theo commercial_type nếu muốn chuẩn hơn
1010 Căn hộ/Chung cư             | 10,126         | 4.6%    | BỎ            | nhóm căn hộ không nên tách đất bằng % cố định
```

Ghi chú thêm:
- `1020 Nhà ở` là nhóm lớn và đang ổn nhất.
- `1030` đang bị trộn giữa `mặt bằng` và `văn phòng`; vẫn có thể dùng `40%` nếu bắt buộc phải có 1 rule, nhưng độ tin cậy thấp hơn `1020`.

### 5.4 Mogi

```text
Loại hình Mogi                   | Mẫu match      | Median   | Đề xuất  | Đánh giá
---------------------------------+----------------+----------+----------+-----------------------------------------------
Đất thổ cư                       | land baseline  | 0%       | 0%       | giữ nguyên
Đất nền dự án                    | land baseline  | 0%       | 0%       | giữ nguyên
Đất nông nghiệp                  | không cần tách | 0%       | 0%       | giữ nguyên
Nhà hẻm ngõ                      | 8,781          | 44.3%    | 45%      | chatbot cũ 25% là quá thấp
Nhà mặt tiền phố                 | 5,535          | 49.1%    | 50%      | rõ ràng cao hơn 25%
Đường nội bộ                     | 1,102          | 41.4%    | 40%      | thấp hơn mặt tiền, cao hơn 25%
Nhà biệt thự, liền kề            | 924            | 48.5%    | 50%      | chatbot cũ 35% vẫn thấp
Nhà xưởng, kho bãi               | 0 sale match   | -        | BỎ       | không có mẫu sale hợp lệ trong 12 tháng
Văn phòng                        | 0 sale match   | -        | BỎ       | không có mẫu sale hợp lệ
Căn hộ chung cư                  | 2,517          | 14.8%    | BỎ       | không phải lô đất
Căn hộ dịch vụ                   | 137            | 40.3%    | BỎ       | nhóm khai thác dòng tiền, không nên dùng 1 % cố định
Căn hộ Officetel                 | 12             | -23.5%   | BỎ       | sai bản chất cho bài toán đất
Căn hộ Penthouse                 | 62             | 26.0%    | BỎ       | sai bản chất cho bài toán đất
Căn hộ tập thể, cư xá            | 17             | -14.0%   | BỎ       | sai bản chất cho bài toán đất
```

### 5.5 Batdongsan.com.vn

```text
Loại hình BDS                    | Mẫu match        | Median  | Đề xuất       | Đánh giá
---------------------------------+------------------+---------+---------------+----------------------------------------------
Đất                              | land baseline    | 0%      | 0%            | giữ nguyên
Đất nền dự án                    | land baseline    | 0%      | 0%            | giữ nguyên
Nhà riêng                        | 46,747           | 23.3%   | 25%           | chatbot cũ 25% là hợp lý
Nhà mặt phố                      | 16,762           | 41.5%   | 40%           | cao hơn 25% nhiều
Biệt thự liền kề                 | 8,587            | 43.3%   | 45%           | chatbot cũ 35% là thấp
Shophouse                        | 1,684 all-history| 43.3%   | BỎ / 45%      | nhóm kinh doanh, biến động theo vị trí
Kho, nhà xưởng                   | 77 all-history   | 8.1%    | BỎ            | mẫu yếu, negative share cao
Căn hộ chung cư                  | 14,343           | -0.7%   | BỎ            | không phải lô đất
Căn hộ chung cư mini             | 108              | 30.2%   | BỎ            | nhóm trộn, không nên ép
Condotel                         | 674              | 8.7%    | BỎ            | sai bản chất cho bài toán đất
```

### 5.6 Nhadat

```text
Loại hình Nhadat                 | Mẫu match        | Median  | Đề xuất  | Đánh giá
---------------------------------+------------------+---------+----------+----------------------------------------------
Bán đất thổ cư                   | land baseline    | 0%      | 0%       | giữ nguyên
Bán đất nền dự án                | land baseline    | 0%      | 0%       | giữ nguyên
Bán đất nông, lâm nghiệp         | không cần tách   | 0%      | 0%       | giữ nguyên
Nhà riêng                        | 5,987            | 49.5%   | 50%      | chatbot cũ 25% là quá thấp
Nhà phố                          | 890              | 48.6%   | 50%      | chatbot cũ 25% là quá thấp
Biệt thự                         | 599              | 47.4%   | 45%      | chatbot cũ 35% vẫn thấp
Nhà Kho - Xưởng                  | 178 all-history  | 21.2%   | 20%      | mức này hợp lý hơn 15%
Nhà hàng - Khách sạn             | 238 all-history  | 58.7%   | 60%      | chatbot cũ 45% là thấp
Văn phòng                        | 7 all-history    | 65.7%   | BỎ       | mẫu quá ít
Mặt bằng                         | 0                | -       | BỎ       | không có mẫu sale hợp lệ
Căn hộ chung cư                  | 2,306            | 25.3%   | BỎ       | không phải lô đất
Bán căn hộ Mini, Dịch vụ         | 116              | 50.7%   | BỎ       | nhóm trộn và khai thác dòng tiền
```

## 6. Điều chỉnh so với bảng chatbot cũ

Phần sai nhiều nhất của bảng cũ:
- `alonhadat`:
  - `Nhà mặt tiền 25%` -> dữ liệu cho thấy nên là khoảng `50%`
  - `Nhà trong hẻm 25%` -> dữ liệu nghiêng về `35%`
- `guland`:
  - `Nhà riêng 25%` -> dữ liệu là `55%`
  - `Kho/xưởng 15%` -> dữ liệu nghiêng về `45%`
- `mogi`:
  - `Nhà hẻm/mặt tiền/nội bộ 25%` -> dữ liệu là `40-50%`
  - `Biệt thự 35%` -> dữ liệu nghiêng về `50%`
- `nhadat`:
  - `Nhà riêng/Nhà phố 25%` -> dữ liệu là `50%`
  - `Khách sạn 45%` -> dữ liệu nghiêng về `60%`
- `batdongsan`:
  - `Nhà riêng 25%` thì ổn
  - `Nhà mặt phố 25%` là thấp, dữ liệu nghiêng về `40%`
  - `Biệt thự 35%` là thấp, dữ liệu nghiêng về `45%`
- `nhatot`:
  - `Nhà ở 25%` tạm được, nhưng median data là `~30%`

## 7. Bộ rule tôi khuyên dùng ngay

Nếu cần 1 bộ rule vận hành ngay, tôi khuyên dùng bộ này:

### Alonhadat
- `Đất thổ cư, đất ở` -> `0%`
- `Đất nền, liền kề, đất dự án` -> `0%`
- `Đất nông, lâm nghiệp` -> `0%`
- `Nhà mặt tiền` -> `50%`
- `Nhà trong hẻm` -> `35%`
- `Biệt thự, nhà liền kề` -> `40%`
- `Kho, xưởng` -> `20%` low confidence
- `Nhà hàng, khách sạn` -> `60%`
- còn lại -> `BỎ`

### Guland
- `Đất` -> `0%`
- `Nhà riêng` -> `55%`
- `Kho, nhà xưởng` -> `45%`
- `Khách sạn` -> `30%` low confidence / ưu tiên manual
- `Văn phòng` -> `40%`
- còn lại -> `BỎ`

### Nhatot
- `1040 Đất` -> `0%`
- `1020 Nhà ở` -> `30%`
- `1030 Văn phòng/Mặt bằng` -> `40%` low confidence
- còn lại -> `BỎ`

### Mogi
- `Đất thổ cư` -> `0%`
- `Đất nền dự án` -> `0%`
- `Đất nông nghiệp` -> `0%`
- `Nhà hẻm ngõ` -> `45%`
- `Nhà mặt tiền phố` -> `50%`
- `Đường nội bộ` -> `40%`
- `Nhà biệt thự, liền kề` -> `50%`
- còn lại -> `BỎ`

### Batdongsan.com.vn
- `Đất` -> `0%`
- `Đất nền dự án` -> `0%`
- `Nhà riêng` -> `25%`
- `Nhà mặt phố` -> `40%`
- `Biệt thự liền kề` -> `45%`
- còn lại -> `BỎ`

### Nhadat
- `Bán đất thổ cư` -> `0%`
- `Bán đất nền dự án` -> `0%`
- `Bán đất nông, lâm nghiệp` -> `0%`
- `Nhà riêng` -> `50%`
- `Nhà phố` -> `50%`
- `Biệt thự` -> `45%`
- `Nhà Kho - Xưởng` -> `20%`
- `Nhà hàng - Khách sạn` -> `60%`
- còn lại -> `BỎ`

## 8. Khuyến nghị tiếp theo

Nếu muốn chuẩn hơn nữa, nên làm tiếp 3 việc:
1. Tách `nhatot` theo `house_type` và `commercial_type`, không dùng chung `1020` và `1030`.
2. Tách `guland`/`alonhadat` theo raw `loaibds` trước khi map sang `property_type` Cafeland, đặc biệt nhóm `Mini/Dịch vụ`, `trọ`, `mặt bằng`, `văn phòng`.
3. Tạo bảng `land_value_factor_config` để lưu `domain + raw_type + discount_pct + confidence + sample_size` thay vì hardcode.
