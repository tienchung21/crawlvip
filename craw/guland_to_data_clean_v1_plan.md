# Plan convert `guland.vn` sang `data_clean_v1`

## 1. Mục tiêu

Tạo nhánh import mới cho `guland.vn` vào `data_clean_v1`, nhưng **không đi từ `scraped_details_flat` thuần** như Mogi cũ.

Nguồn đúng cho Guland cleanv1 nên là:

- `data_full`:
  - đã chuẩn hóa giá
  - đã chuẩn hóa diện tích
  - đã map khu vực Cafeland mới
  - đã map loại hình Cafeland (`cat_id`, `type_id`)
- kết hợp `scraped_details_flat`:
  - giữ raw text để đối chiếu
  - lấy `trade_type`
  - lấy `loaibds` gốc
  - lấy `url`
  - lấy `created_at`
  - lấy `khoanggia`, `dientich`

Kết luận:

- `data_full` là nguồn chuẩn cho các cột đã normalize
- `scraped_details_flat` chỉ là nguồn bổ sung raw + audit


## 2. Khác biệt với các importer cũ

### 2.1. Không nên đi lại full Step 1 -> Step 5 

Với Guland:

- `cf_province_id`, `cf_ward_id` đã có sẵn trong `data_full`
- `price_vnd` đã có sẵn trong `data_full.price`
- `std_area` đã có sẵn trong `data_full.area`
- `std_category` có thể lấy từ loaibds ở detail_flat
- `std_trans_type` có thể lấy trực tiếp từ `scraped_details_flat.trade_type`

Nếu vẫn import thô rồi chạy lại Step 1 -> Step 5 generic, sẽ bị thừa và còn có rủi ro sai lệch:

- Step 1 generic hiện không viết cho Guland
- Step 5 generic hiện **không cover `type_id = 14`** (`Kho, nhà xưởng`)

Khuyến nghị:

- import Guland vào `data_clean_v1` với dữ liệu đã chuẩn hóa sẵn
- set luôn:
  - `price_vnd`
  - `std_area`
  - `price_m2`
  - `std_category`
  - `std_trans_type`
  - `median_group`
- set `process_status = 0`
- đây mới chỉ là log convert ban đầu, chưa đi tới Step 5


## 3. Bộ dữ liệu nên nhập

### 3.1. Join nguồn

Join chuẩn:

- `data_full df`
- `scraped_details_flat sdf`

Điều kiện:

```sql
df.source = 'guland.vn'
AND sdf.domain = 'guland.vn'
AND CAST(df.source_post_id AS UNSIGNED) = sdf.matin
```

Ghi chú:

- `source_post_id` của Guland đang khớp với `matin`
- đây là join ổn định nhất hiện tại


### 3.2. Loại hình nên nhập theo rule median mới

Với Guland, rule đúng hiện tại là:

- tất cả `trade_type = 'u'` -> `median_group = 4`
- `Nhà riêng` bán -> `median_group = 1`
- `Đất` bán -> `median_group = 3`
- `Căn hộ chung cư` bán -> `median_group = 2`
- `Kho, nhà xưởng` bán -> `median_group = 3`
- `Văn phòng` -> bỏ
- `Nhà trọ` -> bỏ
- `Phòng trọ` -> bỏ
- `Khách sạn` -> bỏ
- `Mặt bằng kinh doanh` -> bỏ

Quy đổi theo `data_full.property_type` / `type_id` đang có:

```text
Import:
- Bán đất thổ cư              type_id=11 -> median_group 3
- Cho thuê đất               type_id=57 -> median_group 4
- Bán nhà riêng              type_id=2  -> median_group 1
- Cho thuê nhà riêng         type_id=2  -> median_group 4
- Bán căn hộ chung cư        type_id=5  -> median_group 2
- Cho thuê căn hộ chung cư   type_id=5  -> median_group 4
- Bán kho, nhà xưởng         type_id=14 -> median_group 3
- Cho thuê nhà kho - Xưởng   type_id=14 -> median_group 4

Bỏ:
- Cho thuê văn phòng
- Bán nhà hàng - Khách sạn
- Cho thuê nhà hàng - Khách sạn
- Bán căn hộ Mini, Dịch vụ
- Cho thuê phòng trọ
```

Snapshot hiện tại trong `data_full` Guland:

```text
Tổng `data_full` Guland                 : 44,255
Import theo plan                       : 43,104
  - Bán đất thổ cư                     : 24,806
  - Bán nhà riêng                      :  8,930
  - Bán căn hộ chung cư                :  5,021
  - Bán kho, nhà xưởng                 :    685
  - Cho thuê đất                       :    603
  - Cho thuê nhà riêng                 :  2,025
  - Cho thuê căn hộ chung cư           :    828
  - Cho thuê nhà kho - Xưởng           :    708
Bỏ theo plan                           :  1,151
```


## 4. Mapping cột đề xuất

### 4.1. Khóa chính / nhận diện

| `data_clean_v1` | Nguồn | Rule |
|---|---|---|
| `ad_id` | `df.source_post_id` | Dùng prefix để tránh đụng domain khác: `guland_{source_post_id}` |
| `domain` | hằng số | `'guland.vn'` |
| `url` | `sdf.url` | Lấy link detail gốc |


### 4.2. Khu vực

| `data_clean_v1` | Nguồn | Rule |
|---|---|---|
| `cf_province_id` | `df.province_id` | Lấy thẳng từ `data_full` |
| `cf_district_id` | `df.district_id` | Lấy thẳng, nếu NULL thì để NULL |
| `cf_ward_id` | `df.ward_id` | Lấy thẳng từ `data_full` |
| `cf_street_id` | `df.street_id` | Lấy thẳng, đa số có thể NULL |
| `project_id` | `df.project_id` | Lấy thẳng từ `data_full` |

Khuyến nghị:

- không map lại Step 1
- không dùng `src_province_id/src_district_id/src_ward_id` cho logic chính

Với Guland, 3 cột `src_* location` có thể:

- để `NULL`
- hoặc copy lại từ `cf_*` để tiện đối chiếu

Khuyến nghị sạch hơn:

- `src_province_id = NULL`
- `src_district_id = NULL`
- `src_ward_id = NULL`

Lý do:

- đây không còn là source ID gốc của domain
- nếu copy lại `cf_*` vào `src_*` sẽ dễ gây hiểu nhầm


### 4.3. Giá / diện tích / loại hình raw

| `data_clean_v1` | Nguồn | Rule |
|---|---|---|
| `src_size` | `sdf.dientich` | Giữ raw text để audit |
| `unit` | `df.unit` | Thường là `md` |
| `src_price` | `sdf.khoanggia` | Giữ raw text để audit |
| `src_category_id` | `sdf.loaibds` | Giữ raw loại hình Guland |
| `src_type` | `sdf.trade_type` | `s` / `u` |

Ghi chú:

- Guland cleanv1 nên giữ raw loại hình ở `src_category_id`
- không nên ghi `type_id` vào `src_category_id`
- vì `src_category_id` có vai trò audit nguồn gốc


### 4.4. Dữ liệu chuẩn hóa

| `data_clean_v1` | Nguồn | Rule |
|---|---|---|
| `price_vnd` | `df.price` | Lấy thẳng |
| `std_area` | `df.area` | Lấy thẳng |
| `price_m2` | `df.price / df.area` | Chỉ khi `area > 0` |
| `std_category` | `df.type_id` | Lưu dạng string |
| `std_trans_type` | `sdf.trade_type` | `s` / `u` |
| `price_land` | tính mới | Giá sau tách đất nếu thuộc nhóm có thể tách |

Rule cụ thể:

```text
price_vnd      = df.price
std_area       = df.area
price_m2       = CASE WHEN df.area > 0 THEN df.price / df.area ELSE NULL END
std_category   = CAST(df.type_id AS CHAR)
std_trans_type = sdf.trade_type
```

Lý do:

- `data_full.type_id` đã là loại hình Cafeland chuẩn
- `trade_type` từ detail vẫn là nguồn đúng nhất cho `s/u`


## 5. Mapping thời gian

### 5.1. Cột dùng

| `data_clean_v1` | Nguồn | Rule |
|---|---|---|
| `orig_list_time` | `df.posted_at` | Convert thành `YYYYMMDD` số nguyên |
| `update_time` | `sdf.created_at` | Convert thành Unix timestamp giây |
| `transfer_time` | now/import time | để Step 6 hoặc set khi import |

Rule đề xuất:

```text
orig_list_time = CAST(DATE_FORMAT(df.posted_at, '%Y%m%d') AS UNSIGNED)
update_time    = UNIX_TIMESTAMP(COALESCE(sdf.created_at, df.time_converted_at))
```

Ghi chú:

- `posted_at` của `data_full` đã là ngày đăng đã parse chuẩn
- đây là nguồn đúng nhất để sinh `std_date`
- `update_time` chỉ nên dùng như thời điểm record vào hệ thống/crawl xong


## 6. `median_group` cho Guland

### 6.1. Rule đúng ở bước convert ban đầu

Ở bước convert ban đầu map như sau:

- tất cả `trade_type = 'u'` -> `median_group = 4`
- `Nhà riêng` bán -> `median_group = 1`
- `Căn hộ chung cư` bán -> `median_group = 2`
- `Đất` bán -> `median_group = 3`
- `Kho, nhà xưởng` bán -> `median_group = 3`

Các loại hình khác:

- không gán `median_group`
- không cho đi tiếp Step 5


### 6.2. Rule `median_group` chính cho Guland

```text
trade_type = 'u'       -> median_group 4
Nhà riêng              -> median_group 1
Căn hộ chung cư        -> median_group 2
Đất                    -> median_group 3
Kho, nhà xưởng         -> median_group 3
```

Các loại khác:

- vẫn có thể import raw nếu muốn audit
- nhưng nếu không match được group hợp lệ thì không được finalize Step 5


### 6.3. Kết luận cho `median_group`

- chỉ dùng `median_group` với giá trị:
  - `1`
  - `2`
  - `3`
  - `4`
- loại hình nào không match các rule trên thì dừng ở log convert, không cho đi tiếp Step 5


## 7. `price_land`

### 7.1. Cần thêm cột mới

Thêm vào `data_clean_v1`:

- `price_land BIGINT NULL`

Ý nghĩa:

- lưu giá sau tách đất ở bước xử lý riêng sau này
- không tính ở bước convert ban đầu

### 7.2. Rule gán ban đầu

Ở phase import:

- luôn để `price_land = NULL`
- chưa đánh dấu là đã tách giá đất

Lý do:

- logic tách giá đất chưa chốt
- không nên nhét vào importer gốc

### 7.3. Step 7 tách giá đất

Step 7 là bước riêng sau Step 5, dùng để tính `price_land`.

Nhóm được xử lý:

- Nhóm Đất:
  - `Đất` -> trừ `0%`
- Nhóm Có nhà:
  - `Nhà riêng` -> trừ `15%`
  - `Kho, nhà xưởng` -> trừ `15%`

Nhóm bỏ, không xử lý:

- `Căn hộ chung cư`
- `Văn phòng`
- `Khách sạn`
- `Mặt bằng kinh doanh`
- `Nhà trọ`
- `Phòng trọ`

Công thức:

```text
price_land = price_vnd * (1 - discount_pct)
```

Trong đó:

```text
Đất              -> price_land = price_vnd
Nhà riêng        -> price_land = price_vnd * 0.85
Kho, nhà xưởng   -> price_land = price_vnd * 0.85
```

### 7.4. Đánh status xử lý tách giá đất

Cần có cờ trạng thái riêng cho Step 7, ví dụ:

- `land_price_status`

Rule:

- record đã được Step 7 xử lý thành công:
  - đánh status đã xử lý
- record thuộc nhóm bỏ:
  - không cần đánh status

Khuyến nghị giá trị:

```text
NULL                 -> chưa xử lý
DONE                 -> đã tính xong `price_land`
```


## 8. `process_status` nên bắt đầu từ đâu

### Khuyến nghị

Import thẳng ở:

- `process_status = 0`
- `last_script = 'convert_guland_to_data_clean_v1_import.py'`

và set sẵn:

- `cf_*`
- `price_vnd`
- `std_area`
- `price_m2`
- `std_category`
- `std_trans_type`
- `median_group`
- `price_land` = NULL

Sau đó các step sau phải chạy riêng, có chủ đích:

- Step 5: gom `median_group`
- Step 7: tính `price_land` cho các loại được phép tách


### Lý do

Với Guland:

- Step 1 đã được giải quyết ở `data_full`
- Step 2 đã được giải quyết ở `data_full`
- Step 3 đã được giải quyết ở `data_full`
- Step 4 đã được giải quyết ở `data_full + scraped_details_flat`
- nhưng bản ghi import ban đầu vẫn chỉ nên là log convert
- việc gom group và tách giá đất là step sau

Vì vậy import ở `process_status = 0` mới đúng với flow bạn chốt.


## 9. Rule lọc trước khi import

Chỉ lấy các row thỏa:

- `df.source = 'guland.vn'`
- join được `sdf.domain = 'guland.vn'`
- `df.province_id IS NOT NULL`
- `df.ward_id IS NOT NULL`
- `df.price > 0`
- `df.area > 0`
- `df.type_id IN (2, 5, 11, 14, 57)` hoặc loại tương ứng được giữ
- `sdf.trade_type IN ('s', 'u')`

Khuyến nghị strong filter để bỏ rác:

- `df.images_status` không liên quan, không dùng để import cleanv1
- `df.uploaded_at` không liên quan
- `df.posted_at IS NOT NULL`


## 10. Chống trùng và incremental

### 9.1. Khóa chống trùng

Khóa đề xuất:

- `data_clean_v1.ad_id = 'guland_' + source_post_id`

Như vậy:

- không đụng `mogi`
- không đụng `nhatot`
- không đụng `nhadat`


### 9.2. Incremental

Có 2 cách:

#### Cách A - Dùng `data_full.id`

- mỗi vòng lấy `data_full.id > last_seen_id`
- nhanh và đơn giản

#### Cách B - Dùng cờ ở `data_full`

Thêm cột:

- `cleanv1_converted`
- `cleanv1_converted_at`

Khuyến nghị:

- nếu làm script sản xuất lâu dài, nên thêm cờ riêng ở `data_full`
- không nên dựa hoàn toàn vào `last_id`


## 11. Lệnh bước sau khi import

Sau khi import xong batch Guland vào `data_clean_v1`:

```bash
python craw/auto/nhadat_step6_normalize_date.py --domain guland.vn
```

Ghi chú:

- Step 5 sẽ chạy sau import để gom `median_group`
- Step 6 có thể dùng để normalize date
- Step 7 là bước tính `price_land`


## 12. Kế hoạch implement đề xuất

### Phase 1

Tạo script mới:

- `craw/auto/convert_guland_to_data_clean_v1.py`

Nhiệm vụ:

- đọc `data_full` + `scraped_details_flat`
- lọc đúng các loại hình được giữ
- insert/upsert vào `data_clean_v1`
- set `process_status = 0`
- set `median_group`
- để `price_land = NULL`


### Phase 2

Chạy Step 5:

- gom `median_group` theo rule:
  - `1` Nhà riêng
  - `2` Căn hộ chung cư
  - `3` Đất, Kho nhà xưởng
  - `4` toàn bộ tin thuê

### Phase 3

Chạy Step 7:

- tính `price_land`
- đánh `land_price_status = DONE` cho các record đã xử lý
- nhóm bỏ thì không đánh status

### Phase 4

Kiểm tra:

- số row import
- số row `process_status = 0`
- số row theo `median_group`
- số row đã có `price_land`
- số row `land_price_status = DONE`
- phân bố theo `std_category`


## 13. Kết luận chốt

Plan đúng cho Guland cleanv1 là:

- **không** import raw từ `scraped_details_flat` thuần
- **lấy `data_full` làm nguồn chính**
- **join thêm `scraped_details_flat` để giữ raw/audit**
- **không chạy lại Step 1 -> Step 5 generic**
- **set sẵn dữ liệu chuẩn hóa nhưng chỉ ghi log convert ban đầu**
- **median_group dùng 1/2/3/4**
- **loại hình nào không match thì không cho qua Step 5**
- **`process_status = 0` ở bước import**
- **Step 7 mới tính `price_land`**

Đây là cách ít rủi ro nhất vì nó tận dụng đúng phần việc mà `data_full` đã làm tốt:

- giá
- diện tích
- loại hình
- tỉnh/xã
- dự án
