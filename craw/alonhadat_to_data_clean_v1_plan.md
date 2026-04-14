# Plan convert `alonhadat.com.vn` sang `data_clean_v1`

## 1. Mục tiêu

Tạo nhánh import mới cho `alonhadat.com.vn` vào `data_clean_v1`.

Nguồn dùng:

- `data_full`
  - đã có giá chuẩn
  - đã có diện tích chuẩn
  - đã map khu vực theo `transaction_city_merge`
- `scraped_details_flat`
  - giữ raw `loaibds`
  - giữ `trade_type`
  - giữ `url`
  - giữ raw `khoanggia`, `dientich`

Kết luận:

- `data_full` là nguồn chính cho số liệu chuẩn
- `scraped_details_flat` là nguồn raw để audit và gom nhóm


## 2. Join nguồn

Join chuẩn:

```sql
FROM data_full df
JOIN scraped_details_flat sdf
  ON CAST(df.source_post_id AS UNSIGNED) = sdf.matin
 AND sdf.domain = 'alonhadat.com.vn'
WHERE df.source = 'alonhadat.com.vn'
```


## 3. Mapping cột khi import

### 3.1. Khóa và nguồn

- `ad_id = 'alonhadat_' + source_post_id`
- `domain = 'alonhadat.com.vn'`
- `url = sdf.url`

### 3.2. Khu vực

- `cf_province_id = df.province_id`
- `cf_district_id = df.district_id`
- `cf_ward_id = df.ward_id`
- `cf_street_id = df.street_id`
- `project_id = df.project_id`

### 3.3. Raw

- `src_size = sdf.dientich`
- `src_price = sdf.khoanggia`
- `src_category_id = sdf.loaibds`
- `src_type = sdf.trade_type`

### 3.4. Chuẩn hóa

- `std_area = df.area`
- `price_vnd = df.price`
- `price_m2 = df.price / df.area`
- `std_category = sdf.loaibds`
- `std_trans_type = sdf.trade_type`
- `std_date = DATE(df.posted_at)`
- `orig_list_time = YYYYMMDD từ df.posted_at`
- `update_time = UNIX_TIMESTAMP(sdf.created_at)`


## 4. `process_status` khi import

Import ban đầu:

- `process_status = 0`

Ý nghĩa:

- đây mới là log convert ban đầu
- chưa coi là đã qua các step phía sau


## 5. Rule `median_group`

### 5.1. Group bán

```text
1  Nhà mặt tiền
1  Nhà trong hẻm
1  Biệt thự, nhà liền kề

2  Căn hộ chung cư
2  Shop, kiot, quán

3  Trang trại
3  Mặt bằng
3  Đất thổ cư, đất ở
3  Đất nền, liền kề, đất dự án
3  Đất nông, lâm nghiệp
```

### 5.2. Group thuê

```text
4  tất cả trade_type = 'u'
```

### 5.3. Loại bỏ

```text
bo  Phòng trọ, nhà trọ
bo  Văn phòng
bo  Kho, xưởng
bo  Nhà hàng, khách sạn
```

### 5.4. Kết luận cho Step 5

- Step 5 chỉ làm nhiệm vụ gom `median_group`
- loại hình nào không match các rule trên:
  - không gán `median_group`
  - không cho qua Step 5


## 6. Step 7 tách `price_land`

### 6.1. Cột cần có

- `price_land BIGINT NULL`
- `land_price_status VARCHAR(20) NULL`

### 6.2. Rule tách giá đất

Nhóm Đất, trừ `0%`:

```text
Đất thổ cư, đất ở
Đất nền, liền kề, đất dự án
Đất nông, lâm nghiệp
```

Nhóm Có nhà, trừ `15%`:

```text
Nhà mặt tiền
Nhà trong hẻm
Biệt thự, nhà liền kề
Kho, xưởng
Trang trại
```

Nhóm bỏ, không tách:

```text
Căn hộ chung cư
Văn phòng
Nhà hàng, khách sạn
Shop, kiot, quán
Mặt bằng
Phòng trọ, nhà trọ
```

### 6.3. Công thức

```text
price_land = price_vnd * (1 - discount_pct)
```

Cụ thể:

```text
Đất thổ cư, đất ở                 -> price_land = price_vnd
Đất nền, liền kề, đất dự án       -> price_land = price_vnd
Đất nông, lâm nghiệp              -> price_land = price_vnd

Nhà mặt tiền                      -> price_land = price_vnd * 0.85
Nhà trong hẻm                     -> price_land = price_vnd * 0.85
Biệt thự, nhà liền kề             -> price_land = price_vnd * 0.85
Kho, xưởng                        -> price_land = price_vnd * 0.85
Trang trại                        -> price_land = price_vnd * 0.85
```

### 6.4. Đánh status

Record đã xử lý xong Step 7:

- `land_price_status = 'DONE'`
- `process_status = 7`

Record thuộc nhóm bỏ:

- `price_land = NULL`
- `land_price_status = NULL`
- không đánh `process_status = 7`


## 7. Rule lọc khi import

Chỉ lấy row thỏa:

- `df.source = 'alonhadat.com.vn'`
- `sdf.domain = 'alonhadat.com.vn'`
- `df.province_id IS NOT NULL`
- `df.ward_id IS NOT NULL`
- `df.price > 0`
- `df.area > 0`
- `sdf.trade_type IN ('s', 'u')`


## 8. Cờ nguồn

Nguồn `scraped_details_flat` hiện có:

- `cleanv1_converted`
- `cleanv1_converted_at`

Rule:

- insert/upsert thành công vào `data_clean_v1`
  - `cleanv1_converted = 1`
  - `cleanv1_converted_at = NOW()`


## 9. Kế hoạch implement

### Phase 1

Tạo script:

- `craw/auto/convert_alonhadat_to_data_clean_v1.py`

Nhiệm vụ:

- đọc `data_full` + `scraped_details_flat`
- map cột chuẩn
- set `process_status = 0`
- set `median_group`
- đánh cờ nguồn `cleanv1_converted`

### Phase 2

Chạy Step 5:

- gom `median_group` theo rule đã chốt

### Phase 3

Chạy Step 7:

- tính `price_land`
- set `land_price_status = 'DONE'`
- set `process_status = 7` cho các row đã tách xong


## 10. Kết luận chốt

Plan Alonhadat cleanv1 hiện chốt là:

- đi từ `data_full` + `scraped_details_flat`
- `std_category = src_category_id`
- `std_date = DATE(posted_at)`
- thuê là `median_group = 4`
- nhóm median bán đúng theo bảng đã chốt
- Step 7 chỉ tách `price_land` cho:
  - nhóm đất `0%`
  - nhóm có nhà `15%`
- nhóm bỏ không đánh status tách đất
