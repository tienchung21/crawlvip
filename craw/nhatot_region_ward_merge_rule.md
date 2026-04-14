# Nhatot: Quy tắc lấy `province_id` / `ward_id` chuẩn để upload

Mục tiêu của file này là chốt **cách lấy id tỉnh/xã chuẩn nhất** cho dữ liệu Nhatot trước khi đẩy lên API upload Cafeland.

## 1. Nguồn raw gốc

Bảng nguồn raw của Nhatot là `ad_listing_detail`.

Các cột raw liên quan khu vực:

- `ad_listing_detail.region_v2`: id tỉnh gốc của Nhatot
- `ad_listing_detail.area_v2`: id quận/huyện gốc của Nhatot
- `ad_listing_detail.ward`: id phường/xã gốc của Nhatot

Các cột tên để hiển thị/đối chiếu:

- `region_name_v3`
- `area_name`
- `ward_name_v3`

Lưu ý: để map chuẩn cho upload, **ưu tiên join theo id gốc** (`region_v2`, `area_v2`, `ward`), không map theo tên.

## 2. Bảng merge chuẩn cần dùng

### 2.1 `location_detail`

Bảng trung gian ánh xạ id gốc domain sang id Cafeland cũ.

Các cột cần dùng:

- `region_id`: id tỉnh gốc
- `area_id`: id quận/huyện gốc
- `ward_id`: id xã/phường gốc
- `level`:
  - `1` = tỉnh
  - `2` = quận/huyện
  - `3` = xã/phường
- `cafeland_id`: id Cafeland cũ

### 2.2 `transaction_city_merge`

Bảng chuyển từ id Cafeland cũ sang id Cafeland mới dùng cho upload.

Các cột cần dùng:

- `old_city_id`: id Cafeland cũ
- `new_city_id`: id Cafeland mới
- `action_type`

Trong thực tế map xã/phường của step Nhatot đang dùng:

- `location_detail.cafeland_id -> transaction_city_merge.old_city_id`
- lấy `transaction_city_merge.new_city_id`

## 3. Quy tắc merge chuẩn nhất

### 3.1 Tỉnh (`province_id` / `city_id` upload)

Join theo id gốc tỉnh:

```sql
ad_listing_detail.region_v2 = location_detail.region_id
AND location_detail.level = 1
```

Lấy id Cafeland cũ:

```sql
location_detail.cafeland_id
```

Sau đó đổi sang id mới để upload:

```sql
LEFT JOIN transaction_city_merge m ON location_detail.cafeland_id = m.old_city_id
province_id_upload = COALESCE(m.new_city_id, location_detail.cafeland_id)
```

## 3.2 Xã / Phường (`ward_id` / `wards_id` upload)

Join theo đủ 3 khóa gốc:

```sql
ad_listing_detail.ward = location_detail.ward_id
AND ad_listing_detail.area_v2 = location_detail.area_id
AND ad_listing_detail.region_v2 = location_detail.region_id
AND location_detail.level = 3
```

Lấy id Cafeland cũ:

```sql
location_detail.cafeland_id
```

Sau đó đổi sang id mới:

```sql
LEFT JOIN transaction_city_merge m ON location_detail.cafeland_id = m.old_city_id
ward_id_upload = COALESCE(m.new_city_id, location_detail.cafeland_id)
```

## 3.3 Quận / Huyện

Hiện tại nhánh `data_no_full` đang **bỏ qua district_id**.

Quy tắc nếu cần lấy sau này:

```sql
ad_listing_detail.area_v2 = location_detail.area_id
AND ad_listing_detail.region_v2 = location_detail.region_id
AND location_detail.level = 2
```

Nhưng ở flow hiện tại:

- `district_id` có thể để `0`
- upload vẫn chạy được với dữ liệu test hiện tại

## 4. Đây cũng là rule mà step 1 Nhatot đang dùng

File tham chiếu:

- `/home/chungnt/crawlvip/craw/auto/nhatot_step1_mergekhuvuc_v2.py`

Rule trong file đó:

### Province

```sql
JOIN location_detail l ON d.src_province_id = l.region_id
SET d.cf_province_id = l.cafeland_id
WHERE l.level = 1
```

### District

```sql
JOIN location_detail l ON d.src_district_id = l.area_id AND d.src_province_id = l.region_id
SET d.cf_district_id = l.cafeland_id
WHERE l.level = 2
```

### Ward

```sql
JOIN location_detail l ON d.src_ward_id = l.ward_id
  AND d.src_district_id = l.area_id
  AND d.src_province_id = l.region_id
LEFT JOIN transaction_city_merge m ON l.cafeland_id = m.old_city_id
SET d.cf_ward_id = COALESCE(m.new_city_id, l.cafeland_id)
WHERE l.level = 3
```

Khác biệt cần lưu ý:

- Step 1 hiện mới convert `province` sang `cafeland_id` cũ
- còn `ward` thì đã `COALESCE(new_city_id, cafeland_id)`
- nếu mục tiêu là **upload chuẩn**, nên áp quy tắc `COALESCE(m.new_city_id, l.cafeland_id)` cho cả tỉnh

## 5. Kết luận chốt để dùng cho upload

### Lấy `city_id`

1. dùng `ad_listing_detail.region_v2`
2. join `location_detail` theo `region_id`, `level = 1`
3. lấy `location_detail.cafeland_id`
4. join `transaction_city_merge` qua `old_city_id`
5. lấy:

```sql
city_id = COALESCE(transaction_city_merge.new_city_id, location_detail.cafeland_id)
```

### Lấy `wards_id`

1. dùng `ad_listing_detail.ward`
2. join `location_detail` theo `ward_id + area_id + region_id`, `level = 3`
3. lấy `location_detail.cafeland_id`
4. join `transaction_city_merge` qua `old_city_id`
5. lấy:

```sql
wards_id = COALESCE(transaction_city_merge.new_city_id, location_detail.cafeland_id)
```

## 6. SQL mẫu dùng trực tiếp

```sql
SELECT
    a.ad_id,
    a.region_v2,
    a.area_v2,
    a.ward,
    COALESCE(mp.new_city_id, lp.cafeland_id) AS upload_city_id,
    COALESCE(mw.new_city_id, lw.cafeland_id) AS upload_ward_id
FROM ad_listing_detail a
LEFT JOIN location_detail lp
    ON a.region_v2 = lp.region_id
   AND lp.level = 1
LEFT JOIN transaction_city_merge mp
    ON lp.cafeland_id = mp.old_city_id
LEFT JOIN location_detail lw
    ON a.ward = lw.ward_id
   AND a.area_v2 = lw.area_id
   AND a.region_v2 = lw.region_id
   AND lw.level = 3
LEFT JOIN transaction_city_merge mw
    ON lw.cafeland_id = mw.old_city_id
WHERE a.ad_id = ?;
```

## 7. Kết luận ngắn

Nếu mục tiêu là **lấy id tỉnh/xã chuẩn nhất để upload**, thì phải merge theo thứ tự:

```text
ad_listing_detail (id gốc Nhatot)
-> location_detail (cafeland_id cũ)
-> transaction_city_merge (new_city_id)
```

Không nên:

- map theo tên tỉnh/xã
- dùng trực tiếp `region_name_v3` / `ward_name_v3` để lấy id
- dùng `cf_province_id` cũ mà chưa qua `transaction_city_merge`
