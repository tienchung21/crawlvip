# Nhatot -> data_no_full Mapping Plan

## Huong moi

Bo huong join `data_clean_v1`.

Huong moi chi dung:
- `ad_listing_detail d`
- `scraped_detail_images di`

Muc tieu:
- di thang tu raw Nhatot sang `data_no_full`
- giu du lieu tho can thiet de upload API khac
- khong phu thuoc pipeline `cleanv1`

## Nguon du lieu

- Nguon chinh: `ad_listing_detail d`
- Nguon anh: `scraped_detail_images di`

## Join de xuat

- `di.detail_id = d.ad_id`

Ghi chu:
- voi Nhatot, hien tai anh duoc dong bo vao `scraped_detail_images` theo quy uoc `detail_id = ad_id`

## Dieu kien chon row

De xuat dieu kien toi thieu:
- `d.list_id IS NOT NULL`
- `d.price IS NOT NULL`
- `d.size IS NOT NULL`
- `d.category_name IS NOT NULL`
- `d.type IN ('s', 'u')`
- khong lay row da ton tai trong `data_no_full`:
  - `NOT EXISTS (SELECT 1 FROM data_no_full n WHERE n.source = 'nhatot' AND n.source_post_id = d.list_id)`

## Mapping cot chinh

| Cot `data_no_full` | Nguon | Ghi chu |
|---|---|---|
| `title` | `COALESCE(d.subject|
| `posted_at` | `FROM_UNIXTIME(CAST(COALESCE(d.orig_list_time, d.list_time) AS UNSIGNED) / 1000)` | uu tien `orig_list_time` |
| `img` | anh dau tien trong `scraped_detail_images` 
| `price` | `CAST(d.price AS DECIMAL(20,2))` | |
| `area` | `CAST(d.size AS DECIMAL(20,2))` | |
| `width` | `CAST(d.width AS DECIMAL(10,2))` | |
| `length` | `CAST(d.length AS DECIMAL(10,2))` | |
| `description` | `d.body` | |
| `bathrooms` | `CAST(d.toilets AS SIGNED)` | |
| `bedrooms` | `CAST(d.rooms AS SIGNED)` | |
| `legal_status` | `d.property_legal_document` | (nhatot quy định 1	Ðã có s?
2	Ðang ch? s?
3	Gi?y t? khác
4	H?p d?ng d?t c?c
5	H?p d?ng mua bán
6	S? h?ng riêng

check xem pháp lý bên nhatdat là j rồi quy tương ứng
| `lat` | `CAST(d.latitude AS DECIMAL(20,14))` | |
| `long` | `CAST(d.longitude AS DECIMAL(20,14))` | |
| `broker_name` | `COALESCE(d.full_name, d.account_name)` | |

| `source` | `'nhatot'` | |
| `time_converted_at` | `NOW()` | |
| `source_post_id` | `d.list_id` | |

## Mapping text location

| Cot `data_no_full` | Nguon | Ghi chu |
|---|---|---|
| `city` | `COALESCE(d.region_name_v3, d.region_name)` | | dùng bảng location_detail mà merge , học theo cách dùng cuủa step 1 nhatot ấy

| `ward` | `COALESCE(d.ward_name_v3, d.ward_name)` | | dùng bảng location_detail mà merge , học theo cách dùng cuủa step 1 nhatot ấy
| `street` | `d.street_name` | |   truyền street_name


## Mapping type / category

Quy uoc he thong dang dung:
- `cat_id = 1` -> bán
- `cat_id = 3` -> thuê

| Cot `data_no_full` | Nguon | Ghi chu |
|---|---|---|
| `type` | `d.type` | `s` / `u` |
| `cat_id` | `CASE WHEN d.type='u' THEN 3 ELSE 1 END` | theo quy uoc he thong |
| `unit` | `CASE WHEN d.type='u' THEN 'thang' ELSE 'md' END` | |

## Nhatot co nhung loai hinh nao

Day la danh sach nhom loai hinh thuc te trong `ad_listing_detail`.

### 1. `category_name = 'Nhà ở'`

Su dung `house_type`:

| `house_type` | Ten loai hinh |
|---|---|
| `1` | `Nhà mặt phố, mặt tiền` |
| `2` | `Nhà ngõ, hẻm` |
| `3` | `Biệt thự` |
| `4` | `Nhà phố liền kề` |

Phan bo hien tai:
- `s + house_type=3`: 78,563
- `s + house_type=1`: 28,061
- `u + house_type=3`: 22,597
- `u + house_type=1`: 14,530
- `s + house_type=4`: 5,227
- `u + house_type=4`: 2,717
- `s + house_type=2`: 1,950
- `u + house_type=2`: 1,420

### 2. `category_name = 'Căn hộ/Chung cư'`

Su dung `apartment_type`:

| `apartment_type` | Ten loai hinh |
|---|---|
| `1` | `Chung cư` |
| `2` | `Căn hộ dịch vụ, mini` |
| `3` | `Chua co label verify trong repo` |
| `4` | `Penthouse` |
| `5` | `Chua co label verify trong repo` |
| `6` | `Chua co label verify trong repo` |

Phan bo lon nhat:
- `u + apartment_type=1`: 19,998
- `u + apartment_type=2`: 19,803
- `s + apartment_type=1`: 16,493
- `u + apartment_type=3`: 1,568
- `s + apartment_type=2`: 845
- `s + apartment_type=5`: 787
- `u + apartment_type=6`: 315

### 3. `category_name = 'Đất'`

Su dung `land_type`:

| `land_type` | Ghi chu |
|---|---|
| `1` | can map ten |
| `2` | can map ten |
| `3` | can map ten |
| `4` | can map ten |

Phan bo hien tai:
- `s + land_type=1`: 37,770
- `s + land_type=4`: 4,366
- `s + land_type=2`: 2,765
- `u + land_type=1`: 674
- `s + land_type=3`: 318
- `u + land_type=4`: 193

### 4. `category_name = 'Văn phòng, Mặt bằng kinh doanh'`

Su dung `commercial_type`:

| `commercial_type` | Ghi chu |
|---|---|
| `1` | can map ten |
| `2` | can map ten |
| `3` | can map ten |
| `4` | can map ten |

Phan bo hien tai:
- `u + commercial_type=4`: 20,803
- `u + commercial_type=3`: 3,457
- `s + commercial_type=4`: 1,601
- `u + commercial_type=1`: 333
- `s + commercial_type=1`: 243
- `u + commercial_type=2`: 217

### 5. `category_name = 'Phòng trọ'`

- khong co subtype rieng dang dung
- toan bo nhom nay co the map thang -> `Phòng trọ`
- hien co: 41,000 row, deu la `type='u'`

## Mapping property_type de xuat

| Dieu kien nguon | `property_type` de xuat |
|---|---|
| `category_name='Nhà ở'` + `house_type=1` | `Nhà phố` |
| `category_name='Nhà ở'` + `house_type=2` | `Nhà riêng` |
| `category_name='Nhà ở'` + `house_type=3` | `Biệt thự` |
| `category_name='Nhà ở'` + `house_type=4` | `Nhà phố liền kề` |
| `category_name='Căn hộ/Chung cư'` + `apartment_type=1` | `Căn hộ chung cư` |
| `category_name='Căn hộ/Chung cư'` + `apartment_type=2` | `Căn hộ dịch vụ, mini` |
| `category_name='Căn hộ/Chung cư'` + `apartment_type=4` | `Penthouse` |
| `category_name='Phòng trọ'` | `Phòng trọ` |
| `category_name='Đất'` + `land_type=*` | can ban chot ten voi Nhadat |
| `category_name='Văn phòng, Mặt bằng kinh doanh'` + `commercial_type=*` | can ban chot ten voi Nhadat |

## Mapping type_id

- Chua final trong file nay
- Nen chot sau khi ban merge xong bang `property_type` voi Nhadat

## Mapping legal_status / stratum_id

Luu y quan trong:
- `property_legal_document` cua Nhatot la enum dung chung
- nhung y nghia phu thuoc theo `category`
- vi vay khong duoc map `stratum_id` chi theo so `1..6`
- phai map theo cap: `(category, property_legal_document)`

### Bang y nghia raw theo category

| `property_legal_document` | `category=1010` | `category=1020` | `category=1030` | `category=1040` | `category=1050` |
|---|---|---|---|---|---|
| `1` | Đã có sổ | Đã có sổ | Đã có sổ | Đã có sổ |  |
| `2` | Đang chờ sổ | Đang chờ sổ | Đang chờ sổ | Đang chờ sổ |  |
| `3` |  |  | Giấy tờ khác |  |  |
| `4` | Hợp đồng đặt cọc | Không có sổ |  | Không có sổ |  |
| `5` | Hợp đồng mua bán | Sổ chung / công chứng vi bằng |  | Sổ chung / công chứng vi bằng |  |
| `6` | Sổ hồng riêng | Giấy tờ viết tay |  | Giấy tờ viết tay |  |

### De xuat luu vao `legal_status`

- `legal_status` nen luu text da resolve theo cap `(category, property_legal_document)`
- khong nen luu chi moi so raw neu muc tieu la upload/API

### Match sang phap ly chuan Nhadat

Danh sach chuan Nhadat:
- `1` -> `Sổ hồng`
- `2` -> `Giấy đỏ`
- `3` -> `Giấy tay`
- `4` -> `Giấy tờ hợp lệ`
- `5` -> `Đang hợp thức hóa`
- `6` -> `Chủ quyền tư nhân`
- `7` -> `Hợp đồng`
- `8` -> `Không xác định`

### Bang match da duyet

| Category | Raw Nhatot | `legal_status` chuan | `stratum_id` |
|---|---|---|---:|
| moi category | `Đã có sổ` | `Sổ hồng` | `1` |
| `1010` | `Sổ hồng riêng` | `Sổ hồng` | `1` |
| moi category | `Hợp đồng mua bán` | `Hợp đồng` | `7` |
| `1010` | `Hợp đồng đặt cọc` | `Hợp đồng` | `7` |
| moi category | `Giấy tờ khác` | `Giấy tờ hợp lệ` | `4` |
| `1020`, `1040` | `Sổ chung / công chứng vi bằng` | `Giấy tờ hợp lệ` | `4` |
| moi category | `Đang chờ sổ` | `Đang hợp thức hóa` | `5` |
| `1020`, `1040` | `Giấy tờ viết tay` | `Giấy tay` | `3` |
| `1020`, `1040` | `Không có sổ` | `Không xác định` | `8` |
| `1050` hoac rong | rong / khong ro | `Không xác định` | `8` |



## Mapping type_id da chot cho Nhatot

Can cu API + quy tac da chot:
- `cat_id = 1` -> Nha dat ban
- `cat_id = 2` -> Dat
- `cat_id = 3` -> Cho thue

Danh sach `type_id` hop le tu API `get-category`:

### `cat_id = 1` (ban)

- `1` -> Ban nha pho du an
- `2` -> Ban nha rieng
- `3` -> Ban biet thu
- `5` -> Ban can ho chung cu
- `56` -> Ban can ho Mini, Dich vu
- `8` -> Ban dat nen du an
- `10` -> Ban dat nong, lam nghiep
- `11` -> Ban dat tho cu
- `13` -> Ban nha hang - Khach san
- `14` -> Ban kho, nha xuong

### `cat_id = 2` (dat)

- `8` -> Ban dat nen du an
- `10` -> Ban dat nong, lam nghiep
- `11` -> Ban dat tho cu

### `cat_id = 3` (thue)

- `1` -> Nha pho
- `2` -> Nha rieng
- `3` -> Biet thu
- `5` -> Can ho chung cu
- `6` -> Van phong
- `12` -> Mat bang
- `13` -> Nha hang - Khach san
- `14` -> Nha kho - Xuong
- `15` -> Phong tro
- `57` -> Dat khu cong nghiep

### Mapping Nhatot -> `type_id`

### Ben ban (`type = "s"`, `cat_id = 1`, tru nhom dat)

| Dieu kien | `type_id` |
|---|---:|
| `category=1020` + `house_type=1` | `2` |
| `category=1020` + `house_type=2` | `2` |
| `category=1020` + `house_type=3` | `3` |
| `category=1020` + `house_type=4` | `1` |
| `category=1010` + `apartment_type=2` | `56` |
| `category=1010` + `apartment_type IN (1,3,4,5,6)` | `5` |
| `category=1030` + `commercial_type=1` | `13` |
| `category=1030` + `commercial_type=2` | `14` |
| `category=1030` + `commercial_type=3` | `NULL` |
| `category=1030` + `commercial_type=4` | `NULL` |
| `category=1050` | `NULL` |
Ghi chu:
- `commercial_type=3/4` ben ban hien chua co `type_id` tuong ung trong API ban. Khong nen ep sang `56`.
- `category=1050` ben ban la du lieu le, khong co loai tuong ung trong API ban.

### Nhom dat (`category = 1040`, dung `cat_id = 2`)

| Dieu kien | `cat_id` | `type_id` |
|---|---:|---:|
| `land_type=1` | `2` | `11` |
| `land_type=2` | `2` | `8` |
| `land_type=3` | `2` | `10` |
| `land_type=4` | `2` | `10` |

Ghi chu:
- quy tac nay ap dung cho ca `type='s'` va `type='u'`
- tuc la voi dat, khong dung `cat_id=1/3`, ma gom ve `cat_id=2`

### Ben thue (`type = "u"`, `cat_id = 3`, tru nhom dat)

| Dieu kien | `type_id` |
|---|---:|
| `category=1020` + `house_type=1` | `1` |
| `category=1020` + `house_type=2` | `2` |
| `category=1020` + `house_type=3` | `3` |
| `category=1020` + `house_type=4` | `1` |
| `category=1010` + `apartment_type IN (1,2,3,4,5,6)` | `5` |
| `category=1030` + `commercial_type=4` | `6` |
| `category=1030` + `commercial_type=3` | `12` |
| `category=1030` + `commercial_type=1` | `13` |
| `category=1030` + `commercial_type=2` | `14` |
| `category=1050` | `15` |

Ghi chu:
- `category=1010` ben thue khong co `56` trong API, nen map toan bo ve `5`.
- Khi ETL that, nen bo qua row co `type_id IS NULL` de tranh upload sai loai.

## Cot phu

| Cot `data_no_full` | Nguon | Ghi chu |
|---|---|---|
| `floors` | `CAST(d.floors AS SIGNED)` | |
| `house_direction` | `d.direction` | |
| `road_width` | `NULL` | chua co nguon on dinh |
| `living_rooms` | `NULL` | |
| `id_img` | `d.ad_id` | quy uoc Nhatot = `ad_id` |
| `project_name` | `d.pty_project_name` | |
| `slug_name` | `NULL` | |
| `images_status` | `'PENDING'` | neu can upload anh |
| `uploaded_at` | `NULL` | |

## Anh va video

### Anh
- `data_no_full.img`: lay 1 anh dau tien tu `scraped_detail_images`
- full gallery: giu o `scraped_detail_images`
- Nhatot hien sync anh theo `images[]` cua API vao `scraped_detail_images`
- insert anh co check ton tai theo cap `detail_id + image_url` de tranh duplicate

### Video
- video giu o `ad_listing_detail.videos`
- hien tai luu ca mang JSON, khong cat rieng moi `url`
- khong nhat thiet dua video vao `data_no_full`

## Cach trien khai de xuat

1. Chon `ad_listing_detail d`
2. `LEFT JOIN` subquery anh dau tien tu `scraped_detail_images`
3. `INSERT INTO data_no_full (...) SELECT ...`
4. Chong trung bang `NOT EXISTS` theo `source='nhatot' AND source_post_id=d.list_id`
5. Bo qua row co `type_id IS NULL`
