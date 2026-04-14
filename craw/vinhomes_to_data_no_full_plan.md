## Plan Matching Vinhomes -> data_no_full

### 1) Nguon vao
- `scraped_details_flat`
- Filter:
  - `domain = 'vinhome'`
  - `trade_type IN ('s','u')`

### 2) Mapping chinh
- `title` -> `title`
- `mota` -> `description`
- `khoanggia` -> `price`
- `dientich` -> `area`
- `huongnha` -> `house_direction`
- `sophongngu` -> `bedrooms`
- `sophongvesinh` -> `bathrooms`
- `thuocduan` -> `project_name`
- `diachi` -> `address`
- `lat` -> `lat`
- `lng` -> `long`
- `loaihinh` -> `property_type`                                      
- `trade_type`:
  - `s` -> `type = 's'`
  - `u` -> `type = 'u'`
- `cat_id` / `type_id`:
  - Bán (`trade_type = s`):
    - `cat_id=1` (mac dinh cho toan bo ben ban)
    - `type_id` theo loai:
      - Căn hộ: `type_id=5`
      - Nhà liền kề: `type_id=1`
      - Shophouse: `type_id=1`
      - Thương mại dịch vụ: `type_id=56`
      - Biệt thự Tứ lập: `type_id=3`
      - Biệt thự Song lập: `type_id=3`
      - Biệt thự Đơn lập: `type_id=3`
  - Thuê (`trade_type = u`):
    - `cat_id=3`, `type_id=1` (tat ca ben thue)
- `source = 'vinhome'`
- `source_post_id = matin`
- `slug_name` <- slug cuoi trong `url`
- `unit`:
  - `s` -> `VND`   (xem lại mấy nguồn khác dùng m2 hay vnd)
  - `u` -> `thang`

Ghi chu cu (bo qua): da thay bang mapping tren.






### 2.1) Mapping diachi -> city/district/ward (de ra city_id, district_id, ward_id)
- Tach `diachi` theo dau phay:
  - 3+ phan: `ward = part[-3]`, `district = part[-2]`, `province = part[-1]`
  - 2 phan: `district = part[-2]`, `province = part[-1]`
  - 1 phan: `province = part[-1]`
- Chuan hoa ten (lowercase, bo dau, bo prefix: `Tinh/Thanh pho/TP/Quan/Huyen/Phuong/Xa/Thi xa/Thi tran`)
- Match bang `transaction_city`:
  - Province: `city_parent_id = 0`
  - District: `city_parent_id = province_id`
  - Ward: `city_parent_id = district_id`
- Sau khi co old_id, map sang id moi qua `transaction_city_merge`:
  - `new_city_id` = `transaction_city_merge.new_city_id` (theo `old_city_id`)
  - Luu vao `data_no_full` chi can `province_id = new_prov` va `ward_id = new_ward` (district co the bo qua neu new_dist NULL)

### 3) project_id (neu can)
Map `thuocduan` -> `duan_id`:
- `Vinhomes Grand Park` -> `1650`
- `Vinhomes Ocean Park` -> `2407`
- `Vinhomes Ocean Park 2` -> `3337`
- `Vinhomes Ocean Park 3` -> `3721`
- `Vinhomes Royal Island` -> `4430`
- `Vinhomes Golden City` -> `4503`
- `Vinhomes Global Gate` -> `4532`

### 4) Anh
- Lay anh tu `scraped_detail_images`
- `data_no_full.img` chi can 1 anh (anh dau)  ()

### 5) Chong trung
- Key: `source + source_post_id`
- Neu trung: update gia/dien tich/anh

### 6) Script de xuat
- Tao `craw/auto/convert_vinhomes_to_data_no_full.py`
- Select tu `scraped_details_flat` -> insert/update vao `data_no_full`
