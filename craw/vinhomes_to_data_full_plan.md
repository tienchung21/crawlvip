# Vinhomes JSON -> data_full Plan

Nguon dang co:

- `/home/chungnt/crawlvip/craw/logs/vinhomes_secondary_search_thue.json`
- `/home/chungnt/crawlvip/craw/logs/vinhomes_market_thue.json`

## 1. Ket luan nhanh

Nguon nen dung de dua vao `data_full`:

- `vinhomes_secondary_search_thue.json`

Nguon khong nen dung lam nguon chinh de dua thang vao `data_full`:

- `vinhomes_market_thue.json`

Ly do:

- `secondary_search_thue` co nhieu truong hon, co `url`, `title`, `project_name`, `price_origin`, `service_type`, `property_type`, `direction`, `balcony_direction`, `floor_number`, `total_floors`, `num_bedroom`, `num_bathroom`, `area`, `area_using`, `list_photo`
- `market_thue` thieu `url`, thieu location, thieu project_name, thieu huong, thieu tang, khong co description
- `market_thue` cung khong co khoa join ro rang voi `secondary_search_thue`; `id` va `houseId` khong thay trung truc tiep voi `id`/`vhm_id` ben file search

## 2. Co the patch vao data_full tu secondary_search_thue

Mapping de xuat:

- `source = 'market.vinhomes.vn'`
- `source_post_id = item.id`
- `title = item.title`
- `slug_name = item.alias`
- `price = item.price_origin`
- `area = item.area`
- `description = NULL`
- `property_type = item.property_type`
- `type = 'u'`
- `house_direction = item.direction`
- `floors = item.total_floors`
- `bathrooms = item.num_bathroom`
- `bedrooms = item.num_bedroom`
- `lat = NULL`
- `long = NULL`
- `width = NULL`
- `length = NULL`
- `city = NULL`
- `district = NULL`
- `ward = NULL`
- `street = NULL`
- `province_id = NULL`
- `district_id = NULL`
- `ward_id = NULL`
- `street_id = NULL`
- `id_img = NULL`
- `project_name = item.project_name`
- `images_status = 'PENDING'`
- `stratum_id = NULL`
- `cat_id = 3`
- `type_id = map_tu property_type`
- `unit = 'tháng'`
- `project_id = NULL`
- `posted_at = NULL`
- `address = NULL`
- `broker_name = NULL`
- `phone = NULL`
- `road_width = NULL`
- `living_rooms = NULL`

## 3. Truong anh co the luu

Anh nen lay tu:

- `item.list_photo[*].w1200h800`
- fallback:
- `item.list_photo[*].w900h600`
- fallback:
- `item.thumbnail`

Neu convert vao `data_full`:

- `img = JSON array cac url anh`

Neu muon dua vao `scraped_detail_images` kieu cu:

- loop tung anh, insert tung dong

## 4. Mapping type_id de xuat

Can map tu `property_type`:

- `Căn hộ -> cat_id=3, type_id=5`
- `Biệt thự / Villa -> cat_id=3, type_id=3`
- `Shophouse / Nhà phố thương mại -> cat_id=3, type_id=12`
- `Nhà phố / Nhà liền kề -> cat_id=3, type_id=2`

Can note:

- File JSON hien tai can inspect them ky cac gia tri `property_type` thuc te truoc khi code bang map day du

## 5. Truong huu ich nhung khong co trong data_full hoac chua co cho de map

Tu `secondary_search_thue`:

- `unit_number`
- `vhm_id`
- `house_style`
- `balcony_direction`
- `project_alias`
- `project_area_cluster_alias`
- `project_area_cluster_name`
- `project_tower_alias`
- `project_tower_name`
- `price_unit`
- `area_using`
- `for_rent`
- `is_primary`
- `is_rent_sold`
- `lowest_price_unit_labels`

Nhung truong nay hien chua co cot phu hop trong `data_full`, nen neu can thi luu bang raw rieng.

## 6. Danh gia file vinhomes_market_thue.json

Co the lay duoc:

- `id`
- `name`
- `numberBathroom`
- `numberBedroom`
- `netArea`
- `bestPrice`
- `price`
- `slug`
- `images`
- `houseId`
- `houseName`

Nhung van de:

- khong co `url`
- khong co `project_name`
- khong co `title` chuan theo dang tin
- khong co `direction`
- khong co `floor_number`
- khong co `city/district/ward`
- khong co `description`
- khong co khoa join chac chan voi file `secondary_search_thue`

Ket luan:

- khong nen patch file nay vao `data_full`
- neu muon giu lai thi nen tao bang raw rieng, vi du `vinhomes_market_raw`

## 7. Plan thuc hien de xuat

Buoc 1:

- dung `vinhomes_secondary_search_thue.json` lam nguon chinh

Buoc 2:

- tao file mapping `property_type -> cat_id/type_id`

Buoc 3:

- insert vao `data_full` voi:
  - `source = market.vinhomes.vn`
  - `source_post_id = item.id`
  - `type = 'u'`
  - `unit = 'tháng'`

Buoc 4:

- luu `img` tu `list_photo`

Buoc 5:

- bo qua `vinhomes_market_thue.json` trong luong insert `data_full`

## 8. Chot tam thoi

Tu 2 file hien tai, cac truong patch duoc vao `data_full` mot cach sach nhat la:

- `source`
- `source_post_id`
- `title`
- `slug_name`
- `price`
- `area`
- `property_type`
- `type`
- `house_direction`
- `floors`
- `bathrooms`
- `bedrooms`
- `project_name`
- `img`
- `cat_id`
- `type_id`
- `unit`

Nhung truong location va contact hien tai deu chua co.
