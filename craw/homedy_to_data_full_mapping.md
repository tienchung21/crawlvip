# Homedy -> data_full (Plan chot)

Nguon:
- `scraped_details_flat` (`domain='homedy.com'`)
- location map: `location_homedy` (dung `cafeland_id`)
- project map: `duan_homedy_duan_merge`

## 1) Mapping cot co ban

`data_full.title` <- `sdf.title`  
`data_full.address` <- `sdf.diachi`  # không cần thiết 
`data_full.posted_at` <- parse `sdf.ngaydang`  
`data_full.price` <- parse so tu `sdf.khoanggia`  # phần này xem guland làm cho ki cách pase(tỉ , triệu)
`data_full.area` <- parse so tu `sdf.dientich`  
`data_full.description` <- `sdf.mota`  
`data_full.bedrooms` <- `sdf.sophongngu`   
`data_full.bathrooms` <- `sdf.sophongvesinh`    
`data_full.lat` <- `sdf.lat`  
`data_full.long` <- `sdf.lng`  
`data_full.broker_name` <- `sdf.tenmoigioi`  
`data_full.phone` <- `sdf.sodienthoai`  
`data_full.source` <- `'homedy.com'`  
`data_full.source_post_id` <- `sdf.matin`  
`data_full.id_img` <- `sdf.id`  
`data_full.type` <- `sdf.trade_type` (`s`/`u`)  
`data_full.unit` <- `sdf.trade_type='s' -> 'md'`, `sdf.trade_type='u' -> 'thang'`  

Anh:
- `data_full.img` <- anh dau tien tu `scraped_detail_images` theo `detail_id=sdf.id`, `ORDER BY idx ASC`

## 2) Mapping khu vuc (bat buoc qua location_homedy)

Input ext:
- `sdf.city_ext`, `sdf.district_ext`, `sdf.ward_ext`

Join:
- `lh_city`: `level_type='city' AND location_id = CAST(sdf.city_ext AS UNSIGNED)`
- `lh_dist`: `level_type='district' AND location_id = CAST(sdf.district_ext AS UNSIGNED)` # ko cần 
- `lh_ward`: `level_type='ward' AND location_id = CAST(sdf.ward_ext AS UNSIGNED)`

Ghi vao `data_full`:
- `province_id = lh_city.cafeland_id`
- `district_id = lh_dist.cafeland_id` # ko cần 
- `ward_id = lh_ward.cafeland_id`

## 3) Mapping du an

Chot logic dung:
- `sdf.thuocduan` la `ProjectId` cua tin Homedy.
- `ProjectId` nay khop voi `duan_homedy.homedy_id` (KHONG khop `duan_homedy.project_id`/`Code`).
- Bang merge `duan_homedy_duan_merge.homedy_project_id` phai duoc build theo `duan_homedy.homedy_id`.

Join:
- `duan_homedy_duan_merge.homedy_project_id = CAST(sdf.thuocduan AS UNSIGNED)`

Ghi vao `data_full`:
- `project_id = duan_homedy_duan_merge.duan_id`
- `project_name = duan_homedy_duan_merge.duan_ten`  # ko cần 

## 4) Mapping loaihinh -> type_id/property_type

### Ban (`trade_type='s'`)

| loaihinh | type_id | property_type |
|---|---:|---|
| 57,70,71,164,165,166,167,68,168,169 | 5 | Bán căn hộ chung cư |
| 73 | 56 | Bán căn hộ Mini, Dịch vụ |
| 62 | 2 | Bán nhà riêng |
| 63,66 | 1 | Bán nhà phố dự án |
| 56,170,171,172,190 | 3 | Bán biệt thự |
| 58 | 11 | Bán đất thổ cư |
| 77,78 | 10 | Bán đất nông, lâm nghiệp |
| 79 | 8 | Bán đất nền dự án |
| 83 | 14 | Bán kho, nhà xưởng |
| 85 | 13 | Bán nhà hàng - Khách sạn |
| 61,80,81,174 | NULL | NULL (de sau) |

### Thue (`trade_type='u'`)

| loaihinh | type_id | property_type |
|---|---:|---|
| 57,73,70,71,164,165,166,167,68,168,169,76 | 5 | Căn hộ chung cư |
| 62 | 2 | Nhà riêng |
| 63,66 | 1 | Nhà phố |
| 56,170,171,172,190 | 3 | Biệt thự |
| 81 | 15 | Phòng trọ |
| 59 | 6 | Văn phòng |
| 86 | 12 | Mặt bằng |
| 87 | 14 | Nhà Kho - Xưởng |
| 61,84 | NULL | NULL (de sau) |

## 5) Rule cat_id

- `trade_type='u'` -> `cat_id = 3`
- `trade_type='s'`:
  - nhom dat (`loaihinh` in `58,77,78,79`) -> `cat_id = 2`  (theo note: "đất thì type=2")
  - con lai -> `cat_id = 1`

## 6) Truong de NULL (giai doan 1)

`width`, `length`, `legal_status`, `floors`, `house_direction`, `road_width`, `living_rooms`, `stratum_id`

## 7) Anti-duplicate + update mode

Khoa chong trung:
- `source='homedy.com' AND source_post_id=sdf.matin`

Che do:
- Da ton tai -> `UPDATE` (gia, mo ta, khu vuc, project, loaihinh map)
- Chua ton tai -> `INSERT`
