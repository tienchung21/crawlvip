# Plan convert MeeyMap/Meeyland -> data_full

Tai lieu nay chot plan convert tu `scraped_details_flat` sang `data_full` cho:

- ban: `trade_type = 's'` (chu yeu domain `meeymap.com`)
- thue: `trade_type = 'u'` (chu yeu domain `meeyland.com`)

## 1) Scope + filter

Nguon:
- `scraped_details_flat sdf`
- `scraped_detail_images sdi` (lay anh dau tien)
- `location_meeland lm_*` (map id khu vuc qua `cafeland_id`)

Filter row:
- `sdf.domain IN ('meeymap.com','meeyland.com')`
- `sdf.trade_type IN ('s','u')`
- `sdf.matin IS NOT NULL`
- `sdf.title IS NOT NULL`
- `COALESCE(sdf.created_at, NOW()) >= (NOW() - INTERVAL 6 MONTH)`
- `NOT EXISTS (SELECT 1 FROM data_full df WHERE df.source = sdf.domain AND df.source_post_id = sdf.matin)`

Ghi chu:
- neu can chay incremental: them `COALESCE(sdf.datafull_converted,0)=0`.

## 2) Mapping cot chinh sang data_full

| Nguon | Dich `data_full` | Rule |
|---|---|---|
| `sdf.title` | `title` | lay nguyen ban |
| `sdi.image_url` (idx nho nhat) | `img` | anh dai dien |
| `sdf.khoanggia` | `price` | uu tien parse so VND; neu dang la so thi dung truc tiep |
| `sdf.dientich` | `area` | parse so |
| `sdf.mota` | `description` | giu text |
| `mapping loaihinh+trade_type` | `property_type` | theo bang muc 4 + 5 |
| `sdf.trade_type` | `type` | `s -> Cần bán`, `u -> Cho thuê` |
| `sdf.huongnha` | `house_direction` | raw |
| `sdf.sotang` | `floors` | parse so nguyen |
| `sdf.sophongvesinh` | `bathrooms` | parse so |
| `sdf.sophongngu` | `bedrooms` | parse so |
| `sdf.duongvao` | `road_width` | parse so neu co |
| `sdf.phaply` | `legal_status` | raw |
| `sdf.lat` | `lat` | so thuc |
| `sdf.lng` | `long` | so thuc |
| `sdf.tenmoigioi` | `broker_name` | raw |
| `sdf.sodienthoai` | `phone` | raw |
| `sdf.domain` | `source` | giu nguon goc: `meeymap.com`/`meeyland.com` |
| `NOW()` | `time_converted_at` | thoi diem convert |
| `sdf.matin` | `source_post_id` | ma tin goc |
| `sdf.chieungang` | `width` | parse so |
| `sdf.chieudai` | `length` | parse so |
| map location | `province_id`,`ward_id` | theo muc 3 |
| `sdf.id` | `id_img` | link ve detail goc |
| `sdf.thuocduan` | `project_name` | uu tien ten du an tu detail |
| `sdf.thuocduan` + `duan_meeyland_duan_merge` | `project_id` | join `duan_meeyland_duan_merge.meeyland_project_id = sdf.thuocduan`, lay `duan_id` |
| mapping loaihinh+trade_type | `cat_id`,`type_id` | theo muc 4 + 5 |
| `cat_id` | `unit` | `cat_id=3 -> thang`, khac -> `md` |

Rule contact override:
- neu `sdf.tenmoigioi = 'Tài khoản Tin Crawl (system)'` thi:
  - `broker_name = 'Hỗ trợ online'`
  - `phone = '0942 825 711'`

## 3) Mapping location (province/ward)

Uu tien map theo `location_meeland.cafeland_id`, chi dung `code`:

- `province_id`:
  - join `lm_city.level_type='city'`
  - `lm_city.code = sdf.city_ext`

- `ward_id`:
  - join `lm_w.level_type='ward'`
  - `lm_w.code = sdf.ward_ext`

## 4) Mapping loaihinh cho BAN (`trade_type='s'`)

Mac dinh:
- `cat_id = 1` cho nhom ban
- `cat_id = 2` cho nhom dat
- loaihinh khong co ma trong danh sach duoi -> `skip_type`

| `sdf.loaihinh` (raw) | `property_type` | `cat_id` | `type_id` |
|---|---|---:|---:|
| `nha_rieng` | Bán nhà riêng | 1 | 2 |
| `nha_mat_pho` | Bán nhà riêng | 1 | 2 |
| `biet_thu_lien_ke` | Bán biệt thự | 1 | 3 |
| `can_ho_chung_cu` | Bán căn hộ chung cư | 1 | 5 |
| `chung_cu_mini` | Bán căn hộ Mini, Dịch vụ | 1 | 56 |
| `can_ho_van_phong_officetel` | Bán căn hộ Mini, Dịch vụ | 1 | 56 |
| `can_ho_dich_vu_homestay` | Bán căn hộ Mini, Dịch vụ | 1 | 56 |
| `can_ho_khach_san_condotel` | Bán nhà hàng - Khách sạn | 1 | 13 |
| `khach_san_nha_nghi` | Bán nhà hàng - Khách sạn | 1 | 13 |
| `shophouse_nha_pho_thuong_mai` | Bán nhà phố dự án | 1 | 1 |
| `kho_nha_xuong` | Bán kho, nhà xưởng | 1 | 14 |
| `nha_tap_the` | Bán căn hộ Mini, Dịch vụ | 1 | 56 |
| `dat` | Bán đất thổ cư | 2 | 11 |
| `dat_nen_du_an` | Bán đất nền dự án | 2 | 8 |

## 5) Mapping loaihinh cho THUE (`trade_type='u'`)

Mac dinh:
- tat ca nhom thue dung `cat_id = 3`
- loaihinh khong co ma trong danh sach duoi -> `skip_type`

| `sdf.loaihinh` (raw) | `property_type` | `cat_id` | `type_id` |
|---|---|---:|---:|
| `nha_mat_pho` | Nhà phố | 3 | 1 |
| `shophouse_nha_pho_thuong_mai` | Nhà phố | 3 | 1 |
| `nha_rieng` | Nhà riêng | 3 | 2 |
| `biet_thu_lien_ke` | Biệt thự | 3 | 3 |
| `can_ho_chung_cu` | Căn hộ chung cư | 3 | 5 |
| `can_ho_dich_vu_homestay` | Căn hộ chung cư | 3 | 5 |
| `chung_cu_mini` | Căn hộ chung cư | 3 | 5 |
| `van_phong` | Văn phòng | 3 | 6 |
| `van_phong_coworking` | Văn phòng | 3 | 6 |
| `mat_bang_cua_hang_ki_ot` | Mặt bằng | 3 | 12 |
| `kho_nha_xuong` | Nhà kho - Xưởng | 3 | 14 |
| `phong_tro` | Phòng trọ | 3 | 15 |
| `nha_tro` | Phòng trọ | 3 | 15 |
| `khach_san_nha_nghi` | Nhà hàng - Khách sạn | 3 | 13 |

## 6) Rule skip / quality

- `skip_type`: `loaihinh` khong nam trong bang mapping chot.
- `skip_region`: khong map duoc `province_id` hoac `ward_id`.
- `skip_price`: gia parse <= 0 hoac qua nguong DB.
- `skip_required`: thieu `title`/`matin`.

## 7) Huong implement script

File de tao:
- `craw/auto/convert_meeymap_to_data_full.py`

CLI de xuat:
- `--domain meeymap.com,meeyland.com`
- `--trade-type s,u`
- `--preview-limit`
- `--insert`
- `--debug`

Flow:
1. select batch tu `scraped_details_flat`.
2. build payload theo mapping.
3. insert `data_full`.
4. update `sdf.datafull_converted=1` cho row insert thanh cong.
5. log so row `inserted/skipped`.

## 8) Doi chieu schema data_full

Plan nay chi map vao cac cot co that trong `data_full`:
- `title,address,posted_at,img,price,area,description,property_type,type,house_direction,floors,bathrooms,road_width,living_rooms,bedrooms,legal_status,lat,long,broker_name,phone,source,time_converted_at,source_post_id,width,length,city,district,ward,street,province_id,district_id,ward_id,street_id,id_img,project_name,slug_name,images_status,stratum_id,cat_id,type_id,unit,project_id,uploaded_at`.

Trong plan hien tai KHONG set cac cot:
- `posted_at,address,city,district,ward,street,district_id,street_id,living_rooms,stratum_id,project_id,uploaded_at`.
