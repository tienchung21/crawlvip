# Mapping `data_no_full`

Tai lieu nay mo ta mapping de convert du lieu sang bang `data_no_full`.

## Muc tieu

- Nguon chinh: `data_clean_v1` (`dc`)
- Nguon bo sung: `scraped_details_flat` (`sdf`)
- Nguon anh: `scraped_detail_images` (`di`)
- Pham vi uu tien: cac domain khong day du lien he, nhung da duoc chuan hoa gia, dien tich, dia chi, loai hinh trong `data_clean_v1`

## Join de xuat

- `dc.ad_id = sdf.matin`
- `dc.domain = sdf.domain`
- anh dau tien:
  - `di.detail_id = sdf.id`
  - lay `di.image_url` co `idx` nho nhat

## Dieu kien de xuat

- `dc.domain = <domain_can_convert>`
- `dc.process_status >= 4`
- `dc.price_vnd > 0`
- `dc.std_area > 0`
- `dc.cf_province_id IS NOT NULL`
- `COALESCE(sdf.datafull_converted, 0) = 0` chi la tham chieu neu muon tranh dung lai du lieu da dua vao `data_full`
- Co che chong trung cho `data_no_full`:
  - uu tien `source + source_post_id` la unique logic

## Mapping cot

| Cot `data_no_full` | Nguon de xuat | Ghi chu |
|---|---|---|
| `id` | Auto increment | Khong map |
| `title` | `sdf.title` | Neu `NULL` thi fallback tu `dc.ad_id` la khong nen; nen bo qua row neu thieu |
| `address` | `COALESCE(sdf.diachicu, sdf.diachi)` | Uu tien dia chi cu day du quan/huyen neu co |   ##bỏ 
| `posted_at` | `FROM_UNIXTIME(dc.orig_list_time)` | Neu `dc.orig_list_time` null thi fallback `FROM_UNIXTIME(dc.update_time)` |
| `img` | `di.image_url` dau tien | Lay anh co `idx` nho nhat |
| `price` | `dc.price_vnd` | Da duoc chuan hoa o `cleanv1` |
| `area` | `dc.std_area` | Da duoc chuan hoa o `cleanv1` |
| `description` | `sdf.mota` | Giu nguyen text goc |
| `property_type` | Mapping tu `dc.std_category + dc.std_trans_type` | ### phần này nhắc tao bổ sung lại logic merge vì dây là domian mới
| `type` | `dc.std_trans_type` | Thuong la `s`/`u` neu can, neu khong thi map lai tu `src_type` |   ### phần này báo cáo cho tao biết , bên mogi chuyển sang strtum_id và cat_id lúc convert qua hy lúc upload 
| `house_direction` | `NULL` cho `batdongsan.com.vn`; `sdf.huongnha` cho domain khac | BDS hien tai khong co du lieu cot nay trong `scraped_details_flat` | 
| `floors` | `NULL` cho `batdongsan.com.vn`; parse so tu `sdf.sotang` cho domain khac | BDS hien tai khong co du lieu cot nay trong `scraped_details_flat` |
| `bathrooms` | Parse so tu `sdf.sophongvesinh` | Vi cot dich la `int` |
| `living_rooms` | `NULL` | Nguon hien tai khong co cot on dinh | # bỏ
| `bedrooms` | Parse so tu `sdf.sophongngu` | Vi cot dich la `int` |
| `legal_status` | `sdf.phaply` | Raw | # bỏ 
| `lat` | Tach tu `sdf.map` neu co | Neu `map` dang `lat,long` thi tach; neu khong thi `NULL` |  # bỏ
| `long` | Tach tu `sdf.map` neu co | Neu `map` dang `lat,long` thi tach; neu khong thi `NULL` | # bỏ 
| `broker_name` | `sdf.tenmoigioi` | Raw |
| `phone` | `sdf.sodienthoai` | Bang nay cho phep `NULL`, nen khong can ep |  # bỏ
| `source` | `dc.domain` hoac ten nguon rut gon | Nen thong nhat voi uploader API moi |
| `time_converted_at` | `NOW()` | Thoi diem convert |
| `source_post_id` | `dc.ad_id` | Uu tien `dc.ad_id`, fallback `sdf.matin` |
| `width` | Parse so tu `sdf.chieungang` | Vi cot dich la `decimal` | # bỏ
| `length` | Parse so tu `sdf.chieudai` | Vi cot dich la `decimal` |  # bỏ 
| `city` | Ten tinh moi theo `dc.cf_province_id` | Voi BDS khong can query `sdf.city_ext` vi cot nay dang trong |
| `district` | `NULL` cho BDS, hoac ten quan/huyen tu bang merge neu can bo sung sau | Khong query `sdf.district_ext` cho BDS vi cot nay dang trong |
| `ward` | Ten xa moi theo `dc.cf_ward_id` | Voi BDS khong can query `sdf.ward_ext` vi cot nay dang trong |
| `street` | `sdf.street_ext` | Neu khong co thi co the tach tu `diachi` | #Bỏ 
| `province_id` | `dc.cf_province_id` | Cot chinh de upload |
| `district_id` | `dc.cf_district_id` | Neu domain khong co thi `NULL` |
| `ward_id` | `dc.cf_ward_id` | Cot chinh de upload |
| `street_id` | `dc.cf_street_id` | Neu khong co thi `NULL` | # bỏ 
| `id_img` | `sdf.id` | Giu lien ket ve record detail goc | 
| `project_name` | `COALESCE(sdf.thuocduan, NULL)` | Raw |
| `slug_name` | Tu dong normalize tu `sdf.title` | Theo quy tac slug cua he thong |   # bỏ 
| `images_status` | `'PENDING'` | Neu van dung FTP/upload pipeline hien tai |
| `stratum_id` | Map tu `sdf.phaply` | Can bang mapping rieng, vd so hong/so do -> `1` |
| `cat_id` | Map tu `dc.std_trans_type` | Ban = `1`, thue = `3` neu giong `data_full` |  # cái này nhắc tao bổ sung 
| `type_id` | Map tu `dc.std_category + dc.std_trans_type` | Nen dung chung bo map voi uploader |
| `unit` | Neu thue thi `'thang'`, neu ban thi `'md'` hoac quy tac cua API moi | Giong `data_full` neu muon dung chung uploader |
| `project_id` | `dc.project_id` | Neu `cleanv1` da co |
| `uploaded_at` | `NULL` luc moi convert | Se duoc set sau khi upload thanh cong |

## Mapping uu tien theo bang

### Lay tu `data_clean_v1`

- `source_post_id`
- `price`
- `area`
- `province_id`
- `district_id`
- `ward_id`
- `street_id`
- `project_id`
- `cat_id`
- `type_id`
- `type`
- `unit`
- `posted_at` nguon chinh tu `orig_list_time`

### Lay tu `scraped_details_flat`

- `title`
- `address`
- `description`
- `bathrooms`
- `bedrooms`
- `broker_name`
- `id_img`
- `project_name` chi khi domain co du lieu; rieng BDS hien tai cot `thuocduan` dang trong

### Lay tu `scraped_detail_images`

- `img`

## Cot can parse/chuan hoa

- `posted_at`
- `bathrooms`
- `bedrooms`
- `slug_name`
- `property_type`
- `cat_id`
- `type_id`
- `stratum_id`

## Cot co the de `NULL`

- `district`
- `district_id`
- `street`
- `street_id`
- `living_rooms`
- `lat`
- `long`
- `phone`
- `project_id`
- `project_name`
- `uploaded_at`

## Ghi chu rieng cho `batdongsan.com.vn`

- Nen giu query:
  - `title`
  - `mota`
  - `sophongvesinh`
  - `tenmoigioi`
- Nen bo query de tranh ton tai nguyen, vi hien tai dang trong toan bo hoac gan nhu toan bo:
  - `huongnha`
  - `mattien`
  - `duongvao`
  - `sotang`
  - `chieungang`
  - `chieudai`
  - `thuocduan`
  - `sodienthoai`
  - `map`
  - `diachicu`
  - `loaibds`
  - `phongan`
  - `nhabep`
  - `santhuong`
  - `chodexehoi`
  - `chinhchu`
  - `city_ext`
  - `district_ext`
  - `ward_ext`
  - `street_ext`

## Ghi chu thuc thi

- Nen tao file convert rieng, vi du: `craw/auto/convert_cleanv1_to_data_no_full.py`
- Nen co co che mark rieng cho `data_no_full`, vi du:
  - `data_no_full_converted` o bang nguon, hoac
  - `NOT EXISTS` theo `source + source_post_id`
- Neu sau nay API moi cho phep khong co so dien thoai, khong nen dung lai dieu kien `full=1` cua `scraped_details_flat` lam nguon chinh.
- `data_clean_v1` la nguon chinh vi da xu ly gia, dien tich, loai hinh, khu vuc.
