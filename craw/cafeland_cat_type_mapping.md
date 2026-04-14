# Cafeland `cat_id` / `type_id` Mapping

Tài liệu này là bản tổng hợp ngắn gọn các rule đang dùng thật trong code cho:

- `alonhadat.com.vn -> data_full`
- `guland.vn -> data_full`
- `nhatot -> data_no_full`

Nguồn sự thật:

- `craw/auto/convert_alonhadat_to_data_full.py`
- `craw/auto/convert_guland_to_data_full.py`
- `craw/auto/convert_nhatot_to_data_no_full.py`

## 1. Bộ `cat_id` / `type_id` chuẩn bên Cafeland

### `cat_id = 1` : Bán nhà đất

| `type_id` | Tên loại hình |
|---:|---|
| `1` | Bán nhà phố dự án |
| `2` | Bán nhà riêng |
| `3` | Bán biệt thự |
| `5` | Bán căn hộ chung cư |
| `56` | Bán căn hộ Mini, Dịch vụ |
| `8` | Bán đất nền dự án |
| `10` | Bán đất nông, lâm nghiệp |
| `11` | Bán đất thổ cư |
| `13` | Bán nhà hàng - Khách sạn |
| `14` | Bán kho, nhà xưởng |

### `cat_id = 2` : Nhóm đất

| `type_id` | Tên loại hình |
|---:|---|
| `8` | Bán đất nền dự án |
| `10` | Bán đất nông, lâm nghiệp |
| `11` | Bán đất thổ cư |

### `cat_id = 3` : Cho thuê

| `type_id` | Tên loại hình |
|---:|---|
| `1` | Nhà phố |
| `2` | Nhà riêng |
| `3` | Biệt thự |
| `5` | Căn hộ chung cư |
| `6` | Văn phòng |
| `12` | Mặt bằng |
| `13` | Nhà hàng - Khách sạn |S
| `14` | Nhà kho - Xưởng |
| `15` | Phòng trọ |
| `57` | Đất khu công nghiệp |

### Rule `unit`

- `cat_id = 1` -> `unit = md`
- `cat_id = 2` -> `unit = md`
- `cat_id = 3` -> `unit = tháng` hoặc `thang` tùy file convert

## 2. Alonhadat -> Cafeland

Ghi chú:

- Rule chạy thật đã sửa về dạng `loaibds + trade_type`
- Tất cả `trade_type = 'u'` đều ra `cat_id = 3`

| `loaibds` Alonhadat | `trade_type` | `property_type` Cafeland | `cat_id` | `type_id` |
|---|---|---|---:|---:|
| `Nhà mặt tiền` | `s` | `Bán nhà riêng` | `1` | `2` |
| `Nhà mặt tiền` | `u` | `Cho thuê nhà riêng` | `3` | `2` |
| `Nhà trong hẻm` | `s` | `Bán nhà riêng` | `1` | `2` |
| `Nhà trong hẻm` | `u` | `Cho thuê nhà riêng` | `3` | `2` |
| `Biệt thự, nhà liền kề` | `s` | `Bán biệt thự` | `1` | `3` |
| `Biệt thự, nhà liền kề` | `u` | `Cho thuê biệt thự` | `3` | `3` |
| `Căn hộ chung cư` | `s` | `Bán căn hộ chung cư` | `1` | `5` |
| `Căn hộ chung cư` | `u` | `Cho thuê căn hộ chung cư` | `3` | `5` |
| `Phòng trọ, nhà trọ` | `s` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `Phòng trọ, nhà trọ` | `u` | `Cho thuê phòng trọ` | `3` | `15` |
| `Văn phòng` | `s` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `Văn phòng` | `u` | `Cho thuê văn phòng` | `3` | `6` |
| `Kho, xưởng` | `s` | `Bán kho, nhà xưởng` | `1` | `14` |
| `Kho, xưởng` | `u` | `Cho thuê nhà kho - Xưởng` | `3` | `14` |
| `Nhà hàng, khách sạn` | `s` | `Bán nhà hàng - Khách sạn` | `1` | `13` |
| `Nhà hàng, khách sạn` | `u` | `Cho thuê nhà hàng - Khách sạn` | `3` | `13` |
| `Shop, kiot, quán` | `s` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `Shop, kiot, quán` | `u` | `Cho thuê mặt bằng` | `3` | `12` |
| `Trang trại` | `s` | `Bán đất nông, lâm nghiệp` | `2` | `10` |
| `Trang trại` | `u` | `Cho thuê đất` | `3` | `57` |
| `Mặt bằng` | `s` | `Bán đất nền dự án` | `2` | `8` |
| `Mặt bằng` | `u` | `Cho thuê mặt bằng` | `3` | `12` |
| `Đất thổ cư, đất ở` | `s` | `Bán đất thổ cư` | `2` | `11` |
| `Đất thổ cư, đất ở` | `u` | `SKIP` | | |
| `Đất nền, liền kề, đất dự án` | `s` | `Bán đất thổ cư` | `2` | `11` |
| `Đất nền, liền kề, đất dự án` | `u` | `SKIP` | | |
| `Đất nông, lâm nghiệp` | `s` | `Bán đất nông, lâm nghiệp` | `2` | `10` |
| `Đất nông, lâm nghiệp` | `u` | `SKIP` | | |

### Alonhadat không map

- `Các loại khác` -> bỏ
- `loaibds = NULL` -> `skip_type`

## 3. Guland -> Cafeland

Ghi chú:

- Rule chạy thật đang là `loaibds + trade_type`
- Tất cả `trade_type = 'u'` đều ra `cat_id = 3`

| `loaibds` Guland | `trade_type` | `property_type` Cafeland | `cat_id` | `type_id` |
|---|---|---|---:|---:|
| `Nhà riêng` | `s` | `Bán nhà riêng` | `1` | `2` |
| `Nhà riêng` | `u` | `Cho thuê nhà riêng` | `3` | `2` |
| `Đất` | `s` | `Bán đất thổ cư` | `2` | `11` |
| `Đất` | `u` | `Cho thuê đất` | `3` | `57` |
| `Căn hộ chung cư` | `s` | `Bán căn hộ chung cư` | `1` | `5` |
| `Căn hộ chung cư` | `u` | `Cho thuê căn hộ chung cư` | `3` | `5` |
| `Kho, nhà xưởng` | `s` | `Bán kho, nhà xưởng` | `1` | `14` |
| `Kho, nhà xưởng` | `u` | `Cho thuê nhà kho - Xưởng` | `3` | `14` |
| `Văn phòng` | `s` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `Văn phòng` | `u` | `Cho thuê văn phòng` | `3` | `6` |
| `Nhà trọ` | `s` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `Nhà trọ` | `u` | `Cho thuê phòng trọ` | `3` | `15` |
| `Phòng trọ` | `s` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `Phòng trọ` | `u` | `Cho thuê phòng trọ` | `3` | `15` |
| `Khách sạn` | `s` | `Bán nhà hàng - Khách sạn` | `1` | `13` |
| `Khách sạn` | `u` | `Cho thuê nhà hàng - Khách sạn` | `3` | `13` |
| `Mặt bằng kinh doanh` | `s` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `Mặt bằng kinh doanh` | `u` | `Cho thuê mặt bằng` | `3` | `12` |

### Guland không map

- `loaibds = NULL` -> `skip_type`

## 4. Nhatot -> Cafeland

Ghi chú:

- Nhatot không map bằng `loaibds` text
- Nó map từ `category`, `house_type`, `apartment_type`, `commercial_type`, `land_type`, `type`
- `data_no_full` là nhánh riêng

### Nhatot bán nhà / căn hộ / thương mại

| Điều kiện Nhatot | `property_type` Cafeland | `cat_id` | `type_id` |
|---|---|---:|---:|
| `category=1020` + `house_type=1` | `Bán nhà riêng` | `1` | `2` |
| `category=1020` + `house_type=2` | `Bán nhà riêng` | `1` | `2` |
| `category=1020` + `house_type=3` | `Biệt thự` | `1` | `3` |
| `category=1020` + `house_type=4` | `Bán nhà phố dự án` | `1` | `1` |
| `category=1010` + `apartment_type=2` | `Bán căn hộ Mini, Dịch vụ` | `1` | `56` |
| `category=1010` + `apartment_type IN (1,3,4,5,6)` | `Bán căn hộ chung cư` | `1` | `5` |
| `category=1030` + `commercial_type=1` | `Nhà hàng - Khách sạn` | `1` | `13` |
| `category=1030` + `commercial_type=2` | `Nhà Kho - Xưởng` | `1` | `14` |

### Nhatot nhóm đất

| Điều kiện Nhatot | `property_type` Cafeland | `cat_id` | `type_id` |
|---|---|---:|---:|
| `category=1040` + `land_type=1` | `Bán đất thổ cư` | `2` | `11` |
| `category=1040` + `land_type=2` | `Bán đất nền dự án` | `2` | `8` |
| `category=1040` + `land_type=3` | `Bán đất nông, lâm nghiệp` | `2` | `10` |
| `category=1040` + `land_type=4` | `Bán đất nông, lâm nghiệp` | `2` | `10` |

### Nhatot cho thuê

| Điều kiện Nhatot | `property_type` Cafeland | `cat_id` | `type_id` |
|---|---|---:|---:|
| `category=1020` + `house_type=1` | `Nhà phố` | `3` | `1` |
| `category=1020` + `house_type=2` | `Nhà riêng` | `3` | `2` |
| `category=1020` + `house_type=3` | `Biệt thự` | `3` | `3` |
| `category=1020` + `house_type=4` | `Nhà phố` | `3` | `1` |
| `category=1010` + `apartment_type IN (1,2,3,4,5,6)` | `Căn hộ chung cư` | `3` | `5` |
| `category=1030` + `commercial_type=4` | `Văn phòng` | `3` | `6` |
| `category=1030` + `commercial_type=3` | `Mặt bằng` | `3` | `12` |
| `category=1030` + `commercial_type=1` | `Nhà hàng - Khách sạn` | `3` | `13` |
| `category=1030` + `commercial_type=2` | `Nhà Kho - Xưởng` | `3` | `14` |
| `category=1050` | `Phòng trọ` | `3` | `15` |

### Nhatot không map

- `category=1030` + `commercial_type=3/4` bên bán -> `NULL`
- `category=1050` bên bán -> `NULL`
- row `type_id IS NULL` -> bỏ qua khi convert

## 5. Kết luận ngắn

- `Alonhadat`: `trade_type='u'` -> luôn `cat_id = 3`
- `Guland`: `trade_type='u'` -> luôn `cat_id = 3`
- `Nhatot`: không map theo text domain, mà map theo bộ mã category/type nội bộ

Nếu cần rà lại rule đang chạy thật, mở trực tiếp:

- [`convert_alonhadat_to_data_full.py`](/home/chungnt/crawlvip/craw/auto/convert_alonhadat_to_data_full.py)
- [`convert_guland_to_data_full.py`](/home/chungnt/crawlvip/craw/auto/convert_guland_to_data_full.py)
- [`convert_nhatot_to_data_no_full.py`](/home/chungnt/crawlvip/craw/auto/convert_nhatot_to_data_no_full.py)
