# Land Price Mapping Chuẩn Cho 6 Domain

Tài liệu này chốt rule tách `price_land` cho các domain hiện tại trong `data_clean_v1`.

## 1. Rule chung

- Chỉ tính `price_land` cho tin `bán`
- Tin `thuê`:
  - không tách đất
  - `land_price_status = 'SKIP'`
- `Nhóm Đất`:
  - trừ `0%`
  - `price_land = price_vnd`
- `Nhóm Có nhà`:
  - trừ `15%`
  - `price_land = price_vnd * 0.85`
- `BỎ`:
  - không tính `price_land`
  - `land_price_status = 'SKIP'`
- Các loại `trọ` ở mọi domain:
  - luôn `BỎ`

## 2. Bảng chuẩn theo domain

### 2.1 `nhadat`

#### Nhóm Đất

- `8` = `Bán đất nền dự án`
- `10` = `Bán đất nông, lâm nghiệp`
- `11` = `Bán đất thổ cư`
- `57` = `Đất khu công nghiệp`

#### Nhóm Có nhà

- `1` = `Nhà phố`
- `2` = `Nhà riêng`
- `3` = `Biệt thự`
- `13` = `Nhà hàng - Khách sạn`
- `14` = `Nhà Kho - Xưởng`

#### BỎ

- `5` = `Căn hộ chung cư`
- `6` = `Văn phòng`
- `12` = `Mặt bằng`
- `15` = `Phòng trọ`
- `56` = `Bán căn hộ Mini, Dịch vụ`

#### Ghi chú

- Với `nhadat`, loại hình đang lưu dạng mã số trong `data_clean_v1.std_category`
- Chỉ áp dụng rule này cho tin bán:
  - `std_trans_type = 's'`

### 2.2 `guland.vn`

#### Nhóm Đất

- `Đất`

#### Nhóm Có nhà

- `Nhà riêng`
- `Kho, nhà xưởng`

#### BỎ

- `Căn hộ chung cư`
- `Văn phòng`
- `Khách sạn`
- `Mặt bằng kinh doanh`
- `Nhà trọ`
- `Phòng trọ`

### 2.3 `nhatot`

#### Nhóm Đất

- `1040`

#### Nhóm Có nhà

- `1020`

#### BỎ

- `1010`
- `1030`
- `1050`

#### Ghi chú

- `1050` được coi là nhóm trọ / cho thuê và không tính land

### 2.4 `mogi`

#### Nhóm Đất

- `Đất thổ cư`
- `Đất nền dự án`
- `Đất nông nghiệp`
- `Đất kho xưởng`

#### Nhóm Có nhà

- `Nhà mặt tiền phố`
- `Nhà hẻm ngõ`
- `Nhà biệt thự, liền kề`
- `Đường nội bộ`
- `Nhà xưởng, kho bãi`

#### BỎ

- `Căn hộ chung cư`
- `Căn hộ dịch vụ`
- `Căn hộ tập thể, cư xá`
- `Căn hộ Penthouse`
- `Căn hộ Officetel`
- `Văn phòng`
- `Phòng trọ, nhà trọ`

### 2.5 `batdongsan.com.vn`

#### Nhóm Đất

- `Đất`
- `Đất nền dự án`

#### Nhóm Có nhà

- `Nhà riêng`
- `Nhà mặt phố`
- `Biệt thự liền kề`
- `Kho, nhà xưởng, đất`
- `Kho, nhà xưởng`
- `Shophouse`
- `Trang trại/Khu nghỉ dưỡng`

#### BỎ

- `Căn hộ chung cư`
- `Căn hộ chung cư mini`
- `Văn phòng`
- `Condotel`
- `Cửa hàng, Ki-ốt`
- `BĐS khác`
- `Nhà trọ, phòng trọ`

### 2.6 `alonhadat.com.vn`

#### Nhóm Đất

- `Đất thổ cư, đất ở`
- `Đất nền, liền kề, đất dự án`
- `Đất nông, lâm nghiệp`

#### Nhóm Có nhà

- `Nhà mặt tiền`
- `Nhà trong hẻm`
- `Biệt thự, nhà liền kề`
- `Kho, xưởng`
- `Trang trại`

#### BỎ

- `Căn hộ chung cư`
- `Văn phòng`
- `Nhà hàng, khách sạn`
- `Shop, kiot, quán`
- `Mặt bằng`
- `Phòng trọ, nhà trọ`

## 3. Code hiện đã có logic land

Hiện tại đã có code tách `price_land` ở:

- `craw/auto/convert_guland_to_data_clean_v1.py`
- `craw/auto/convert_alonhadat_to_data_clean_v1.py`

## 4. Code chưa cập nhật theo bảng chuẩn này

Các domain còn lại chưa có script land riêng theo bảng chốt ở tài liệu này:

- `nhadat`
- `nhatot`
- `mogi`
- `batdongsan.com.vn`

## 5. Lưu ý triển khai

- Khi code match loại hình theo text:
  - cần trim khoảng trắng
  - bỏ dấu chấm kết câu dư ở cuối nếu có trong input mô tả
- Không overwrite lại các row đã có:
  - `land_price_status = 'DONE'`
  - `land_price_status = 'SKIP'`
- Chỉ fill những row đang `land_price_status IS NULL`
