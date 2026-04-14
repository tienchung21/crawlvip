# Alonhadat -> data_full Mapping Plan

Tài liệu này mô tả plan convert dữ liệu domain `alonhadat.com.vn` vào bảng `data_full` theo rule đã chốt hiện tại.

Mục tiêu:
- chỉ giữ các trường thực sự cần cho `data_full`
- location đi từ phải qua trái để lấy `province_id`, `ward_id`
- không giữ các trường text location nếu không cần
- những trường raw không chắc thì bỏ, không bịa

## 1. Nguồn dữ liệu

### Bảng nguồn chính

- `scraped_details_flat sdf`
  - `domain = 'alonhadat.com.vn'`

### Bảng nguồn ảnh

- `scraped_detail_images sdi`
  - `sdi.detail_id = sdf.id`

### Bảng location dùng để match

- `location_batdongsan lb`

Ghi chú:
- dữ liệu thô của nguồn này đang là tên xã/phường mới
- không cần qua `old_city_id`
- không cần merge theo `id_city_old`

## 2. Điều kiện chọn row

- `sdf.domain = 'alonhadat.com.vn'`
- `COALESCE(sdf.datafull_converted, 0) = 0`
- `sdf.title IS NOT NULL`
- `sdf.mota IS NOT NULL`
- `sdf.khoanggia IS NOT NULL`
- `sdf.dientich IS NOT NULL`
- `sdf.matin IS NOT NULL`
- không lấy row đã tồn tại trong `data_full`:
  - `NOT EXISTS (SELECT 1 FROM data_full df WHERE df.source = 'alonhadat.com.vn' AND df.source_post_id = sdf.matin)`

## 3. Rule match location

### Hướng match

Đi từ phải qua trái trên `sdf.diachi`.

Ví dụ:
- `Đường Ngô Quyền , Phường Trần Phú , Hà Tĩnh`
- `, Xã Cửa Việt , Quảng Trị`

### Các bước

1. Tách `sdf.diachi` theo dấu phẩy
2. Lấy phần cuối cùng để match `name_city_new`
   - match ra tỉnh/thành mới
   - lấy được `province_id` mới
3. Sau đó lấy phần áp cuối để match xã/phường mới
   - lấy được `ward_id` mới
4. Nếu còn phần trước nữa thì lấy làm `street`
5. Không cần đi qua `id_city_old`
6. Không cần lưu tên tỉnh, tên xã nếu đã lấy được id

### Output location cần giữ

- `province_id`
- `ward_id`
- `street`

### Output location không cần giữ

- `address`
- `city`
- `district`
- `ward`
- `district_id`
- `street_id`

## 4. Các cột sẽ đẩy vào `data_full`

| Cột nguồn | Cột đích `data_full` | Rule |
|---|---|---|
| `sdf.title` | `title` | lấy nguyên bản |
| `sdf.ngaydang` | `posted_at` | parse ngày đăng |
| ảnh đầu tiên từ `sdi.image_url` | `img` | lấy ảnh có `idx` nhỏ nhất |
| `sdf.khoanggia` | `price` | parse về số |
| `sdf.dientich` | `area` | parse về số |
| `sdf.mota` | `description` | lấy nguyên bản |
| rule từ `sdf.loaibds` | `property_type` | map theo bảng rule riêng |
| `sdf.trade_type` | `type` | `s -> Cho thuê`, `u -> Cần bán` |
| `sdf.huongnha` | `house_direction` | lấy raw nếu có |
| `sdf.sotang` | `floors` | chỉ lấy số, ví dụ `1`, `2`, `3`; nếu là `---` thì bỏ |
| `sdf.sophongvesinh` | `bathrooms` | chỉ lấy số; nếu là `---` thì bỏ |
| `sdf.sophongngu` | `bedrooms` | chỉ lấy số; nếu là `---` thì bỏ |
| `sdf.duongvao` | `road_width` | parse số nếu lấy được |
| `sdf.phaply` | `legal_status` | lấy raw |
| `sdf.tenmoigioi` | `broker_name` | lấy raw |
| `sdf.sodienthoai` | `phone` | lấy raw |
| `'alonhadat.com.vn'` | `source` | cố định |
| `NOW()` | `time_converted_at` | cố định |
| `sdf.matin` | `source_post_id` | id gốc của tin |
| `sdf.chieungang` | `width` | parse số nếu lấy được |
| `sdf.chieudai` | `length` | parse số nếu lấy được |
| `street_text` parse từ `sdf.diachi` | `street` | chỉ lưu nếu còn phần trước sau khi đã lấy tỉnh và xã |
| `province_id` match từ `name_city_new` | `province_id` | bắt buộc nếu match được |
| `ward_id` match từ xã/phường mới | `ward_id` | bắt buộc nếu match được |
| `sdf.id` | `id_img` | lưu liên kết detail gốc |
| `sdf.thuocduan` | `project_name` | lấy raw nếu có |
| rule từ `sdf.trade_type` | `cat_id` | chờ chốt số |
| rule từ `sdf.loaibds + sdf.trade_type` | `type_id` | chờ chốt rule |
| rule từ `sdf.trade_type` | `unit` | chờ chốt rule |

## 5. Mapping viết ngắn theo kiểu `A -> B`

- `title -> title`
- `ngaydang -> posted_at`
- `ảnh đầu tiên -> img`
- `khoanggia -> price`
- `dientich -> area`
- `mota -> description`
- `loaibds -> property_type`
- `trade_type -> type`
- `huongnha -> house_direction`
- `sotang -> floors`
- `sophongvesinh -> bathrooms`
- `sophongngu -> bedrooms`
- `duongvao -> road_width`
- `phaply -> legal_status`
- `tenmoigioi -> broker_name`
- `sodienthoai -> phone`
- `'alonhadat.com.vn' -> source`
- `NOW() -> time_converted_at`
- `matin -> source_post_id`
- `chieungang -> width`
- `chieudai -> length`
- `street parse từ diachi -> street`
- `province_id match từ name_city_new -> province_id`
- `ward_id match từ xã/phường mới -> ward_id`
- `id detail -> id_img`
- `thuocduan -> project_name`
- `trade_type -> cat_id`
- `loaibds + trade_type -> type_id`
- `trade_type -> unit`

## 6. Các cột không cần hoặc tạm bỏ

Các cột sau không cần giữ trong phase này:

- `address`
- `city`
- `district`
- `ward`
- `district_id`
- `street_id`
- `slug_name`
- `images_status`
- `project_id`
- `uploaded_at`
- `living_rooms`
- `lat`
- `long`

## 7. Rule parse số

### Với `floors`, `bathrooms`, `bedrooms`

Chỉ lấy số nguyên.

Ví dụ:
- `1 tầng` -> `1`
- `2 WC` -> `2`
- `3 phòng ngủ` -> `3`
- `---` -> `NULL`
- chuỗi không có số -> `NULL`

### Với `width`, `length`, `road_width`, `area`, `price`

- parse số nếu lấy được
- không parse chắc thì để `NULL`
- không tự suy đoán

## 8. Rule `type`

Rule đã chốt:

- `s -> Cho thuê`
- `u -> Cần bán`

Ghi chú:
- không để `s` / `u` ở cột `type`
- cột `type` sẽ là text theo đúng ý nghĩa nghiệp vụ

## 9. Rule còn chờ duyệt

### 9.1 `trade_type -> cat_id`

Cần bạn chốt số cụ thể.

### 9.2 `trade_type -> unit`

Rule đã chốt:
- `cat_id = 1` -> `unit = md`
- `cat_id = 2` -> `unit = md`
- `cat_id = 3` -> `unit = tháng`

### 9.3 `loaibds -> property_type`

Cần lập bảng mapping riêng cho Alonhadat.

### 9.4 `loaibds + trade_type -> type_id`

Chưa chốt, không nên đoán.

### 9.5 `phaply -> stratum_id`

Danh mục `stratum_id` từ API:

| `stratum_id` | Tên |
|---:|---|
| `0` | Tình trạng pháp lý |
| `1` | Sổ hồng |
| `2` | Giấy đỏ |
| `3` | Giấy tay |
| `4` | Giấy tờ hợp lệ |
| `5` | Đang hợp thức hóa |
| `6` | Chủ quyền tư nhân |
| `7` | Hợp đồng |
| `8` | Không xác định |

Rule map Alonhadat:

| `scraped_details_flat.phaply` | `stratum_id` |
|---|---:|
| `Sổ hồng/ Sổ đỏ` | `1` |
| `Giấy tờ hợp lệ` | `4` |
| `Giấy phép XD` | `8` |
| `---` / rỗng / `NULL` | `NULL` |
| giá trị khác chưa rõ | `8` |

## 10. Bộ `cat_id` / `type_id` đã có từ rule cũ

Đây là bộ rule đã dùng trước đó khi convert/upload các nguồn khác như Mogi, Nhatot.

### `cat_id = 1` : Nhà đất bán

| `type_id` | Tên |
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

### `cat_id = 2` : Đất

| `type_id` | Tên |
|---:|---|
| `8` | Bán đất nền dự án |
| `10` | Bán đất nông, lâm nghiệp |
| `11` | Bán đất thổ cư |

### `cat_id = 3` : Cho thuê

| `type_id` | Tên |
|---:|---|
| `1` | Nhà phố |
| `2` | Nhà riêng |
| `3` | Biệt thự |
| `5` | Căn hộ chung cư |
| `6` | Văn phòng |
| `12` | Mặt bằng |
| `13` | Nhà hàng - Khách sạn |
| `14` | Nhà kho - Xưởng |
| `15` | Phòng trọ |
| `57` | Đất khu công nghiệp |

### Ghi chú

- nhóm đất khi đã gom vào `cat_id = 2` thì chỉ dùng các `type_id`:
  - `8`
  - `10`
  - `11`
- không nên tự bịa thêm `type_id` ngoài bộ này
- với Alonhadat, bước tiếp theo là map từng `loaibds` thực tế vào đúng bộ `cat_id/type_id` ở trên

## 11. Quy tắc map `loaibds` Alonhadat -> loại hình Cafeland

Đây là rule nghiệp vụ đã chốt.

Bên trái là tên loại hình của Alonhadat.  
Bên phải là loại hình Cafeland đích để từ đó suy ra `cat_id` và `type_id`.

| `loaibds` Alonhadat | Tên loại hình Cafeland đích | Ghi chú |
|---|---|---|
| `Nhà mặt tiền` | `Bán nhà riêng` | suy ra `type_id = 2` |
| `Nhà trong hẻm` | `Bán nhà riêng` | suy ra `type_id = 2` |
| `Biệt thự, nhà liền kề` | `Bán biệt thự` | suy ra `type_id = 3` |
| `Căn hộ chung cư` | `Bán căn hộ chung cư` | suy ra `type_id = 5` |
| `Phòng trọ, nhà trọ` | `Bán căn hộ Mini, Dịch vụ` | suy ra `type_id = 56` |
| `Văn phòng` | `Bán căn hộ Mini, Dịch vụ` | suy ra `type_id = 56` |
| `Kho, xưởng` | `Bán kho, nhà xưởng` | suy ra `type_id = 14` |
| `Nhà hàng, khách sạn` | `Bán nhà hàng - Khách sạn` | suy ra `type_id = 13` |
| `Shop, kiot, quán` | `Bán căn hộ Mini, Dịch vụ` | suy ra `type_id = 56` |
| `Trang trại` | `Bán đất nông, lâm nghiệp` | suy ra `type_id = 10` |
| `Mặt bằng` | `Bán đất nền dự án` | suy ra `type_id = 8` |
| `Đất thổ cư, đất ở` | `Bán đất thổ cư` | suy ra `type_id = 11` |
| `Đất nền, liền kề, đất dự án` | `Bán đất thổ cư` | suy ra `type_id = 11` |
| `Đất nông, lâm nghiệp` | `Bán đất nông, lâm nghiệp` | suy ra `type_id = 10` |
| `Các loại khác` | `Bỏ` | không convert |

Rule bổ sung:
- `Đất thổ cư, đất ở + u` -> `skip_type`
- `Đất nền, liền kề, đất dự án + u` -> `skip_type`
- `Đất nông, lâm nghiệp + u` -> `skip_type`

### Ghi chú thực thi

- Ví dụ:
  - `loaibds = 'Đất thổ cư, đất ở'`
  - dò sang rule trên
  - ra loại hình Cafeland đích là `Bán đất thổ cư`
  - từ đó suy ra:
    - `cat_id = 2`
    - `type_id = 11`

- Tức là khi code:
  - không map trực tiếp tên Alonhadat sang số
  - mà đi qua lớp trung gian là tên loại hình Cafeland đích
  - rồi mới suy ra `cat_id`, `type_id`

## 12. Cơ chế insert / chống trùng

- unique logic theo:
  - `source = 'alonhadat.com.vn'`
  - `source_post_id = sdf.matin`

Sau khi insert thành công:
- update `scraped_details_flat.datafull_converted = 1`

Nếu row đã tồn tại trong `data_full`:
- không insert lại
- có thể vẫn update `datafull_converted = 1`

## 13. Chỗ bạn cần sửa / duyệt

- `trade_type -> cat_id`
- `trade_type -> unit`
- `loaibds -> property_type`
- `loaibds + trade_type -> type_id`
- có cần giữ `project_name` không
- có cần map `stratum_id` không
