# Guland -> data_full Mapping Plan

Tài liệu này mô tả plan convert dữ liệu domain `guland.vn` vào bảng `data_full` theo rule hiện tại.

Mục tiêu:
- chỉ giữ các trường thực sự cần cho `data_full`
- không bịa dữ liệu thiếu
- những field nguồn trống hoàn toàn thì bỏ khỏi plan
- phần địa chỉ sẽ bổ sung sau vì còn 3 trường hợp parse

## 1. Nguồn dữ liệu

### Bảng nguồn chính

- `scraped_details_flat sdf`
  - `domain = 'guland.vn'`

### Bảng nguồn ảnh

- `scraped_detail_images sdi`
  - `sdi.detail_id = sdf.id`

Ghi chú:
- `sdf.diachi` hiện đã sửa selector để ưu tiên lấy đúng row địa chỉ
- `sdf.thuocduan` đã có selector và có thể dùng nếu cần

## 2. Điều kiện chọn row

- `sdf.domain = 'guland.vn'`
- `sdf.title IS NOT NULL`
- `sdf.mota IS NOT NULL`
- `sdf.khoanggia IS NOT NULL`
- `sdf.dientich IS NOT NULL`
- `sdf.matin IS NOT NULL`
- không lấy row đã tồn tại trong `data_full`:
  - `NOT EXISTS (SELECT 1 FROM data_full df WHERE df.source = 'guland.vn' AND df.source_post_id = sdf.matin)`

## 3. Thống kê nhanh dữ liệu Guland hiện tại

### `trade_type`

- `s`: `7014`
- `u`: `348`

Kết luận:
- với Guland, `s = Cần bán`
- `u = Cho thuê`

### `loaibds + trade_type`

- `<NULL> + s`: `79`
- `<NULL> + u`: `3`
- `Căn hộ chung cư + s`: `2521`
- `Căn hộ chung cư + u`: `112`
- `Đất + s`: `1772`
- `Đất + u`: `23`
- `Khách sạn + s`: `7`
- `Khách sạn + u`: `2`
- `Kho, nhà xưởng + s`: `181`
- `Kho, nhà xưởng + u`: `134`
- `Nhà riêng + s`: `2452`
- `Nhà riêng + u`: `47`
- `Nhà trọ + s`: `2`
- `Nhà trọ + u`: `22`
- `Văn phòng + u`: `5`

### Mức có dữ liệu theo field chính

- tổng row Guland: `7574`
- `title`: `7574`
- `mota`: `7574`
- `khoanggia`: `7574`
- `dientich`: `7574`
- `gia_m2`: `7574`
- `gia_mn`: `3472`
- `sophongngu`: `248`
- `sotang`: `173`
- `huongnha`: `309`
- `chieungang`: `3542`
- `thuocduan`: `610`
- `tenmoigioi`: `7512`
- `sodienthoai`: `7574`
- `diachi`: `7574`
- `loaibds`: `7491`
- `trade_type`: `7574`
- `ngaydang`: `7574`

### Các field đang trống hoàn toàn, bỏ khỏi plan

Các field dưới đây hiện `0/7574` row có dữ liệu:

- `sophongvesinh`
- `huongbancong`
- `mattien`
- `duongvao`
- `dientichsudung`
- `chieudai`
- `phaply`
- `diachicu`

Kết luận:
- `phaply` hiện coi như không có, bỏ `legal_status` và `stratum_id` khỏi plan phase này
- `thuocduan` có thể lưu nếu cần, nhưng không phải trường bắt buộc
- `diachi` đủ dữ liệu, nhưng rule parse sẽ bổ sung sau

## 4. Rule match loại hình Guland -> Cafeland

Đây là mapping bạn đã chốt:

| `loaibds` Guland | `property_type` Cafeland |
|---|---|
| `Nhà riêng` | `Bán nhà riêng` |
| `Đất` | `Bán đất thổ cư` |
| `Căn hộ chung cư` | `Bán căn hộ chung cư` |
| `Kho, nhà xưởng` | `Bán kho, nhà xưởng` |
| `Văn phòng` | `Bán căn hộ Mini, Dịch vụ` |
| `Nhà trọ` | `Bán căn hộ Mini, Dịch vụ` |
| `Phòng trọ` | `Bán căn hộ Mini, Dịch vụ` |
| `Khách sạn` | `Bán nhà hàng - Khách sạn` |
| `Mặt bằng kinh doanh` | `Bán căn hộ Mini, Dịch vụ` |

## 5. Rule `cat_id` / `type_id`

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

### Rule bỏ / skip

| `loaibds` Guland | Hướng xử lý |
|---|---|
| `NULL` | tạm `SKIP`, không bịa loại hình |

## 6. Rule `unit`

Rule đã chốt chung:

- `cat_id = 1` -> `unit = md`
- `cat_id = 2` -> `unit = md`
- `cat_id = 3` -> `unit = tháng`

## 7. Rule `ngaydang -> posted_at`

Các dạng text thực tế hiện có:

- `Cập nhật 30 giây trước`
- `Cập nhật 1 phút trước`
- `Cập nhật 2 phút trước`
- `Cập nhật 1 giờ trước`
- `Cập nhật 2 giờ trước`
- `Cập nhật 6 giờ trước`
- `Cập nhật 19 giờ trước`
- `Cập nhật 20 giờ trước`
- `Cập nhật 1 ngày trước`
- `Cập nhật 2 ngày trước`
- `Cập nhật 3 ngày trước`
- `Cập nhật 4 ngày trước`
- `Cập nhật 5 ngày trước`
- `Cập nhật 6 ngày trước`
- `Cập nhật 1 tuần trước`
- `Cập nhật 2 tuần trước`
- `Cập nhật 3 tuần trước`
- `Cập nhật 1 tháng trước`
- `Cập nhật 2 tháng trước`
- `Cập nhật 3 tháng trước`
- `Cập nhật 4 tháng trước`
- `Cập nhật 5 tháng trước`

Rule parse:

1. lấy `created_at` của row crawl làm mốc
2. trừ đi số lượng tương ứng theo đơn vị trong text
3. quy đổi:
   - `giây` -> trừ số giây
   - `phút` -> trừ số phút
   - `giờ` -> trừ số giờ
   - `ngày` -> trừ số ngày
   - `tuần` -> trừ `7 * số_tuần`
   - `tháng` -> trừ số tháng

## 8. Phần địa chỉ

Địa chỉ Guland hiện có nhiều pattern. Khi code convert phải parse theo từng trường hợp, không dùng một rule chung cứng.

### Chuẩn hóa trước khi parse

1. lấy từ `sdf.diachi`
2. trim khoảng trắng
3. bỏ hậu tố `(Mới)` nếu có
4. tách theo dấu phẩy
5. bỏ các phần rỗng sau khi trim

### Quy tắc match chung

Chỉ dùng:

- `scraped_details_flat`
- `transaction_city_merge`

Không dùng `location_batdongsan` ở bước này.

Quy tắc tổng quát:

1. Lấy phần cuối cùng của địa chỉ làm `province_name_raw`
2. Match `province_name_raw` với `transaction_city_merge.new_city_name`
3. Khi match tỉnh, chỉ lấy row:
   - `transaction_city_merge.new_city_parent_id = 0`
   - ưu tiên `action_type = 0`
4. Từ row match được, lấy:
   - `province_id = transaction_city_merge.new_city_id`
5. Lấy phần ứng viên xã/phường làm `ward_name_raw`
6. Match `ward_name_raw` với `transaction_city_merge.new_city_name`
7. Khi match xã/phường, chỉ lấy row:
   - `transaction_city_merge.new_city_parent_id = province_id`
   - đây là chốt để không lấy nhầm ward cùng tên ở tỉnh khác
8. Từ row match được, lấy:
   - `ward_id = transaction_city_merge.new_city_id`
9. `street` là toàn bộ phần text đứng trước `ward_name_raw`

### Chuẩn hóa tên tỉnh trước khi match

Khi lấy `province_name_raw`, cần normalize trước rồi mới match với `transaction_city_merge.new_city_name`.

Các trường hợp phải coi là tương đương:

- `TP. Hà Nội` = `Hà Nội`
- `TP Hà Nội` = `Hà Nội`
- `Thành phố Hà Nội` = `Hà Nội`
- `TP. Hồ Chí Minh` = `Hồ Chí Minh`
- `TP Hồ Chí Minh` = `Hồ Chí Minh`
- `Thành phố Hồ Chí Minh` = `Hồ Chí Minh`

Rule normalize đề xuất:

1. trim khoảng trắng
2. bỏ tiền tố nếu có:
   - `TP.`
   - `TP`
   - `Thành phố`
3. chuẩn hóa nhiều khoảng trắng thành một khoảng trắng
4. match theo tên đã normalize

Mục tiêu:
- `TP. Hà Nội` và `Hà Nội` phải cùng match ra một `province_id`
- `TP. Hồ Chí Minh` và `Hồ Chí Minh` cũng vậy

### Trường hợp 1: địa chỉ mới

Pattern:
- `[street], ward_new, province_new`

Ví dụ:
- `Phường Bình Thạnh, TP. Hồ Chí Minh (Mới)`
- `Xã Hoàn Long, Hưng Yên (Mới)`
- `Đường Cọ Xanh, Xã Phụng Công, Hưng Yên (Mới)`
- `Đường Trương Quang Giao, Phường Đức Phổ, Quảng Ngãi`

Rule:
- lấy `province_name_raw = phần cuối`
- match `province_name_raw` với `transaction_city_merge.new_city_name`
  - điều kiện `new_city_parent_id = 0`
  - ưu tiên `action_type = 0`
- lấy `province_id = transaction_city_merge.new_city_id`
- lấy `ward_name_raw = phần áp cuối`
- match `ward_name_raw` với `transaction_city_merge.new_city_name`
  - kèm điều kiện `new_city_parent_id = province_id`
- lấy `ward_id = transaction_city_merge.new_city_id`
- `street` = toàn bộ phần đứng trước `ward_name_raw`
  - nếu không có phần đứng trước thì `street = NULL`

### Trường hợp 2: địa chỉ cũ có ward + huyện/quận

Pattern:
- `[street], ward_old, district_old, province_old`

Ví dụ:
- `Xã Đặng Lễ, Huyện Ân Thi, Hưng Yên`
- `Phường 26, Quận Bình Thạnh, TP. Hồ Chí Minh`
- `Đường Đinh Bộ Lĩnh, Phường 26, Quận Bình Thạnh, TP. Hồ Chí Minh`
- `Đường Tuyến Đường Trung Tâm Chợ Xã Và Đường Trục Chính, Xã Xuân Quan, Huyện Văn Giang, Hưng Yên`

Rule:
- đây là địa chỉ cũ, nên match bằng `old_city_name`
- lấy `province_name_old = phần cuối`
- match `province_name_old` với `transaction_city_merge.old_city_name`
  - điều kiện `new_city_parent_id = 0`
- lấy `province_id = transaction_city_merge.new_city_id`
- lấy `district_name_raw = phần ngay trước tỉnh`
- lấy `ward_name_raw = phần trước huyện/quận`
- match `ward_name_raw` với `transaction_city_merge.old_city_name`
  - kèm điều kiện `new_city_parent_id = province_id`
- lấy `ward_id = transaction_city_merge.new_city_id`
- `street` = toàn bộ phần đứng trước `ward_name_raw`
  - nếu không có phần đứng trước thì `street = NULL`
- `district_name_raw` hiện chỉ dùng để hỗ trợ logic, chưa lưu xuống `district` / `district_id`

Ghi chú:
- `quận / huyện / thị xã / thành phố thuộc tỉnh` đều phải coi là một cấp trung gian
- ví dụ `Thành phố Cao Bằng` trong địa chỉ `Đường Kim Đồng, Thành phố Cao Bằng, Cao Bằng` là cấp huyện/quận, không phải ward

### Trường hợp 3: địa chỉ cũ chỉ có district-level + tỉnh

Pattern:
- `[street], district_old, province_old`

Ví dụ:
- `Đường Kim Đồng, Thành phố Cao Bằng, Cao Bằng`

Rule:
- đây vẫn là địa chỉ cũ
- lấy `province_name_old = phần cuối`
- match `province_name_old` với `transaction_city_merge.old_city_name`
  - điều kiện `new_city_parent_id = 0`
- lấy `province_id = transaction_city_merge.new_city_id`
- phần giữa là `district_name_raw`
  - `district_name_raw` chỉ dùng để nhận diện đúng pattern
  - chưa lưu xuống `district` / `district_id`
- trường hợp này không có `ward_name_raw`
- `ward_id = NULL`
- `street = phần đầu`
- phase đầu nên `SKIP` vì chưa suy ra được `ward_id`

### Trường hợp 4: không match được xã/phường

Rule fallback:
- vẫn match `province_id` theo `transaction_city_merge.new_city_name` nếu được
- `ward_id = NULL`
- `street = NULL` hoặc giữ phần đầu để debug tùy nhu cầu implement
- phase đầu nên `SKIP` row nếu không match được `ward_id`, tránh đẩy sai khu vực

### Ghi chú implement

- ưu tiên match tỉnh bằng row `new_city_parent_id = 0` và `action_type = 0`
- `ward_name_raw` luôn phải match trong đúng `new_city_parent_id = province_id`
- với địa chỉ cũ có huyện/quận:
  - dùng `old_city_name -> new_city_id`
  - không dùng `new_city_name` để match trực tiếp
- với địa chỉ dạng `[street], district_old, province_old`:
  - không cố suy diễn district thành ward
  - coi là thiếu ward và skip
- không dùng `diachicu` trong phase này
- chưa lưu `district`, `district_id`
- nếu sau này cần giảm false match, có thể dùng thêm huyện/quận làm điều kiện phụ để xác nhận ward

## 9. Các cột sẽ đẩy vào `data_full`

| Cột nguồn | Cột đích `data_full` | Rule |
|---|---|---|
| `sdf.title` | `title` | lấy nguyên bản |
| `sdf.ngaydang` | `posted_at` | parse từ text tương đối |
| ảnh đầu tiên từ `sdi.image_url` | `img` | lấy ảnh có `idx` nhỏ nhất |
| `sdf.khoanggia` | `price` | parse về số |
| `sdf.dientich` | `area` | parse về số |
| `sdf.mota` | `description` | lấy nguyên bản |
| rule từ `sdf.loaibds` | `property_type` | map theo bảng rule phía trên |
| `sdf.huongnha` | `house_direction` | lấy raw nếu có |
| `sdf.sotang` | `floors` | chỉ lấy số; không có số thì `NULL` |
| `sdf.sophongngu` | `bedrooms` | chỉ lấy số; không có số thì `NULL` |
| `sdf.tenmoigioi` | `broker_name` | lấy raw |
| `sdf.sodienthoai` | `phone` | lấy raw |
| `'guland.vn'` | `source` | cố định |
| `NOW()` | `time_converted_at` | cố định |
| `sdf.matin` | `source_post_id` | id gốc của tin |
| `sdf.chieungang` | `width` | parse số nếu lấy được |
| `sdf.id` | `id_img` | lưu liên kết detail gốc |
| `sdf.thuocduan` | `project_name` | lấy raw nếu có |
| rule từ `sdf.trade_type + sdf.loaibds` | `cat_id` | map theo bảng rule |
| rule từ `sdf.trade_type + sdf.loaibds` | `type_id` | map theo bảng rule |
| rule từ `cat_id` | `unit` | `1/2 -> md`, `3 -> tháng` |

## 10. Mapping viết ngắn theo kiểu `A -> B`

- `title -> title`
- `ngaydang -> posted_at`
- `ảnh đầu tiên -> img`
- `khoanggia -> price`
- `dientich -> area`
- `mota -> description`
- `loaibds -> property_type`
- `huongnha -> house_direction`
- `sotang -> floors`
- `sophongngu -> bedrooms`
- `tenmoigioi -> broker_name`
- `sodienthoai -> phone`
- `'guland.vn' -> source`
- `NOW() -> time_converted_at`
- `matin -> source_post_id`
- `chieungang -> width`
- `id detail -> id_img`
- `thuocduan -> project_name`
- `trade_type + loaibds -> cat_id`
- `trade_type + loaibds -> type_id`
- `cat_id -> unit`

## 11. Các cột không cần hoặc tạm bỏ

Các cột sau không cần giữ trong phase này:

- `address`
- `type`
- `city`
- `district`
- `ward`
- `street`
- `province_id`
- `district_id`
- `ward_id`
- `street_id`
- `slug_name`
- `images_status`
- `project_id`
- `uploaded_at`
- `living_rooms`
- `bathrooms`
- `road_width`
- `legal_status`
- `lat`
- `long`
- `length`
- `stratum_id`

Ghi chú:
- `type` text không cần nếu đã có `cat_id` và `type_id`
- location sẽ bổ sung lại sau khi bạn chốt rule địa chỉ

## 12. Rule parse số

### Với `floors`, `bedrooms`

Chỉ lấy số nguyên.

Ví dụ:
- `7` -> `7`
- `7 tầng` -> `7`
- `---` -> `NULL`
- chuỗi không có số -> `NULL`

### Với `width`, `area`, `price`

- parse số nếu lấy được
- không parse chắc thì để `NULL`
- không tự suy đoán

## 13. Những điểm cần bạn duyệt trước khi code convert

1. `loaibds = NULL`
   - skip toàn bộ
   - hay fallback theo title
2. `project_name`
   - có lưu xuống `data_full.project_name` luôn không
   - hay bỏ như uploader hiện tại

## 14. Plan implement sau khi duyệt

1. Viết file convert Guland riêng theo style Alonhadat
2. Parse `price`, `area`, `width`, `floors`, `bedrooms`
3. Parse location từ `sdf.diachi` theo các trường hợp ở mục 8
4. Map `loaibds + trade_type -> property_type/cat_id/type_id`
5. Insert vào `data_full`
6. Không ghi `lat/long`
7. Không ghi `type`
8. Nếu cần uploader test:
   - không truyền `street_name`
   - không truyền `project_name`
   - nếu không có tọa độ thì không truyền default
