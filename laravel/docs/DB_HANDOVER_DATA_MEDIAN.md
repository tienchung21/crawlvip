# Bàn giao DB - `data_median` (MySQL -> PostgreSQL)

Tài liệu này dùng để bàn giao cho đồng nghiệp về:
- Ý nghĩa bảng `data_median`
- Ý nghĩa các trường quan trọng (`scope`, `trimmed_rows`, ...)
- Cách đẩy dữ liệu từ MySQL local lên PostgreSQL
- Cách chạy API và xử lý lỗi thường gặp

## 1) Tổng quan

Hệ thống hiện có 2 nơi lưu `data_median`:
- MySQL local (`craw_db.data_median`): nguồn dữ liệu gốc hiện tại.
- PostgreSQL (`report.data_median`): đích phục vụ dashboard/report.

API Laravel đã tạo để đồng bộ:
- Route: `GET/POST /api/pg/data-median/sync`
- Nếu không truyền `scope`: API chạy FULL tất cả scope.
- Dùng `upsert` (trùng key thì update, chưa có thì insert).

## 2) Cấu trúc bảng `data_median`

Các cột chính:
- `id`: khóa chính.
- `scope`: phạm vi thống kê (`ward`, `region`, `region_total`, `project`).
- `new_region_id`: ID tỉnh/thành sau map.
- `new_ward_id`: ID phường/xã sau map (có ý nghĩa khi `scope=ward`).
- `project_id`: ID dự án (có ý nghĩa khi `scope=project`).
- `type`: loại giao dịch/nhóm dữ liệu theo nghiệp vụ cũ.
- `category`: category theo nghiệp vụ.
- `median_group`: nhóm median.
- `month`: tháng thống kê, format `YYYY-MM`.
- `avg_price_m2`: giá trung bình/m2 sau trim.
- `median_price_m2`: giá median/m2 sau trim.
- `min_price_m2`: giá nhỏ nhất/m2 sau trim.
- `max_price_m2`: giá lớn nhất/m2 sau trim.
- `total_rows`: tổng số bản ghi đầu vào cho group.
- `trimmed_rows`: số bản ghi còn lại sau trim.
- `converted_at`: thời điểm convert.

## 3) `scope` là gì

`scope` là cấp độ tổng hợp dữ liệu:
- `ward`: theo cấp phường/xã (chi tiết nhất).
- `region`: theo cấp tỉnh/thành + `median_group`.
- `region_total`: tổng theo cấp tỉnh/thành (không tách `median_group`).
- `project`: theo cấp dự án + `median_group`.

Khi chạy full API, hệ thống sẽ đẩy lần lượt cả 4 scope.

## 4) `trimmed_rows` (bạn gọi "treamit") là gì

Trong nghiệp vụ median, dữ liệu giá được trim để giảm outlier:
- Bỏ 10% giá thấp nhất.
- Bỏ 10% giá cao nhất.
- Phần còn lại dùng để tính `avg/median/min/max`.

Ý nghĩa:
- `total_rows`: tổng số dòng trước khi trim.
- `trimmed_rows`: số dòng sau khi trim.

Ví dụ:
- `total_rows = 100`
- bỏ 10 dòng đầu + 10 dòng cuối
- `trimmed_rows = 80`

## 5) Khóa unique và hành vi upsert

Upsert key hiện tại:
- `(scope, new_region_id, new_ward_id, project_id, type, median_group, month)`

Hành vi:
- Key đã có trên PostgreSQL -> `UPDATE`.
- Key chưa có -> `INSERT`.

Lưu ý:
- Nhiều cột trong key là nullable, nên với dữ liệu `NULL` cần kiểm soát nguồn để tránh duplicate ngoài ý muốn.

## 6) API sync đã bàn giao

### 6.1 Endpoint
- `GET /api/pg/data-median/sync`
- `POST /api/pg/data-median/sync`

### 6.2 Query params
- `scope`:
  - bỏ trống / `full` / `all`: chạy full 4 scope.
  - hoặc chạy riêng: `ward|region|region_total|project`.
- `limit`: kích thước mỗi batch nội bộ (50..2000).
- `offset`: điểm bắt đầu (mặc định 0).
- `month`: lọc theo tháng `YYYY-MM` (optional).
- `median_group`: lọc theo nhóm (optional).
- `dry_run=true`: chỉ đếm/quet, không ghi PostgreSQL.

### 6.3 Ví dụ lệnh

Chạy full thật:
```bash
curl "http://crawl-data.test:89/api/pg/data-median/sync?limit=2000&offset=0"
```

Chạy full dry-run:
```bash
curl "http://crawl-data.test:89/api/pg/data-median/sync?limit=2000&offset=0&dry_run=true"
```

Chạy riêng 1 scope:
```bash
curl "http://crawl-data.test:89/api/pg/data-median/sync?scope=ward&limit=2000&offset=0"
```

### 6.4 Mẫu response
```json
{
  "ok": true,
  "scope": "full",
  "scopes": ["ward", "region", "region_total", "project"],
  "offset": 0,
  "limit": 2000,
  "processed_rows": 25984,
  "upserted_rows": 25984,
  "batches": 15,
  "per_scope": {
    "ward": {"processed_rows": 19799, "upserted_rows": 19799, "batches": 10}
  },
  "done": true,
  "dry_run": false
}
```

## 7) Cấu hình kết nối PostgreSQL

Đặt trong `laravel/.env`:
```env
PG_HOST=118.69.81.54
PG_PORT=35432
PG_DATABASE=report
PG_USERNAME=reportuser
PG_PASSWORD=***
PG_SCHEMA=public
PG_SSLMODE=prefer
```

Sau khi đổi `.env`, chạy:
```bash
cd /home/chungnt/crawlvip/laravel
php artisan config:clear
```

## 8) File code liên quan

- Config DB:
  - `laravel/config/database.php`
- Model nguồn MySQL:
  - `laravel/app/Models/LocalDataMedian.php`
- Model đích PostgreSQL:
  - `laravel/app/Models/PgDataMedian.php`
- Controller sync:
  - `laravel/app/Http/Controllers/PgDataMedianSyncController.php`
- Route:
  - `laravel/routes/web.php`
- CSRF except:
  - `laravel/bootstrap/app.php`

## 9) Lỗi thường gặp và cách xử lý

1. Lỗi kết nối PostgreSQL (`connection refused`)
- Kiểm tra `PG_HOST/PG_PORT` trong `.env`.
- Kiểm tra DB cho phép IP hiện tại truy cập.
- Chạy `php artisan config:clear` sau khi sửa env.

2. API trả `processed_rows = 0`
- Nguồn local `data_median` đang rỗng theo filter (`scope`, `month`, `median_group`).
- Thử bỏ filter để test.

3. Lỗi extension PHP pgsql
- Cài extension:
```bash
sudo apt-get install -y php-pgsql
```

## 10) Checklist bàn giao cho người nhận

1. Xác nhận có quyền DB PostgreSQL.
2. Điền đúng `PG_*` trong `.env`.
3. Chạy `php artisan config:clear`.
4. Chạy dry-run để xem số lượng.
5. Chạy thật (bỏ `dry_run`) để đẩy dữ liệu.
6. Verify trên PostgreSQL: số dòng, scope, month, random sample.
